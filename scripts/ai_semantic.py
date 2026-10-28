# scripts/ai_semantic.py —— L1 语义层：指标计划（Metric Plan） -> SQL 模板
#
# 设计原则：模型只负责「选指标 / 选维度 / 选排序」，SQL 由本文件拼。
# 好处：①口径不可能漂移 ②缺指标在拼 SQL 之前就报出来 ③可解释、可评测
import difflib
import json

from ai_metrics import METRICS

DT_LO, DT_HI = "2017-11-25", "2017-12-03"

# 哪些表有 dt 列（没有的表不能加 dt 过滤，这是之前踩过的坑）
TABLES_WITH_DT = {
    "dws_user_day": True,
    "dws_item_day": True,
    "dws_category_day": True,
    "ads_traffic": True,
    "ads_retention": True,
    "ads_funnel": True,
    "ads_rf_segment": False,
}

# 这些指标是比率，展示时要打百分比
PERCENT_METRICS = {"cart_rate", "buy_rate", "d1_rate", "r_total", "conv_browse_to_buy"}

MAX_LIMIT = 500


class UnknownMetric(Exception):
    """指标不在字典里 —— 这是 L1 最重要的一条报错"""


class BadPlan(Exception):
    """计划的字段不合法（维度不支持、排序字段不在结果里等）"""


def dims_of(key: str) -> list:
    """指标允许的维度。字典里写了 dims 就用它，否则按 grain 推断。"""
    m = METRICS[key]
    if "dims" in m:
        return list(m["dims"])
    return ["dt"] if m.get("grain", "日") == "日" else []


def metrics_catalog() -> str:
    """生成给模型看的指标清单（只列「能选什么」，不列表达式——表达式由程序用）"""
    lines = []
    for k, m in METRICS.items():
        ds = dims_of(k)
        dim_txt = "、".join(ds) if ds else "（不可分组，只能算总数）"
        syn = "、".join(m.get("synonyms", [])[:4])
        lines.append(
            f"- {k}：「{m['cn']}」，单位{m.get('unit', '')}，可分组维度：{dim_txt}"
            + (f"，别名：{syn}" if syn else "")
        )
    return "\n".join(lines)


def parse_plan(raw: str) -> dict:
    """把模型输出解析成计划。模型最爱加围栏和解释，这里全部剥掉。"""
    text = "\n".join(ln for ln in raw.splitlines() if not ln.strip().startswith("```"))

    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end <= start:
        raise BadPlan(f"模型没有输出 JSON：{text[:120]}")
    try:
        plan = json.loads(text[start:end + 1])
    except json.JSONDecodeError as e:
        raise BadPlan(f"JSON 解析失败：{e}；原文：{text[:120]}")

    if not isinstance(plan, dict) or "metric" not in plan:
        raise BadPlan("计划里缺少 metric 字段")

    plan["dims"] = plan.get("dims") or []
    if not isinstance(plan["dims"], list):
        raise BadPlan("dims 必须是列表")
    plan["limit"] = min(int(plan.get("limit") or MAX_LIMIT), MAX_LIMIT)
    return plan


def build_sql(plan: dict) -> str:
    """计划 -> SQL。缺指标在这里就炸，不会一路跑到执行。"""
    metric = plan.get("metric", "")
    if metric not in METRICS:
        near = difflib.get_close_matches(metric, list(METRICS), n=3, cutoff=0.3)
        hint = f"；最接近的是 {'、'.join(near)}" if near else ""
        raise UnknownMetric(
            f"字典里没有指标「{metric}」{hint}。"
            f"要么换个说法，要么先在 ai_metrics.py 里把它定义清楚。"
        )

    m = METRICS[metric]
    dims = plan["dims"]
    allowed = dims_of(metric)
    bad = [d for d in dims if d not in allowed]
    if bad:
        raise BadPlan(f"指标 {metric} 不支持维度 {bad}，只支持 {allowed or '无'}")

    alias = metric
    select = ", ".join([*dims, f"{m['expr']} AS {alias}"]) if dims \
        else f"{m['expr']} AS {alias}"

    sql = f"SELECT {select} FROM {m['table']}"

    if TABLES_WITH_DT.get(m["table"], False):
        sql += f" WHERE dt BETWEEN '{DT_LO}' AND '{DT_HI}'"

    if dims:
        sql += " GROUP BY " + ", ".join(dims)

    order_by = plan.get("order_by") or (dims[0] if dims else None)
    if order_by:
        if order_by != alias and order_by not in dims:
            raise BadPlan(f"排序字段 {order_by} 不在结果里（可选：{dims + [alias]}）")
        direction = "DESC" if str(plan.get("order", "asc")).lower().startswith("desc") else "ASC"
        sql += f" ORDER BY {order_by} {direction}"

    sql += f" LIMIT {plan.get('limit') or MAX_LIMIT}"


    return sql