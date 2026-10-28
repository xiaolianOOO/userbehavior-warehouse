# scripts/ai_nl2sql.py —— L1 版：模型只选指标，SQL 由语义层拼
import os
from pathlib import Path

import duckdb

from ai_guard import guard, GuardError
from ai_metrics import METRICS
from ai_semantic import (BadPlan, UnknownMetric, build_sql, metrics_catalog,
                         parse_plan, PERCENT_METRICS)
from ai_fewshot import render_fewshots     # L2 示例层（few-shot）



# ============================================================
# 0. 配置
# ============================================================
def _load_env():
    p = Path("F:/data_warehouse/.env")
    if not p.exists():
        raise FileNotFoundError("找不到 .env，要放在 F:/data_warehouse/.env")
    for line in p.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip().lstrip("\ufeff"), v.strip().strip('"').strip("'"))


_load_env()

USE_LLM = True
LLM_MODEL = os.environ.get("OPENAI_MODEL", "deepseek-chat")

DATA = "F:/data_warehouse/data"
UV_9D = 987_991
BUYERS_9D = 672_404


class NotSupported(Exception):
    pass


# ============================================================
# 1. 规划器 A：规则版（不连模型也能跑，作为对照基线）
# ============================================================
def rule_plan(q: str) -> dict:
    """返回一个指标计划。识别不了抛 NotSupported。"""
    q = q.strip()

    if any(k in q for k in ("没买", "没下单", "未购买", "没消费")):
        return {"metric": "non_buyers", "dims": []}

    if any(k in q for k in ("留存", "复访", "还在来")):
        return {"metric": "d1_rate", "dims": ["dt"]}

    if any(k in q for k in ("漏斗", "转化率", "转化")):
        return {"metric": "r_total", "dims": ["dt"]}

    if any(k in q for k in ("分层", "RF", "人群", "九格")):
        return {"metric": "rf_users", "dims": ["r_seg", "f_seg"],
                "order_by": "rf_users", "order": "desc"}

    if any(k in q for k in ("加购率", "加购比例")):
        return {"metric": "cart_rate", "dims": ["dt"]}

    if any(k in q for k in ("购买率", "下单率", "买的比例")):
        return {"metric": "buy_rate", "dims": ["dt"]}

    if any(k in q for k in ("人均", "每人看", "多少页")):
        return {"metric": "pv_per_user", "dims": ["dt"]}

    if any(k in q for k in ("购买人数", "多少人买", "买家", "下单人")):
        return {"metric": "buy_users", "dims": ["dt"]}

    if any(k in q for k in ("浏览", "pv", "流量", "看了多少")):
        return {"metric": "pv_total", "dims": ["dt"]}

    if any(k in q for k in ("日活", "uv", "多少人", "用户数", "来人")):
        return {"metric": "uv", "dims": ["dt"]}

    raise NotSupported("我只支持：日活 / 浏览 / 购买人数 / 购买率 / 加购率 / 人均页数 / "
                       "留存 / 漏斗 / RF 分层 / 未购买人数")


# ============================================================
# 2. 规划器 B：大模型版 —— 注意它只输出 JSON，不写 SQL
# ============================================================
PLAN_PROMPT = """你是数据仓库的指标规划器。你的任务不是写 SQL，而是选指标。

【可用指标（只能从这些里选，metric 必须原样照抄左边的英文名）】
{metrics}

【输出格式】只输出一个 JSON，不要解释，不要 markdown 代码块：
{{"metric": "<指标名>", "dims": ["<维度>", ...], "order_by": "<可选>", "order": "asc|desc", "limit": 500}}

【规则】
1. metric 必须来自上面的清单，一个字都不能改
2. dims 只能填该指标「可分组维度」里列出的那些；不能分组就填 []
3. 按时间看趋势 → dims 填 ["dt"]；只看一个总数 → dims 填 []
4. 想看排名 → 加 "order_by": "<指标名>", "order": "desc"
5. 只有当问题涉及的概念在字典里【完全不存在】时才输出 NOT_SUPPORTED（例如天气、退款、
   支付方式、竞品、明天会发生什么）。这些是能力边界，不是你偷懒的理由。
6. 如果概念存在、但粒度不够，【不要拒答】，返回最接近的指标：
   - 问「哪些人没买过」→ 返回 non_buyers（回答人数，并在结论里说明「我没有用户名单」）
   - 问「哪种产品卖的最多」→ 返回 item_buy_rank，dims 填 ["item_id"]，
     并加 "order_by": "item_buy_rank", "order": "desc"
   - 问「哪个类目最火」→ 返回 category_pv_rank，dims 填 ["category_id"]，同样降序
   粒度不够就给最接近的那个数，别拒答——拒答是最差的服务。
"""


def _strip_fence(s: str) -> str:
    return "\n".join(ln for ln in s.splitlines() if not ln.strip().startswith("```")).strip()


def llm_plan(q: str) -> dict:
    from openai import OpenAI

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"],
                    base_url=os.environ["OPENAI_BASE_URL"])
    resp = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": PLAN_PROMPT.format(metrics=metrics_catalog())
                                            + "\n\n" + render_fewshots(q)},
            {"role": "user", "content": q},
        ],
        temperature=0,
    )
    raw = (resp.choices[0].message.content or "").strip()

    if "NOT_SUPPORTED" in raw:
        raise NotSupported("模型判断：现有指标回答不了这个问题")

    return parse_plan(_strip_fence(raw))


# ============================================================
# 3. 管道：规划 -> 拼 SQL -> 守卫 -> 执行 -> 出话
# ============================================================
def ask(q: str, con):
    plan = llm_plan(q) if USE_LLM else rule_plan(q)   # ① 选指标
    sql = build_sql(plan)                             # ② 拼 SQL（缺指标在这里炸）
    safe_sql = guard(sql)                             # ③ 守卫
    df = con.execute(safe_sql).df()                   # ④ 执行
    return {"plan": plan, "sql": safe_sql, "df": df,
            "answer": _answer(plan["metric"], df)}


# ============================================================
# 4. 出话：一句话 / 明细 / 口径提示（口径提示直接来自字典）
# ============================================================
def _fmt_day(v) -> str:
    s = str(v)[:10]
    return s if s and s != "NaT" else "—"


def _fmt(v, metric: str) -> str:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)[:12]
    if f != f:
        return "—"
    if metric in PERCENT_METRICS:
        return f"{f:.2%}"
    return f"{f:,.0f}" if abs(f - round(f)) < 1e-9 else f"{f:,.2f}"


def _one_line(metric: str, df) -> str:
    m = METRICS[metric]
    unit = m.get("unit", "")

    if "dt" in df.columns:
        v = df[metric]
        best = df.loc[v.idxmax()]
        return (f"{m['cn']}在 {_fmt(v.min(), metric)} ~ {_fmt(v.max(), metric)} {unit}之间，"
                f"最高是 {_fmt_day(best['dt'])}（{_fmt(best[metric], metric)} {unit}）。")

    dims = [c for c in df.columns if c != metric]
    if not dims:
        return f"{m['cn']} = {_fmt(df[metric].iloc[0], metric)} {unit}"

    top = df.sort_values(metric, ascending=False).iloc[0]
    key = " / ".join(str(top[d]) for d in dims)
    return (f"{m['cn']}最高的是 {key}（{_fmt(top[metric], metric)} {unit}），"
            f"共 {len(df)} 组，合计 {_fmt(df[metric].sum(), metric)} {unit}。")


def _detail(metric: str, df) -> list:
    unit = METRICS[metric].get("unit", "")
    dims = [c for c in df.columns if c != metric]
    if not dims:
        return []

    out = []
    for r in df.itertuples(index=False):        # 不再重排：SQL 已经 ORDER BY 过了
        key = " / ".join(_fmt_day(getattr(r, d)) if d == "dt" else str(getattr(r, d)) for d in dims)
        out.append(f"{key} ｜ {_fmt(getattr(r, metric), metric)} {unit}")
    return out


def _answer(metric: str, df) -> str:
    if df.empty:
        return "没有查到数据。"

    parts = [f"【一句话】\n{_one_line(metric, df)}"]

    detail = _detail(metric, df)
    if detail:
        parts.append("【明细】\n" + "\n".join(f"- {x}" for x in detail))

    note = METRICS[metric].get("note")
    if note:
        parts.append(f"【口径提示】\n- {note}")

    return "\n\n".join(parts)


# ============================================================
# 5. 连接与演示
# ============================================================
def _connect():
    con = duckdb.connect()
    for name, pattern in {
        "dws_user_day":     f"{DATA}/dws/user_day/dt=*/*.parquet",
        "dws_item_day":     f"{DATA}/dws/item_day/dt=*/*.parquet",
        "dws_category_day": f"{DATA}/dws/category_day/dt=*/*.parquet",
        "ads_traffic":      f"{DATA}/ads/ads_traffic/*.parquet",
        "ads_retention":    f"{DATA}/ads/ads_retention/*.parquet",
        "ads_funnel":       f"{DATA}/ads/ads_funnel/*.parquet",
        "ads_rf_segment":   f"{DATA}/ads/ads_rf_segment/*.parquet",
    }.items():
        con.execute(
            f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true)"
        )
    return con


QUESTIONS = [
    "每天有多少人来？",
    "这九天一共浏览了多少次？",
    "每天有多少人买了东西？",
    "购买率怎么样？",
    "人均浏览多少页？",
    "次留情况如何？",
    "转化率是多少？",
    "人群分层各有多少人？",
    "有多少人是不在这九天里买过东西的？",
    "加购率怎么样？",
    "昨天的天气怎么样？",
    "根据已有数据推测明天谁会买？",
    "哪些人没买过？",
    "哪种产品卖的最多？",
]


if __name__ == "__main__":
    con = _connect()
    print(f"[L1 语义层] USE_LLM = {USE_LLM}  model = {LLM_MODEL}  "
          f"指标 {len(METRICS)} 条")
    for q in QUESTIONS:
        print("\n" + "=" * 70)
        print(f"问：{q}")
        try:
            r = ask(q, con)
            print(f"计划：{r['plan']}")
            print(f"SQL ：{r['sql']}")
            print(f"行数：{len(r['df'])}")
            print(f"答  ：\n{r['answer']}")
        except NotSupported as e:
            print(f"拒答：{e}")
        except UnknownMetric as e:
            print(f"缺指标：{e}")
        except BadPlan as e:
            print(f"计划不合法：{e}")
        except GuardError as e:
            print(f"守卫拦下：{e}")