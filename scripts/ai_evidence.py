# scripts/ai_evidence.py —— G6 数字溯源层（evidence）
#
# 要解决的问题：
#   任何一句结论里出现的数字，都必须能追到「哪张表、哪条 SQL、跑出来什么」。
#   模型只负责组织语言，不负责产生数字；数字一律由这里从数据库取。
#
# 三件事：
#   1) collect()      跑一批固定指标，把每个数字登记成一条 evidence（带 SQL）
#   2) check_text()   把一段话里所有阿拉伯数字抠出来，逐个核对有没有出处
#   3) render()       打印 evidence 清单，供人工复核 / 进简报附录
#
# 铁律：正文里不许出现没有出处的阿拉伯数字。
#       结构性数字（"九天"、"十格"、"四类"）请用中文写，否则校验器会拦。

import re

from ai_guard import guard
from ai_semantic import build_sql

# ---------------------------------------------------------------------------
# 一、要采集哪些指标（固定清单，不走模型）
#     简报是每天固定出的一份，选指标这件事不该靠模型，靠配置。
# ---------------------------------------------------------------------------

COLLECT_PLANS = [
    {"metric": "uv",                 "dims": ["dt"]},                 # 日活
    {"metric": "pv_total",           "dims": []},                     # 总浏览次数
    {"metric": "buyers_9d",          "dims": []},                     # 9 天累计买家
    {"metric": "non_buyers",         "dims": []},                     # 未购买人数
    {"metric": "buy_rate",           "dims": ["dt"]},                 # 日购买率
    {"metric": "d1_rate",            "dims": ["dt"]},                 # 次日留存率
    {"metric": "conv_browse_to_buy", "dims": ["dt"]},                 # 浏览→购买转化率
    {"metric": "rf_users",           "dims": ["r_seg", "f_seg"]},     # RF 十格
]

# ---------------------------------------------------------------------------
# 二、采集
# ---------------------------------------------------------------------------

def _fmt(v) -> str:
    """把数字格式化成给人看的样子：整数带千分位，浮点最多四位。"""
    if isinstance(v, float):
        return f"{v:,.4f}".rstrip("0").rstrip(".")
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return str(v)


def collect(con, plans=None):
    """
    跑一遍采集清单，返回 evidence 列表。
    每条 evidence：
      {id, metric, label, value, shown, unit, sql, table, rows}
    """
    plans = plans or COLLECT_PLANS
    evs = []
    n = 0

    for plan in plans:
        plan = dict(plan)                      # 不改调用方的字典
        plan.setdefault("dims", [])            # build_sql 默认调用方已洗完，这里补齐
        plan.setdefault("limit", 500)          # 不补就 KeyError: 'limit'
        metric = plan["metric"]
        sql = guard(build_sql(plan))           # 走同一个守卫，和线上问数一条路
        df = con.execute(sql).df()
        if df.empty:
            continue

        val_col = df.columns[-1]               # 最后一列就是数值列，不依赖别名
        day_col = "dt" if "dt" in df.columns else None
        unit = {"uv": "人", "buy_users": "人", "buyers_9d": "人",
                "non_buyers": "人", "rf_users": "人",
                "pv_total": "次", "buy_actions": "次"}.get(metric, "%")

        def add(label, value):
            nonlocal n
            v = float(value)
            if v != v:              # NaN：12-03 的留存率就是这样，没资格算就别登记
                return
            n += 1
            evs.append({
                "id": f"E{n:02d}",
                "metric": metric,
                "label": label,
                "value": v,
                "shown": _fmt(v),
                "unit": unit,
                "sql": sql.strip().replace("\n", " "),
                "table": plan.get("table") or "",
                "rows": len(df),
            })

        if day_col:
            # 按天：登记首日、末日、最小、最大（值的极值 + 对应日期）
            first = df.iloc[0]
            last = df.iloc[-1]
            add(f"首日（{str(first[day_col])[:10]}）", first[val_col])
            add(f"末日（{str(last[day_col])[:10]}）", last[val_col])

            ok = df.dropna(subset=[val_col])
            if not ok.empty:
                i_min, i_max = ok[val_col].idxmin(), ok[val_col].idxmax()
                add(f"最低（{str(ok.loc[i_min, day_col])[:10]}）", ok.loc[i_min, val_col])
                add(f"最高（{str(ok.loc[i_max, day_col])[:10]}）", ok.loc[i_max, val_col])
        else:
            # 没有 dt：可能真是一行一列，也可能有维度但没按天（rf_users 十格）
            if len(df) == 1:
                add("全期合计", df[val_col].iloc[0])
            else:
                dim_cols = [c for c in df.columns if c != val_col]
                for _, row in df.iterrows():
                    tag = "_".join(str(row[c]) for c in dim_cols)   # 如 R1_F1
                    add(tag, row[val_col])
                add("全期合计", df[val_col].sum())

    return evs


# ---------------------------------------------------------------------------
# 三、校验：正文里的每个阿拉伯数字都必须有出处
# ---------------------------------------------------------------------------

NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _index(evs):
    """把 evidence 的数值建成 值 -> evidence 的索引。

    同时收两种键：
      value —— 数据库原值，可能是 0.1790520...
      shown —— 显示值，正文里写的就是 0.1791
    只收 value 的话，正文写 0.1791 会被判成"无出处"——这是误报，不是写错。
    """
    idx = {}
    for e in evs:
        keys = {round(float(e["value"]), 6)}
        try:
            keys.add(round(float(str(e["shown"]).replace(",", "")), 6))
        except ValueError:
            pass
        for k in keys:
            idx.setdefault(k, []).append(e)
    return idx


def check_text(text: str, evs, tol=1e-6):
    """
    返回 (ok, 无出处的数字列表, 命中的 evidence 列表)。
    允许浮点误差；千分位逗号会被忽略。
    """
    idx = _index(evs)
    bad, hit = [], []

    for tok in NUM_RE.findall(text):
        try:
            v = float(tok.replace(",", ""))
        except ValueError:
            bad.append(tok)
            continue

        found = None
        for key in idx:
            if abs(key - v) <= tol or (v != 0 and abs((key - v) / v) <= tol):
                found = idx[key]
                break

        if found:
            hit.extend(found)
        else:
            bad.append(tok)

    return (len(bad) == 0), bad, hit


# ---------------------------------------------------------------------------
# 四、打印
# ---------------------------------------------------------------------------

def render(evs) -> str:
    lines = [f"{'ID':<5}{'指标':<20}{'说明':<22}{'数值':>14}  行数"]
    lines.append("-" * 76)
    for e in evs:
        lines.append(f"{e['id']:<5}{e['metric']:<20}{e['label']:<22}{e['shown']:>14}  {e['rows']}")
    return "\n".join(lines)


def render_sql(evs) -> str:
    lines = []
    for e in evs:
        lines.append(f"[{e['id']}] {e['metric']} —— {e['label']}")
        lines.append(f"    {e['sql']}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 五、自测
# ---------------------------------------------------------------------------

def _load_bootstrap():
    """14_ai_bootstrap.py 文件名以数字开头，不能直接 import，只能用 importlib。"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "boot", r"F:\data_warehouse\scripts\14_ai_bootstrap.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


if __name__ == "__main__":
    con = _load_bootstrap().connect()

    evs = collect(con)
    print(f"=== 1. 采集到 {len(evs)} 条 evidence ===")
    print(render(evs))

    print("\n=== 2. 抽样看两条 SQL ===")
    print(render_sql(evs[:2]))

    # 造一句话：数字全部来自上面，应当通过
    good = (f"九天共有 {evs[5]['shown']} 人买过东西，"
            f"另有 {evs[6]['shown']} 人一次都没买。")
    ok, bad, hit = check_text(good, evs)
    print("\n=== 3. 正常句子（应通过）===")
    print(f"句子：{good}")
    print(f"结果：{'[OK]' if ok else '[FAIL]'} 无出处的数字 = {bad}")

    # 造一句话：故意改错一个数字，应当被抓住
    wrong = good.replace(evs[6]["shown"], "999,999")
    ok2, bad2, _ = check_text(wrong, evs)
    print("\n=== 4. 故意写错一个数（应被拦住）===")
    print(f"句子：{wrong}")
    print(f"结果：{'[OK]' if ok2 else '[FAIL]'} 无出处的数字 = {bad2}")
    print("\n判定：", "[OK] 溯源校验器工作正常" if (ok and not ok2) else "[FAIL] 校验器有问题")
