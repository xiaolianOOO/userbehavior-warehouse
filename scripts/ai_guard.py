# scripts/ai_guard.py —— SQL 守卫：所有交给 duckdb 执行的 SQL 必须先过这里
import sqlglot
from sqlglot import exp

# 白名单：只允许查这 7 张表（和 ai_bootstrap.py 保持一致）
ALLOW_TABLES = {
    "dws_user_day", "dws_item_day", "dws_category_day",
    "ads_traffic", "ads_retention", "ads_funnel", "ads_rf_segment",
}

# 有 dt 的表：查询必须显式给出 dt 范围；ads_rf_segment 是按人聚合的，没有 dt
DT_TABLES = ALLOW_TABLES - {"ads_rf_segment"}

MAX_LIMIT = 500

# 比较类节点：只有出现在这些节点里的 dt 才算"真的在过滤"
CMP_NODES = (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.Between, exp.In)


class GuardError(Exception):
    """守卫拦下的查询。消息直接给人看。"""


def _has_dt_filter(stmt):
    """在 WHERE 子树里找 dt，且它必须处在比较/范围/IN 里。"""
    for where in stmt.find_all(exp.Where):
        for cmp_node in where.find_all(*CMP_NODES):
            for col in cmp_node.find_all(exp.Column):
                if col.name.lower() == "dt":
                    return True
    return False


def guard(sql: str, allow_tables=None, dt_tables=None, max_limit: int = MAX_LIMIT) -> str:
    """
    校验并（必要时）改写 SQL。通过则返回可安全执行的 SQL，否则抛 GuardError。
    """
    allow_tables = allow_tables or ALLOW_TABLES
    dt_tables = dt_tables or DT_TABLES

    # 1) 必须能解析
    try:
        stmts = [s for s in sqlglot.parse(sql, read="duckdb") if s is not None]
    except Exception as e:
        raise GuardError(f"SQL 解析失败：{e}")

    # 2) 必须恰好一条语句（防 "SELECT 1; DROP TABLE x"）
    if len(stmts) != 1:
        raise GuardError(f"只允许一条语句，检测到 {len(stmts)} 条")

    stmt = stmts[0]

    # 3) 必须是 SELECT（WITH ... SELECT 也解析成 Select，正常放行）
    if not isinstance(stmt, exp.Select):
        raise GuardError(f"只允许 SELECT，检测到 {type(stmt).__name__}")

    # 4) 表必须在白名单，且不能是表函数（read_parquet / read_csv / glob 这些都是绕过口子）
    used = set()
    for t in stmt.find_all(exp.Table):
        if not isinstance(t.this, exp.Identifier):
            raise GuardError(f"禁止使用表函数：{t.sql()}")
        name = t.name.lower()
        if name not in allow_tables:
            raise GuardError(f"表 {t.name} 不在白名单；可用：{', '.join(sorted(allow_tables))}")
        used.add(name)

    # 5) 有 dt 的表必须给 dt 范围
    for name in used & dt_tables:
        if not _has_dt_filter(stmt):
            raise GuardError(
                f"查询 {name} 必须带 dt 范围过滤，"
                f"例如 dt BETWEEN '2017-11-25' AND '2017-12-03'"
            )

    # 6) LIMIT：没写、或者写超了，就包一层压到 max_limit
    need_wrap = True
    lim = stmt.args.get("limit")
    if lim is not None:
        try:
            if int(lim.expression.this) <= max_limit:
                need_wrap = False
        except Exception:
            need_wrap = True

    if need_wrap:
        return (
            f"SELECT * FROM (\n{sql}\n) AS _guard LIMIT {max_limit}"
        )
    return sql


# ============ 自测：8 条用例，3 条该过、5 条该拦 ============
CASES = [
    ("正常查询", "SELECT count(*) FROM dws_user_day WHERE dt = '2017-12-02'", True),
    ("非 SELECT", "DROP TABLE dws_user_day", False),
    ("多语句", "SELECT count(*) FROM dws_user_day WHERE dt='2017-12-02'; DROP TABLE dws_user_day", False),
    ("缺 dt 过滤", "SELECT count(*) FROM dws_user_day", False),
    ("非白名单表", "SELECT * FROM raw_user_behavior WHERE dt='2017-12-02'", False),
    ("表函数绕过", "SELECT * FROM read_parquet('F:/data_warehouse/data/raw/UserBehavior.csv')", False),
    ("LIMIT 超限", "SELECT * FROM dws_user_day WHERE dt='2017-12-02' LIMIT 100000", True),
    ("无 dt 表豁免", "SELECT count(*) FROM ads_rf_segment", True),
]


def _selftest():
    print("=== 守卫自测 ===")
    passed = 0
    for title, sql, should_pass in CASES:
        try:
            out = guard(sql)
            ok = should_pass
            tail = "（已改写为 LIMIT 500）" if out != sql else ""
            print(f"{'[OK]  ' if ok else '[FAIL]'} 放行 | {title}{tail}")
        except GuardError as e:
            ok = not should_pass
            print(f"{'[OK]  ' if ok else '[FAIL]'} 拦下 | {title} —— {e}")
        passed += ok
    print(f"\n自测结果：{passed}/{len(CASES)}")
    assert passed == len(CASES), "守卫自测未全过"


if __name__ == "__main__":
    _selftest()