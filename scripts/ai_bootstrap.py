# scripts/ai_bootstrap.py —— AI 层地基：7 张表挂成视图 + 行数锚点 + 跨层对账
import duckdb

DATA = "F:/data_warehouse/data"

# 名字 -> parquet 路径模式（dt=* 表示所有分区）
VIEWS = {
    "dws_user_day":     f"{DATA}/dws/user_day/dt=*/*.parquet",
    "dws_item_day":     f"{DATA}/dws/item_day/dt=*/*.parquet",
    "dws_category_day": f"{DATA}/dws/category_day/dt=*/*.parquet",
    "ads_traffic":      f"{DATA}/ads/ads_traffic/*.parquet",
    "ads_retention":    f"{DATA}/ads/ads_retention/*.parquet",
    "ads_funnel":       f"{DATA}/ads/ads_funnel/*.parquet",
    "ads_rf_segment":   f"{DATA}/ads/ads_rf_segment/*.parquet",
}

# 行数锚点：Spark 侧已经验证过的数，任一不符就停
EXPECT = {
    "dws_user_day":     6_968_544,
    "dws_item_day":     15_183_398,
    "dws_category_day": 73_629,
    "ads_traffic":      9,
    "ads_retention":    9,
    "ads_funnel":       9,
    "ads_rf_segment":   10,
}


def connect():
    """内存库 + 7 个视图。所有 AI 代码都从这里拿连接。"""
    con = duckdb.connect()
    for name, pattern in VIEWS.items():
        con.execute(
            f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true)"
        )
    return con


def check_rowcounts(con):
    print("\n=== 1. 行数锚点 ===")
    all_ok = True
    for name, want in EXPECT.items():
        got = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        ok = (got == want)
        all_ok = all_ok and ok
        print(f"{'[OK]  ' if ok else '[FAIL]'} {name:18s} {got:>12,}   （期望 {want:,}）")
    assert all_ok, "行数与 Spark 侧对不上，先别往下走"


def show_schema(con):
    print("\n=== 2. 每张表有哪些列（给 G3 口径字典用）===")
    for name in VIEWS:
        cols = con.execute(f"DESCRIBE {name}").fetchall()
        names = ", ".join(f"{r[0]}:{r[1]}" for r in cols)
        print(f"\n{name}\n  {names}")


def check_cross_layer(con):
    """从 DWS 重算每日购买人数，和 ADS 漏斗表里已有的数比对。"""
    print("\n=== 3. 跨层对账：DWS 现算 vs ADS 已有 ===")
    df = con.execute("""
        WITH a AS (
            SELECT CAST(dt AS VARCHAR) AS dt, SUM(has_buy) AS buy_uv_recalc
            FROM dws_user_day
            GROUP BY dt
        ),
        b AS (
            SELECT CAST(dt AS VARCHAR) AS dt, buy_total_uv
            FROM ads_funnel
        )
        SELECT a.dt, a.buy_uv_recalc, b.buy_total_uv
        FROM a JOIN b USING (dt)
        ORDER BY a.dt
    """).df()

    df["对账"] = (df["buy_uv_recalc"] == df["buy_total_uv"]).map({True: "[OK]", False: "[FAIL]"})
    print(df.to_string(index=False))

    assert len(df) == 9, f"应当 9 行，实际 {len(df)} 行"
    assert (df["buy_uv_recalc"] == df["buy_total_uv"]).all(), "跨层对账失败"
    print("\n跨层对账 = [OK]")


def main():
    con = connect()
    check_rowcounts(con)
    show_schema(con)
    check_cross_layer(con)


if __name__ == "__main__":
    main()