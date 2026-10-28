# ============================================================
# scripts/10_ads_funnel.py  —  ADS 漏斗表（集合漏斗，不判时序）
# 输入：data/dws/user_day       人×日，6,968,544 行
# 输出：data/ads/ads_funnel     9 行（每天一行）
# ============================================================
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"

from pyspark.sql import SparkSession, functions as F

DWS_USER = "F:/data_warehouse/data/dws/user_day"
ADS_OUT  = "F:/data_warehouse/data/ads/ads_funnel"

DWS_ROWS    = 6_968_544
DT_COUNT    = 9
PV_UV_ALL   = 984_114     # 全量口径有浏览的去重用户（01_profile 段 2b）
BUY_UV_ALL  = 672_404     # 全量口径买过的去重用户（同上）

spark = (SparkSession.builder
         .appName("ads_funnel")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    d = spark.read.parquet(DWS_USER)

    # ---- 1) 三个集合条件（★1 只看"有没有做过"，不看顺序）----
    IS_PV   = F.col("has_pv")   == 1
    IS_CART = F.col("has_cart") == 1
    IS_BUY  = F.col("has_buy")  == 1
    S = lambda cond: F.sum(F.when(cond, 1).otherwise(0))   # 满足条件的人数

    # ---- 2) 日粒度：每天一行 ----
    t = (d.groupBy("dt").agg(
            F.count("*").alias("uv"),                        # 当天总人数
            S(IS_PV).alias("s1_uv"),                          # 第1步：浏览
            S(IS_PV & IS_CART).alias("s2_uv"),                # 第2步：浏览 + 加购
            S(IS_PV & IS_CART & IS_BUY).alias("s3_uv"),       # 第3步：浏览 + 加购 + 购买
            S(IS_BUY).alias("buy_total_uv"),                  # 当天买过的人（不分路径）
            S(IS_PV & (~IS_CART) & IS_BUY).alias("buy_pv_nocart"),      # 买了，没加购
            S((~IS_PV) & IS_CART & IS_BUY).alias("buy_nopv_cart"),      # 买了，没浏览
            S((~IS_PV) & (~IS_CART) & IS_BUY).alias("buy_nopv_nocart"), # 直接买
         ))

    # ---- 3) 转化率 ----
    t = (t.withColumn("r1", F.round(F.col("s2_uv") / F.col("s1_uv"), 4))            # 浏览→加购
          .withColumn("r2", F.round(F.col("s3_uv") / F.col("s2_uv"), 4))            # 加购→购买
          .withColumn("r_total", F.round(F.col("s3_uv") / F.col("s1_uv"), 4))       # 浏览→购买
          .orderBy("dt"))

    # ---- 4) 对账 ----
    rows = t.count()
    s = t.agg(F.sum("uv").alias("uv"), F.sum("buy_total_uv").alias("buy")).first()

    assert rows == DT_COUNT, f"行数 {rows} != {DT_COUNT}"
    assert s["uv"] == DWS_ROWS, f"Σuv {s['uv']:,} != {DWS_ROWS:,}"

    # 每行必须满足 漏斗台阶且四类买家人群相加 = 买过的人（分类穷尽）
    bad = t.filter(
        (F.col("s3_uv") > F.col("s2_uv")) |
        (F.col("s2_uv") > F.col("s1_uv")) |
        (F.col("s1_uv") > F.col("uv")) |
        (F.col("buy_total_uv") != F.col("s3_uv") + F.col("buy_pv_nocart")
                                   + F.col("buy_nopv_cart") + F.col("buy_nopv_nocart"))
    ).count()
    assert bad == 0, f"有 {bad} 天不满足漏斗台阶或买家人群不守恒"
    print(f"[1] 行数 = {rows}   Σuv = {s['uv']:,}   漏斗台阶 + 买家四分类 = [OK]")

    # ---- 5) 落盘 ----
    t.write.mode("overwrite").parquet(ADS_OUT)
    print(f"[2] 写盘完成 -> {ADS_OUT}")
    back = spark.read.parquet(ADS_OUT)
    assert back.count() == rows
    print(f"[3] 读回 {back.count()} 行   对账 = [OK]")

    print("\n=== 每日漏斗 ===")
    (back.select("dt", "uv", "s1_uv", "s2_uv", "s3_uv",
                 "r1", "r2", "r_total",
                 "buy_total_uv", "buy_pv_nocart", "buy_nopv_cart", "buy_nopv_nocart")
         .orderBy("dt").show(20, truncate=False))

    # ---- 6) 9 天整体漏斗（★3 不能把 9 行相加）----
    u = d.groupBy("user_id").agg(F.max("has_pv").alias("has_pv"),
                                 F.max("has_cart").alias("has_cart"),
                                 F.max("has_buy").alias("has_buy"))
    o = u.agg(S(IS_PV).alias("s1"),
              S(IS_PV & IS_CART).alias("s2"),
              S(IS_PV & IS_CART & IS_BUY).alias("s3"),
              S(IS_BUY).alias("buy_total")).first()

    assert o["s1"] <= PV_UV_ALL,  f"窗口内浏览人数 {o['s1']:,} > 全量 {PV_UV_ALL:,}"
    assert o["buy_total"] <= BUY_UV_ALL, f"窗口内买家 {o['buy_total']:,} > 全量 {BUY_UV_ALL:,}"

    print("\n=== 9 天整体漏斗（按人去重，不能把上面 9 行相加）===")
    print(f"  浏览 {o['s1']:,} → 浏览+加购 {o['s2']:,} → 浏览+加购+购买 {o['s3']:,}")
    print(f"  买过的人合计 {o['buy_total']:,}   （全量口径 {BUY_UV_ALL:,}，窗口内必然 ≤ 全量）")
    print(f"  整体转化：浏览→加购 {o['s2']/o['s1']:.2%}   加购→购买 {o['s3']/o['s2']:.2%}   "
          f"浏览→购买 {o['s3']/o['s1']:.2%}")
    skip_people = o["buy_total"] - o["s3"]          # 人数：买过但没走完三步
    print(f"  不走标准路径的买家：{skip_people:,} 人 "
          f"（占买家的 {skip_people / o['buy_total']:.2%}）")
    print(f"  注：日表里 buy_pv_nocart 等三列相加是「人次」，跨天不能相加")

except Exception as e:
    print("\n!!! 出错：", type(e).__name__, e)
    raise
finally:
    spark.stop()