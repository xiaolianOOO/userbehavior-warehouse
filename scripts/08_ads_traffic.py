# ============================================================
# scripts/08_ads_traffic.py  —  ADS 流量总览（每天一行，共 9 行）
# 输入：data/dws/user_day     人×日，6,968,544 行
# 输出：data/ads/ads_traffic  9 行
# ============================================================
import sys
sys.stdout.reconfigure(encoding="utf-8")          # Windows 控制台是 GBK，先改 UTF-8

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"
# ↑ 必须在 SparkSession.builder 之前：JVM 一启动就把环境抄走

from pyspark.sql import SparkSession, functions as F, Window

# ---------- 路径 ----------
DWS_USER = "F:/data_warehouse/data/dws/user_day"
ADS_OUT  = "F:/data_warehouse/data/ads/ads_traffic"

# ---------- 参照值（DWS 阶段实测，写死当尺子）----------
DWS_ROWS = 6_968_544
DT_COUNT = 9
EXPECT = {"pv": 89_660_671, "cart": 5_530_446, "fav": 2_888_258, "buy": 2_015_807}

spark = (SparkSession.builder
         .appName("ads_traffic")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    d = spark.read.parquet(DWS_USER)
    print("列名 =", d.columns)      # 第一次跑先看一眼，确认和下面一致

    # ---- 1) 按天汇总 ----
    t = (d.groupBy("dt").agg(
            F.count("*").alias("uv"),                  # 一行=一个人，所以行数就是 UV
            F.sum("pv_cnt").alias("pv_cnt"),
            F.sum("cart_cnt").alias("cart_cnt"),
            F.sum("fav_cnt").alias("fav_cnt"),
            F.sum("buy_cnt").alias("buy_cnt"),
            F.sum("has_cart").alias("cart_uv"),        # ★ 加购人数（不是加购次数）
            F.sum("has_fav").alias("fav_uv"),
            F.sum("has_buy").alias("buy_uv"),
         ))

    # ---- 2) 派生指标 ----
    t = (t.withColumn("pv_per_uv", F.round(F.col("pv_cnt") / F.col("uv"), 2))
          .withColumn("cart_rate", F.round(F.col("cart_uv") / F.col("uv"), 4))
          .withColumn("fav_rate",  F.round(F.col("fav_uv")  / F.col("uv"), 4))
          .withColumn("buy_rate",  F.round(F.col("buy_uv")  / F.col("uv"), 4)))

    # ---- 3) 日环比（今天 vs 昨天，第一天是 null）----
    w = Window.orderBy("dt")
    t = (t.withColumn("uv_dod",  F.round((F.col("uv")       - F.lag("uv").over(w))       / F.lag("uv").over(w), 4))
          .withColumn("pv_dod",  F.round((F.col("pv_cnt")   - F.lag("pv_cnt").over(w))   / F.lag("pv_cnt").over(w), 4))
          .withColumn("pxu_dod", F.round((F.col("pv_per_uv")- F.lag("pv_per_uv").over(w))/ F.lag("pv_per_uv").over(w), 4))
          .orderBy("dt"))

    # ---- 4) 对账 ----
    rows = t.count()
    s = t.agg(F.sum("uv").alias("uv"), F.sum("pv_cnt").alias("pv"),
              F.sum("cart_cnt").alias("cart"), F.sum("fav_cnt").alias("fav"),
              F.sum("buy_cnt").alias("buy")).first()

    assert rows == DT_COUNT, f"行数 {rows} != {DT_COUNT}"
    assert s["uv"] == DWS_ROWS, f"Σuv {s['uv']:,} != {DWS_ROWS:,}"
    for k in ("pv", "cart", "fav", "buy"):
        assert s[k] == EXPECT[k], f"{k} {s[k]:,} != {EXPECT[k]:,}"
    print(f"[1] 行数 = {rows}   Σuv = {s['uv']:,}   对账 = [OK]")

    # ---- 5) 落盘：小表不分区，dt 就留在文件里当普通列 ----
    t.write.mode("overwrite").parquet(ADS_OUT)
    print(f"[2] 写盘完成 -> {ADS_OUT}")

    back = spark.read.parquet(ADS_OUT)
    n = back.count()
    assert n == rows, f"读回 {n} != {rows}"
    print(f"[3] 读回 {n} 行   对账 = [OK]")

    # ---- 6) 给人看 ----
    print("\n=== 每日流量总览 ===")
    (back.select("dt", "uv", "pv_cnt", "pv_per_uv",
                 "cart_rate", "fav_rate", "buy_rate", "uv_dod")
         .orderBy("dt").show(20, truncate=False))

except Exception as e:
    print("\n!!! 出错：", type(e).__name__, e)
    raise
finally:
    spark.stop()