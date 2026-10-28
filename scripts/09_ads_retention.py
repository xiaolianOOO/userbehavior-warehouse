# ============================================================
# scripts/09_ads_retention.py  —  ADS 留存表（次日 + 3 日）
# 输入：data/dws/user_day        人×日，6,968,544 行
# 输出：data/ads/ads_retention   9 行（末尾几天部分指标为 NULL）
# ============================================================
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"

from pyspark.sql import SparkSession, functions as F

DWS_USER = "F:/data_warehouse/data/dws/user_day"
ADS_OUT  = "F:/data_warehouse/data/ads/ads_retention"

DWS_ROWS = 6_968_544
DT_COUNT = 9
LAST_DT  = "2017-12-03"

spark = (SparkSession.builder
         .appName("ads_retention")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    d = spark.read.parquet(DWS_USER)

    # ---- 1) 瘦身：只留两列（★1 这一步决定 join 会不会炸）----
    base = d.select(F.to_date("dt").alias("dt"), "user_id").cache()
    n = base.count()
    assert n == DWS_ROWS, f"{n:,} != {DWS_ROWS:,}"
    print(f"[1] 瘦身后 {n:,} 行   列 = {base.columns}")

    # ---- 2) 每日 UV（留存率的分母）----
    uv = base.groupBy("dt").agg(F.count("*").alias("uv"))

    # ---- 3) 留存：把"当天"和"偏移 N 天后"接起来 ----
    def retained(offset):
        left  = base.withColumn("dt_next", F.date_add("dt", offset))
        right = base.withColumnRenamed("dt", "dt_next")
        # ★2 left_semi：只保留"右表里找得到"的左表行，一行不会变多行
        hit = left.join(right, ["user_id", "dt_next"], "left_semi")
        return hit.groupBy("dt").agg(F.count("*").alias(f"ret_{offset}d"))

    r1 = retained(1)
    r3 = retained(3)

    # ---- 4) 拼成一张表 ----
    t = (uv.join(r1, "dt", "left")
           .join(r3, "dt", "left")
           .fillna(0, subset=["ret_1d", "ret_3d"]))

    # ---- 5) 算留存率：★3 只算"配得上对"的日子 ----
    last = F.to_date(F.lit(LAST_DT))
    t = (t.withColumn("d1_rate",
                      F.when(F.date_add("dt", 1) <= last,
                             F.round(F.col("ret_1d") / F.col("uv"), 4)))
          .withColumn("d3_rate",
                      F.when(F.date_add("dt", 3) <= last,
                             F.round(F.col("ret_3d") / F.col("uv"), 4)))
          .orderBy("dt"))

    # ---- 6) 对账 ----
    rows = t.count()
    s = t.agg(F.sum("uv").alias("uv")).first()
    bad = t.filter(F.col("ret_1d") > F.col("uv")).count()
    n_d1 = t.filter(F.col("d1_rate").isNotNull()).count()
    n_d3 = t.filter(F.col("d3_rate").isNotNull()).count()

    assert rows == DT_COUNT, f"行数 {rows} != {DT_COUNT}"
    assert s["uv"] == DWS_ROWS, f"Σuv {s['uv']:,} != {DWS_ROWS:,}"
    assert bad == 0, f"有 {bad} 天留存人数 > UV，算法一定错了"
    assert n_d1 == 8, f"次日留存有效天数 {n_d1} != 8"      # 12-03 没有 12-04
    assert n_d3 == 6, f"3 日留存有效天数 {n_d3} != 6"      # 12-01 以后没有第 4 天
    print(f"[2] 行数 = {rows}   Σuv = {s['uv']:,}   "
          f"次日有效 {n_d1} 天   3日有效 {n_d3} 天   对账 = [OK]")

    # ---- 7) 落盘 ----
    t.write.mode("overwrite").parquet(ADS_OUT)
    print(f"[3] 写盘完成 -> {ADS_OUT}")

    back = spark.read.parquet(ADS_OUT)
    assert back.count() == rows
    print(f"[4] 读回 {back.count()} 行   对账 = [OK]")

    print("\n=== 留存表 ===")
    back.orderBy("dt").show(20, truncate=False)

except Exception as e:
    print("\n!!! 出错：", type(e).__name__, e)
    raise
finally:
    spark.stop()