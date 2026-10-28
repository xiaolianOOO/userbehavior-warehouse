# scripts/06_dws_item.py — DWS：商品×日
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"

from pyspark.sql import SparkSession, functions as F

DWD_MAIN = "F:/data_warehouse/data/dwd/user_behavior"
DWS_ITEM = "F:/data_warehouse/data/dws/item_day"

DWD_ROWS = 100_095_182
EXPECT = {"pv": 89_660_671, "cart": 5_530_446, "fav": 2_888_258, "buy": 2_015_807}

spark = (SparkSession.builder
         .appName("dws_item_day")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(DWD_MAIN)

    # ================= ① 聚合：一件商品一天 = 一行 =================
    item_day = (df.groupBy("dt", "item_id")                    # ★1 主键
        .agg(
            F.count("*").alias("n_action"),
            F.sum(F.when(F.col("behavior_type") == "pv",   1).otherwise(0)).alias("pv_cnt"),
            F.sum(F.when(F.col("behavior_type") == "cart", 1).otherwise(0)).alias("cart_cnt"),
            F.sum(F.when(F.col("behavior_type") == "fav",  1).otherwise(0)).alias("fav_cnt"),
            F.sum(F.when(F.col("behavior_type") == "buy",  1).otherwise(0)).alias("buy_cnt"),
            F.countDistinct("user_id").alias("uv"),            # ★2 真·去重，最贵
        )
        .withColumn("has_cart", (F.col("cart_cnt") > 0).cast("int"))
        .withColumn("has_fav",  (F.col("fav_cnt")  > 0).cast("int"))
        .withColumn("has_buy",  (F.col("buy_cnt")  > 0).cast("int"))
        .withColumn("conv_rate",                          # 下单人数 / 浏览人数（可空）
            F.when(F.col("pv_cnt") > 0, F.col("buy_cnt") / F.col("pv_cnt")))
    )

    # ================= ② 落盘 =================
    item_day.write.mode("overwrite").partitionBy("dt").parquet(DWS_ITEM)

    # ================= ③ 对账（三层） =================
    back = spark.read.parquet(DWS_ITEM)
    n_rows = back.count()

    s = back.agg(
        F.sum("n_action").alias("act"),
        F.sum("pv_cnt").alias("pv"),
        F.sum("cart_cnt").alias("cart"),
        F.sum("fav_cnt").alias("fav"),
        F.sum("buy_cnt").alias("buy"),
    ).first()

    print(f"[1] 商品日行数 = {n_rows:,}")
    print(f"[2] 行为总数   = {s['act']:,}")
    print(f"[3] pv={s['pv']:,}  cart={s['cart']:,}  fav={s['fav']:,}  buy={s['buy']:,}")

    assert s["act"] == DWD_ROWS, f"行为总数 {s['act']:,} != {DWD_ROWS:,}"
    assert s["pv"]   == EXPECT["pv"],   f"pv  {s['pv']:,}  != {EXPECT['pv']:,}"
    assert s["cart"] == EXPECT["cart"], f"cart {s['cart']:,} != {EXPECT['cart']:,}"
    assert s["fav"]  == EXPECT["fav"],  f"fav  {s['fav']:,} != {EXPECT['fav']:,}"
    assert s["buy"]  == EXPECT["buy"],  f"buy  {s['buy']:,} != {EXPECT['buy']:,}"
    print("[4] 行级守恒 = [OK]")

    uv_tbl = back.groupBy("dt").count()
    sum_uv = uv_tbl.agg(F.sum("count").alias("s")).first()["s"]
    n_null = back.filter(F.col("dt").isNull()).count()
    assert n_null == 0, f"dt 为 NULL 的行 = {n_null:,}"
    assert sum_uv == n_rows, f"每日行数之和 {sum_uv:,} != {n_rows:,}"
    print("[5] 日粒度守恒 = [OK]")

    # ================= ④ 长尾体检（这张表最值钱的部分） =================
    print("\n=== 每件商品在这 9 天里出现了几天 ===")
    days = back.groupBy("item_id").agg(F.countDistinct("dt").alias("days"))
    days.groupBy("days").count().orderBy("days").show()

    print("=== 每天平均 pv vs 中位数 pv（同一件商品）===")
    back.agg(
        F.avg("pv_cnt").alias("平均"),
        F.expr("percentile_approx(pv_cnt, 0.5)").alias("中位数"),
        F.max("pv_cnt").alias("最高"),
    ).show()

    print("=== TOP10 爆款（按总 pv）===")
    (back.groupBy("item_id").agg(F.sum("pv_cnt").alias("总pv"))
         .orderBy(F.desc("总pv")).show(10))
finally:
    spark.stop()