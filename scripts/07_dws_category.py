# scripts/07_dws_category.py — DWS：类目×日
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"

from pyspark.sql import SparkSession, functions as F

DWD_MAIN = "F:/data_warehouse/data/dwd/user_behavior"
DWS_CAT  = "F:/data_warehouse/data/dws/category_day"

DWD_ROWS = 100_095_182
EXPECT = {"pv": 89_660_671, "cart": 5_530_446, "fav": 2_888_258, "buy": 2_015_807}
MAX_CAT_ROWS = 9_439 * 9      # 84,951 = 理论上界

spark = (SparkSession.builder
         .appName("dws_category_day")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(DWD_MAIN)

    # ================= ① 聚合：一个类目一天 = 一行 =================
    cat_day = (df.groupBy("dt", "category_id")
        .agg(
            F.count("*").alias("n_action"),
            F.sum(F.when(F.col("behavior_type") == "pv",   1).otherwise(0)).alias("pv_cnt"),
            F.sum(F.when(F.col("behavior_type") == "cart", 1).otherwise(0)).alias("cart_cnt"),
            F.sum(F.when(F.col("behavior_type") == "fav",  1).otherwise(0)).alias("fav_cnt"),
            F.sum(F.when(F.col("behavior_type") == "buy",  1).otherwise(0)).alias("buy_cnt"),
            F.countDistinct("user_id").alias("uv"),          # 今天有多少人碰过这个类目
            F.countDistinct("item_id").alias("n_item"),      # 今天这个类目下有多少件商品被碰过
        )
        .withColumn("conv_rate",
            F.when(F.col("uv") > 0, F.col("buy_cnt") / F.col("uv")))   # 人均下单次数
    )

    # ================= ② 落盘 =================
    cat_day.write.mode("overwrite").partitionBy("dt").parquet(DWS_CAT)

    # ================= ③ 对账 =================
    back = spark.read.parquet(DWS_CAT)
    n_rows = back.count()

    s = back.agg(
        F.sum("n_action").alias("act"),
        F.sum("pv_cnt").alias("pv"),
        F.sum("cart_cnt").alias("cart"),
        F.sum("fav_cnt").alias("fav"),
        F.sum("buy_cnt").alias("buy"),
    ).first()

    print(f"[1] 类目日行数 = {n_rows:,}   （上界 {MAX_CAT_ROWS:,}）")
    print(f"[2] 行为总数   = {s['act']:,}")
    print(f"[3] pv={s['pv']:,}  cart={s['cart']:,}  fav={s['fav']:,}  buy={s['buy']:,}")

    assert n_rows < MAX_CAT_ROWS, f"行数 {n_rows:,} 达到上界，说明聚合写错了"
    assert s["act"] == DWD_ROWS
    assert s["pv"] == EXPECT["pv"] and s["cart"] == EXPECT["cart"]
    assert s["fav"] == EXPECT["fav"] and s["buy"] == EXPECT["buy"]
    print("[4] 行级守恒 = [OK]")

    uv_tbl = back.groupBy("dt").count()
    assert back.filter(F.col("dt").isNull()).count() == 0
    assert uv_tbl.agg(F.sum("count")).first()[0] == n_rows
    print("[5] 日粒度守恒 = [OK]")

    # ================= ④ 看什么最热 =================
    print(f"\n=== DWS 里的类目数 / 全量类目数 ===")
    n_cat = back.select("category_id").distinct().count()
    print(f"DWS 类目 = {n_cat:,}    体检全量 = 9,439    差 = {9_439 - n_cat:,}")

    print("\n=== TOP10 类目（按 9 天总 pv）===")
    (back.groupBy("category_id")
         .agg(F.sum("pv_cnt").alias("总pv"),
              F.sum("uv").alias("uv合计"),
              F.sum("n_item").alias("商品日数"))
         .orderBy(F.desc("总pv")).show(10))

    print("=== 每个类目日均被多少人碰（分布）===")
    back.agg(
        F.avg("uv").alias("平均"),
        F.expr("percentile_approx(uv, 0.5)").alias("中位数"),
        F.max("uv").alias("最高"),
    ).show()
finally:
    spark.stop()