# scripts/03_ods.py —— ODS 落盘：原样存，按北京日期分区
import sys
sys.stdout.reconfigure(encoding="utf-8")
import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")        # Windows 下 Spark 写盘需要
os.environ["PATH"] = os.environ["PATH"] + r";F:\DevTools\hadoop\bin"

from pyspark.sql import SparkSession, functions as F

CSV      = "F:/data_warehouse/data/raw/UserBehavior.csv"
ODS_MAIN = "F:/data_warehouse/data/ods/user_behavior"          # 按 dt 分区
ODS_OOR  = "F:/data_warehouse/data/ods/user_behavior_out_of_range"  # 越界，不分区

WINDOW_LO = 1511539200        # 北京 2017-11-25 00:00:00
WINDOW_HI = 1512316800        # 北京 2017-12-04 00:00:00（左闭右开）

TOTAL, EXPECT_IN, EXPECT_OOR = 100_150_807, 100_095_231, 55_576

SCHEMA = "user_id long, item_id long, category_id long, behavior_type string, ts long"

spark = (SparkSession.builder
         .appName("ods_load")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")      # ← 命根子
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    assert spark.conf.get("spark.sql.session.timeZone") == "Asia/Shanghai"

    df = (spark.read.option("header", False).schema(SCHEMA).csv(CSV))

    # dt 从 ts 算出来：from_unixtime 按 session.timeZone 解释 → 北京时间
    df = df.withColumn("dt", F.date_format(F.from_unixtime("ts"), "yyyy-MM-dd"))

    in_win = (F.col("ts") >= WINDOW_LO) & (F.col("ts") < WINDOW_HI)
    df_in, df_oor = df.filter(in_win), df.filter(~in_win)

    df_in.write.mode("overwrite").partitionBy("dt").parquet(ODS_MAIN)
    df_oor.write.mode("overwrite").parquet(ODS_OOR)
    print("[1] 写盘完成")

    # 从 Parquet 读回来对账（比再扫 CSV 快得多）
    n_in  = spark.read.parquet(ODS_MAIN).count()
    n_oor = spark.read.parquet(ODS_OOR).count()
    print(f"[2] 主表 = {n_in:,} / 越界 = {n_oor:,} / 合计 = {n_in + n_oor:,}")

    assert n_in  == EXPECT_IN,  f"主表行数不对: {n_in}"
    assert n_oor == EXPECT_OOR, f"越界行数不对: {n_oor}"
    assert n_in + n_oor == TOTAL, "守恒被打破了"
    print("[3] 守恒 = [OK]")

    # 逐日对账（应与 01_profile 段6 完全一致）
    spark.read.parquet(ODS_MAIN).groupBy("dt").count().orderBy("dt").show(20)


    # 【新增】dt 值域校验：必须正好 9 个，且不许出现 NULL
    dts = [r["dt"] for r in
           spark.read.parquet(ODS_MAIN).select("dt").distinct().collect()]
    print(f"[4] dt 值域 = {sorted(dts)}")
    assert len(dts) == 9, f"dt 应该有 9 个值，实际 {len(dts)} 个"
    assert None not in dts, "有行的 dt 算不出来（ts 为 NULL）"
    print("[5] dt 值域 = [OK]")

finally:
    spark.stop()