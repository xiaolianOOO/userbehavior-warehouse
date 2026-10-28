# scripts/check_dup.py — 重复数据下钻：查清“重复到底长什么样”（一次性工具，不算正式步骤）
import sys
sys.stdout.reconfigure(encoding="utf-8")   # Windows 控制台 GBK，强制 UTF-8

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"

from pyspark.sql import SparkSession, functions as F

# ---------------- 常量 ----------------
KEYS          = ["user_id", "item_id", "behavior_type", "ts"]   # 四列身份键
ODS_MAIN      = "F:/data_warehouse/data/ods/user_behavior"
EXPECT_GROUPS = 49     # 期望：重复组数（= 04_dwd.py 的 [3] 重复 = 49）
EXPECT_ROWS   = 98     # 期望：涉及原始行数（49 组 × 2 行）

spark = (SparkSession.builder
         .appName("check_dup")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    df = spark.read.parquet(ODS_MAIN)

    # ---------------- ① 按四列键分组，只留组内条数 > 1 的 ----------------
    g = df.groupBy(KEYS).count().filter(F.col("count") > 1).cache()
    n_groups = g.count()

    # ---------------- ② 一次 agg 算出三个数（替代原来那个瞎写的 n + 49）----------------
    stat = g.agg(
        F.count("*").alias("groups"),                       # 重复组数
        F.sum("count").alias("rows_total"),                 # 涉及的原始行数
        F.sum(F.col("count") - 1).alias("rows_to_drop"),    # 每组去掉 1 条后应删除的行数
    ).first()

    n_rows, n_drop = stat["rows_total"], stat["rows_to_drop"]

    print(f"重复组数     = {n_groups:,}")
    print(f"涉及原始行数 = {n_rows:,}")
    print(f"应删除行数   = {n_drop:,}")

    print("\n=== 每组重复几条 ===")
    g.groupBy("count").count().orderBy("count").show()

    # ---------------- ③ 广播关联，回捞原始行（只洗四列，不洗一亿行）----------------
    detail = df.join(F.broadcast(g.select(KEYS)), KEYS, "left_semi")
    print(f"回捞到的行数 = {detail.count():,}    （应等于 {n_rows:,}）")

    # ---------------- ④ 重复的行为分布（这次的“值钱发现”就在这里）----------------
    print("=== 重复行的行为分布 ===")
    detail.groupBy("behavior_type").count().orderBy(F.desc("count")).show()

    # ---------------- ⑤ 关键一问：同组内类目是否一致 ----------------
    n_cat = (detail.groupBy(KEYS)
                   .agg(F.countDistinct("category_id").alias("n_cat"))
                   .filter(F.col("n_cat") > 1).count())
    print(f"同组内类目不一致的组数 = {n_cat}   （期望 0）")

    # ---------------- ⑥ 采样展示 ----------------
    print("=== 这 ",n_groups * 2, "行原样（按时间排序）===")
    detail.orderBy("ts").select(KEYS + ["category_id", "dt"]).show(n_groups * 2, truncate=False)

    # ---------------- ⑦ 断言 ----------------
    assert n_groups == EXPECT_GROUPS, f"组数 {n_groups} != {EXPECT_GROUPS}"
    assert n_rows   == EXPECT_ROWS,   f"行数 {n_rows} != {EXPECT_ROWS}"
    assert n_drop   == EXPECT_ROWS - EXPECT_GROUPS, "删行数 != 行数 - 组数"
    print("对账 = [OK]")

finally:
    spark.stop()