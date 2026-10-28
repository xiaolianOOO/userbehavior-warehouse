# scratch/rf_probe.py  —— 一次性探查：R 与 F 的分布，跑完即删
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"

from pyspark.sql import SparkSession, functions as F

DWS_USER = "F:/data_warehouse/data/dws/user_day"
LAST_DT  = "2017-12-03"

spark = (SparkSession.builder
         .appName("rf_probe")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    d = spark.read.parquet(DWS_USER)

    # 一人一行：最后活跃日、行为总次数、是否买过
    u = (d.groupBy("user_id").agg(
            F.max("dt").alias("last_dt"),
            F.sum("n_action").alias("freq_actions"),
            F.max("has_buy").alias("has_buy_any"))
         .withColumn("recency_days",
                     F.datediff(F.to_date(F.lit(LAST_DT)), F.col("last_dt"))))
    u.cache()
    n = u.count()
    print(f"[1] 用户数 = {n:,}")

    print("\n=== R 分布（recency_days，距 2017-12-03 的天数）===")
    (u.groupBy("recency_days")
       .agg(F.count("*").alias("uv"),
            F.round(F.count("*") / n, 4).alias("share"))
       .orderBy("recency_days").show(20, truncate=False))

    print("\n=== F 分布（freq_actions，9 天行为次数）===")
    u.select("freq_actions").summary("min", "25%", "50%", "75%", "max").show()
    print("F 三分位点 =", u.approxQuantile("freq_actions", [1/3, 2/3], 0.0001))

    print("\n=== 买家 vs 非买家 ===")
    (u.groupBy("has_buy_any")
       .agg(F.count("*").alias("uv"),
            F.round(F.avg("recency_days"), 2).alias("avg_recency"),
            F.round(F.avg("freq_actions"), 1).alias("avg_freq"))
       .show())
finally:
    spark.stop()