# scratch/verify_dwd.py — 只读校验，不写盘
import sys; sys.stdout.reconfigure(encoding="utf-8")
import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"
from pyspark.sql import SparkSession, functions as F

DWD_MAIN = "F:/data_warehouse/data/dwd/user_behavior"
DWD_BAD  = "F:/data_warehouse/data/dwd/bad_record"
TOTAL, ODS_IN, ODS_OOR_N, DUP = 100_150_807, 100_095_231, 55_576, 49

spark = (SparkSession.builder.appName("verify_dwd").master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
try:
    df  = spark.read.parquet(DWD_MAIN)      # dt 从目录名读回，自动成为一列
    bad = spark.read.parquet(DWD_BAD)

    n_main, n_bad = df.count(), bad.count()
    print(f"[1] 主表   = {n_main:,}")
    print(f"[2] 次品筐 = {n_bad:,}")
    print(f"[3] 三项和 = {n_main + DUP + n_bad:,}")

    assert n_main == ODS_IN - DUP,   "主表 != 窗口内 − 重复"
    assert n_bad  == ODS_OOR_N,      "次品筐 != 55,576"
    assert n_main + DUP + n_bad == TOTAL, "三项守恒失败"
    print("[4] 守恒   = [OK]")

    bad.groupBy("bad_reason").count().orderBy(F.desc("count")).show()
    df.groupBy("dt").count().orderBy("dt").show(20)
finally:
    spark.stop()