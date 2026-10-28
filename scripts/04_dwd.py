# scripts/04_dwd.py — DWD（明细层）备菜：去重 + 转北京日 + 脏数据隔离
import sys
sys.stdout.reconfigure(encoding="utf-8")   # Windows 控制台默认 GBK，强制 UTF-8，避免中文打印报错

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")   # winutils 所在父目录（没设过才设）
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"              # 让 JVM 能找到 winutils.exe

from pyspark.sql import SparkSession, functions as F

# ---------------- 路径常量 ----------------
ODS_MAIN = "F:/data_warehouse/data/ods/user_behavior"                 # ODS 主表（按 dt 分区）
ODS_OOR  = "F:/data_warehouse/data/ods/user_behavior_out_of_range"    # ODS 越界表（不分区）
DWD_MAIN = "F:/data_warehouse/data/dwd/user_behavior"                 # DWD 主表（按 dt 分区）
DWD_BAD  = "F:/data_warehouse/data/dwd/bad_record"                    # 次品筐（脏数据隔离表）

# ---------------- 业务常量 ----------------
KEYS = ["user_id", "item_id", "behavior_type", "ts"]   # 去重键（身份列，不含 category_id）

TOTAL     = 100_150_807    # 原始总行数（9 天全量）
ODS_IN    = 100_095_231    # ODS 主表行数（时间窗口内）
ODS_OOR_N = 55_576         # ODS 越界表行数（时间窗口外）
MAX_DUP   = 49             # 窗口内重复行数上界（实测值，非写死规矩）

LO, HI = 1511539200, 1512316800   # 时间窗口，左闭右开（北京 11-25 00:00 ~ 12-04 00:00）

# 北京日期分区列表（2017-11-25 ~ 2017-11-30，2017-12-01 ~ 2017-12-03）
DT_LIST = [f"2017-11-{d:02d}" for d in range(25, 31)] + [f"2017-12-{d:02d}" for d in (1, 2, 3)]

# ---------------- Spark 会话 ----------------
spark = (SparkSession.builder
         .appName("dwd_build")
         .master("local[4]")                                        # 本地 4 线程
         .config("spark.driver.memory", "2g")                        # driver（驱动程序）内存
         .config("spark.sql.shuffle.partitions", "8")                # shuffle（洗牌）后分区数
         .config("spark.sql.session.timeZone", "Asia/Shanghai")      # 会话时区，必须显式锁定
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")                             # 只显示 ERROR，屏蔽 WARN 噪音

try:
    n_main, n_dup = 0, 0

    # ---------------- ① 逐日去重 + 落盘 ----------------
    for dt in DT_LIST:
        # 分区裁剪：只读这一天，不扫全表
        df_d = spark.read.parquet(ODS_MAIN).filter(F.col("dt").cast("string") == dt)
        n_before = df_d.count()
        clean    = df_d.dropDuplicates(KEYS)      # 去重：同一组 KEYS 只留一条
        n_after  = clean.count()

        # drop("dt")：目录名已带日期，文件里再存一份会导致读回时列名冲突
        clean.drop("dt").write.mode("overwrite").parquet(f"{DWD_MAIN}/dt={dt}")

        n_main += n_after
        n_dup  += n_before - n_after
        print(f"  {dt}: {n_before:,} -> {n_after:,}   去重 {n_before - n_after}")

    # ---------------- ② 次品筐：打标签，不删除 ----------------
    bad = (spark.read.parquet(ODS_OOR)
           .withColumn("bad_reason",
               F.when(F.col("ts").isNull(), "ts_null")                  # 空时间戳
                .when(F.col("ts") < 0, "ts_negative")                   # 负数，必须排在“早于窗口”前面
                .when(F.col("ts") < LO, "ts_before_window")             # 早于窗口
                .when(F.col("ts") < 2_000_000_000, "ts_after_window")   # 晚于窗口
                .otherwise("ts_far_future")))                           # 过于遥远（≥ 2e9）
    bad.drop("dt").write.mode("overwrite").parquet(DWD_BAD)
    n_bad = bad.count()

    # ---------------- ③ 对账 ----------------
    print(f"\n[1] 主表   = {n_main:,}")
    print(f"[2] 次品筐 = {n_bad:,}")
    print(f"[3] 重复   = {n_dup}")
    print(f"[4] 三项和 = {n_main + n_dup + n_bad:,}")

    assert n_bad == ODS_OOR_N, f"次品筐 {n_bad:,} != {ODS_OOR_N:,}"

    # 三段守恒：去重后主表 + 被去重行数 + 隔离行数 = 原始总量
    assert n_main + n_dup + n_bad == TOTAL, \
        f"守恒失败 {n_main + n_dup + n_bad:,} != {TOTAL:,}"

    assert n_dup <= MAX_DUP,          f"重复数 {n_dup} > {MAX_DUP}"
    assert n_main == ODS_IN - n_dup,  f"主表 {n_main:,} != {ODS_IN:,} - {n_dup}"
    print("[5] 守恒   = [OK]")

    print("\n=== bad_reason 分布 ===")
    bad.groupBy("bad_reason").count().orderBy(F.desc("count")).show()

finally:
    spark.stop()      # 无论成功失败都关 JVM，否则下次端口占不到