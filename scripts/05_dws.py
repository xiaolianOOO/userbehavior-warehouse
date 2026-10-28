# scripts/05_dws.py — DWS（汇总数据层 Data Warehouse Summary）：用户×日
import sys
sys.stdout.reconfigure(encoding="utf-8")          # 让 UTF-8 中文在 GBK 控制台不崩

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")   # Windows 写盘需要 winutils.exe
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"               # 必须在 getOrCreate 之前设好

from pyspark.sql import SparkSession, functions as F

# ---------------- 配置区（改这里，不改下面） ----------------
DWD_MAIN = "F:/data_warehouse/data/dwd/user_behavior"   # DWD 主表（按 dt 分区）
DWS_USER = "F:/data_warehouse/data/dws/user_day"        # DWS 用户日表（本次产出）

DWD_ROWS = 100_095_182   # DWD 主表行数（窗口内、去重后）
TOTAL_UV = 987_994       # 全量（含越界）去重用户数，仅作对账参考

spark = (SparkSession.builder
         .appName("dws_user_day")
         .master("local[4]")                                      # local[N] = 本地 N 线程
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")   # 命根子：时区锁死
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")   # 关掉 WARN 噪音，让真错误可见

try:
    df = spark.read.parquet(DWD_MAIN)     # dt 从目录名 dt=yyyy-MM-dd 自动读回成一列

    # ================= ① 聚合：一个人一天 = 一行 =================
    user_day = (df.groupBy("dt", "user_id")          # ★1 主键 = (user_id, dt)
        .agg(
            F.count("*").alias("n_action"),                                       # 当日行为总数
            F.sum(F.when(F.col("behavior_type") == "pv",   1).otherwise(0)).alias("pv_cnt"),
            F.sum(F.when(F.col("behavior_type") == "cart", 1).otherwise(0)).alias("cart_cnt"),
            F.sum(F.when(F.col("behavior_type") == "fav",  1).otherwise(0)).alias("fav_cnt"),
            F.sum(F.when(F.col("behavior_type") == "buy",  1).otherwise(0)).alias("buy_cnt"),
            F.countDistinct("item_id").alias("n_item"),                           # ★2 最贵的一列
            F.min("ts").alias("first_ts"),                                        # 当日首次行为
            F.max("ts").alias("last_ts"),                                         # 当日末次行为
        )
        # ================= ② 存在性标记：ADS 漏斗要用 =================
        .withColumn("has_pv",   (F.col("pv_cnt")   > 0).cast("int"))
        .withColumn("has_cart", (F.col("cart_cnt") > 0).cast("int"))
        .withColumn("has_fav",  (F.col("fav_cnt")  > 0).cast("int"))
        .withColumn("has_buy",  (F.col("buy_cnt")  > 0).cast("int"))
    )

    # ================= ③ 落盘：partitionBy 自动管理 dt =================
    user_day.write.mode("overwrite").partitionBy("dt").parquet(DWS_USER)   # ★3

    # ================= ④ 读回对账（证明磁盘上的也对） =================
    back = spark.read.parquet(DWS_USER)

    n_rows = back.count()
    s = back.agg(
        F.sum("n_action").alias("act"),
        F.sum("pv_cnt").alias("pv"),
        F.sum("cart_cnt").alias("cart"),
        F.sum("fav_cnt").alias("fav"),
        F.sum("buy_cnt").alias("buy"),
    ).first()

    print(f"[1] 用户日行数 = {n_rows:,}")
    print(f"[2] 行为总数   = {s['act']:,}")
    print(f"[3] pv={s['pv']:,}  cart={s['cart']:,}  fav={s['fav']:,}  buy={s['buy']:,}")
    print(f"[4] 四类相加   = {s['pv'] + s['cart'] + s['fav'] + s['buy']:,}")

    assert s["act"] == DWD_ROWS, f"行为总数 {s['act']:,} != DWD {DWD_ROWS:,}"
    assert s["pv"] + s["cart"] + s["fav"] + s["buy"] == DWD_ROWS, "四类相加不等于 DWD 行数"
    print("[5] 行级守恒 = [OK]")

    # ================= ⑤ 新增：日粒度守恒（这次补上的那条） =================
    uv_tbl = back.groupBy("dt").count()                              # 每日 UV
    sum_uv = uv_tbl.agg(F.sum("count").alias("s")).first()["s"]      # 九日之和
    n_null = back.filter(F.col("dt").isNull()).count()               # 孤儿行

    print(f"[6] 每日UV之和 = {sum_uv:,}   行数 = {n_rows:,}   dt为NULL = {n_null:,}")

    assert n_null == 0, f"有 {n_null:,} 行的 dt 为 NULL，会永远不出现在任何趋势图里"
    assert sum_uv == n_rows, \
        f"每日UV之和 {sum_uv:,} != 行数 {n_rows:,}（差 {sum_uv - n_rows:,}）"
    print("[7] 日粒度守恒 = [OK]")

    # ================= ⑥ 展示 =================
    print("\n=== 每日 UV（去重用户数）===")
    uv_tbl.orderBy("dt").show()

    uv_all = back.select("user_id").distinct().count()
    print(f"9 天去重用户 = {uv_all:,}   体检全量用户 = {TOTAL_UV:,}   差 = {TOTAL_UV - uv_all:,}")
finally:
    spark.stop()