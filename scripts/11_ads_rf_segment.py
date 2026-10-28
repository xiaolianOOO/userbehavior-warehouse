# ============================================================
# scripts/11_ads_rf_segment.py  —  ADS RF 二维分层（无 M，因为无金额）
# 输入：data/dws/user_day        人×日，6,968,544 行
# 输出：data/ads/ads_rf_segment  10 行（买家 3×3 九格 + 从未购买 1 行）
# ============================================================
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
os.environ.setdefault("HADOOP_HOME", r"F:\DevTools\hadoop")
os.environ["PATH"] += r";F:\DevTools\hadoop\bin"

from pyspark.sql import SparkSession, functions as F

DWS_USER  = "F:/data_warehouse/data/dws/user_day"
ADS_OUT   = "F:/data_warehouse/data/ads/ads_rf_segment"
LAST_DT   = "2017-12-03"

N_USERS       = 987_991        # 9 天去重用户（05_dws 实测）
N_BUYERS      = 672_404        # 买过的人（01_profile 段 2b）
N_BUY_ACTIONS = 2_015_807      # buy 行为总次数（DWS 实测）
N_ROWS_EXPECT = 10             # 9 格 + 1 行"从未购买"

spark = (SparkSession.builder
         .appName("ads_rf_segment")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

try:
    d = spark.read.parquet(DWS_USER)

    # ---- 1) 一人一行：购买次数、最后一次购买日、总行为次数 ----
    u = (d.groupBy("user_id").agg(
            F.sum("buy_cnt").alias("buy_times"),          # buy_times：9 天购买次数
            F.max(F.when(F.col("has_buy") == 1, F.col("dt"))).alias("last_buy_dt"),
            F.sum("n_action").alias("actions"),           # actions：9 天总行为次数
         ))
    u = (u.withColumn("is_buyer", (F.col("buy_times") > 0).cast("int"))
          .withColumn("recency_days",                     # recency_days：距最后购买的天数
                      F.when(F.col("buy_times") > 0,
                             F.datediff(F.to_date(F.lit(LAST_DT)), F.col("last_buy_dt")))))

    n = u.count()
    assert n == N_USERS, f"用户数 {n:,} != {N_USERS:,}"

    # ---- 2) F 的分档点：用买家内部的三分位，不拍脑袋 ----
    buyers = u.filter(F.col("is_buyer") == 1)
    nb = buyers.count()
    assert nb == N_BUYERS, f"买家 {nb:,} != {N_BUYERS:,}"
    f1, f2 = buyers.approxQuantile("buy_times", [1/3, 2/3], 0.0001)
    print(f"[1] 用户 {n:,}   买家 {nb:,}   F 三分位点 = {f1:.0f} / {f2:.0f}")

    # ---- 3) 打标 ----
    seg = (u.withColumn("r_seg",
                        F.when(F.col("is_buyer") == 0, "RN")          # 从未购买
                         .when(F.col("recency_days") <= 1, "R1")      # 最近 2 天买过
                         .when(F.col("recency_days") <= 3, "R2")      # 3–4 天前买过
                         .otherwise("R3"))                            # 5 天以上
             .withColumn("f_seg",
                         F.when(F.col("is_buyer") == 0, "F0")         # 从未购买
                          .when(F.col("buy_times") <= f1, "F1")
                          .when(F.col("buy_times") <= f2, "F2")
                          .otherwise("F3")))

    # ---- 4) 交叉出格子 ----
    t = (seg.groupBy("r_seg", "f_seg").agg(
                F.count("*").alias("uv"),
                F.round(F.avg("recency_days"), 2).alias("avg_recency"),
                F.round(F.avg("buy_times"), 2).alias("avg_buy_times"),
                F.round(F.avg("actions"), 1).alias("avg_actions"),
                F.sum("buy_times").alias("buy_actions_sum"))
            .withColumn("uv_share", F.round(F.col("uv") / N_USERS, 4))
            .orderBy("r_seg", "f_seg"))

    # ---- 5) 对账：三条互不相关的恒等式 ----
    rows = t.count()
    s = t.agg(F.sum("uv").alias("uv"),
              F.sum("buy_actions_sum").alias("buys")).first()
    buyers_in_grid = t.filter(F.col("r_seg") != "RN").agg(F.sum("uv")).first()[0]

    assert rows == N_ROWS_EXPECT, f"格子数 {rows} != {N_ROWS_EXPECT}"
    assert s["uv"] == N_USERS,    f"Σuv {s['uv']:,} != {N_USERS:,}"
    assert s["buys"] == N_BUY_ACTIONS, f"Σ购买次数 {s['buys']:,} != {N_BUY_ACTIONS:,}"
    assert buyers_in_grid == N_BUYERS, f"九格里的买家 {buyers_in_grid:,} != {N_BUYERS:,}"
    print(f"[2] 格子 {rows} 行   Σuv {s['uv']:,}   Σ购买 {s['buys']:,}   "
          f"买家 {buyers_in_grid:,}   对账 = [OK]")

    # ---- 6) 落盘 ----
    t.write.mode("overwrite").parquet(ADS_OUT)
    print(f"[3] 写盘完成 -> {ADS_OUT}")
    back = spark.read.parquet(ADS_OUT)
    assert back.count() == rows
    print(f"[4] 读回 {back.count()} 行   对账 = [OK]")

    print("\n=== RF 分层（买家九格 + 从未购买）===")
    back.orderBy("r_seg", "f_seg").show(20, truncate=False)

    print("\n=== 每格买家占总体比例 ===")
    (back.withColumn("buyer_share", F.round(F.col("uv") / N_BUYERS, 4))
         .select("r_seg", "f_seg", "uv", "uv_share", "buyer_share",
                 "avg_recency", "avg_buy_times", "avg_actions")
         .orderBy("r_seg", "f_seg").show(20, truncate=False))

except Exception as e:
    print("\n!!! 出错：", type(e).__name__, e)
    raise
finally:
    spark.stop()