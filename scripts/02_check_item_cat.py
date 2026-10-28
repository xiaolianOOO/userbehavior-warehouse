# scripts\02_check_item_cat.py
# 目的：验证 item_id -> category_id 是否"一对一"
import sys, time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

sys.stdout.reconfigure(encoding="utf-8")

CSV    = "file:///F:/data_warehouse/data/raw/UserBehavior.csv"
SCHEMA = "user_id long, item_id long, category_id long, behavior_type string, ts long"

SAMPLE       = None      # ← 调试开关：先只读前 10 万行。调通后改成 None 跑全量
EXPECT_ITEMS = 4162024     # 体检时算出的商品数，用来对账

t0 = time.time()

spark = (SparkSession.builder
         .appName("check_item_cat")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

def mark(msg):
    print(f"[{time.time()-t0:6.1f}s] {msg}", flush=True)

try:
    mark("1) 读 CSV ...")
    df = (spark.read
          .option("header", "false")
          .schema(SCHEMA)
          .csv(CSV))
    if SAMPLE:
        df = df.limit(SAMPLE)
        mark(f"   （调试模式：只取前 {SAMPLE} 行）")

    mark("2) 取两列 -> 去重 -> 缓存  （会 shuffle，慢是正常的）...")
    pairs = df.select("item_id", "category_id").distinct().cache()

    mark("3) 数 (商品,类目) 组合数 ...")
    n_pairs = pairs.count()
    mark(f"   = {n_pairs}")

    mark("4) 数商品数 ...")
    n_items = pairs.select("item_id").distinct().count()
    mark(f"   = {n_items}")

    print("=" * 50)
    print(f"(商品,类目) 组合数 = {n_pairs}")
    print(f"去重商品数        = {n_items}")
    print(f"差值              = {n_pairs - n_items}")
    print("=" * 50)

    # 只有跑全量时才核对总账
    if SAMPLE is None:
        assert n_items == EXPECT_ITEMS, \
            f"商品数对不上！应={EXPECT_ITEMS} 实={n_items}"
        print("对账 = [OK] 商品数与体检结果一致")

    if n_pairs == n_items:
        print("结论 = [OK] 一对一：一个商品只属于一个类目")
    else:
        print("结论 = [WARN] 一对多：存在商品对应多个类目")

        # 先找出"可疑"的商品 id，最多 20 个
        bad = (pairs.groupBy("item_id")
                    .agg(F.count("*").alias("n_cat"))
                    .filter(F.col("n_cat") > 1)
                    .orderBy(F.col("n_cat").desc())
                    .limit(20)
                    .collect())          # ← 最多 20 行，collect 才安全

        bad_ids = [r["item_id"] for r in bad]
        print(f"可疑商品 id（最多 20 个）: {bad_ids}")

        # 看每个商品的「类目 × 时间区间」，而不是头 100 行
        print("每个可疑商品：类目 与 它存在的时间区间")
        (df.filter(F.col("item_id").isin(bad_ids))
           .groupBy("item_id", "category_id")
           .agg(F.count("*").alias("cnt"),
                F.min("ts").alias("min_ts"),
                F.max("ts").alias("max_ts"))
           .withColumn("min_dt", F.from_unixtime("min_ts"))
           .withColumn("max_dt", F.from_unixtime("max_ts"))
           .orderBy("item_id", "min_ts")
           .show(100, truncate=False))

        # 类目切换次数：判「一次换岗」还是「来回跳」
        w = Window.partitionBy("item_id").orderBy("ts")
        n_switch = (df.filter(F.col("item_id").isin(bad_ids))
                      .select("item_id", "category_id", "ts")
                      .withColumn("prev_cat", F.lag("category_id").over(w))
                      .filter(F.col("prev_cat").isNotNull() &
                              (F.col("category_id") != F.col("prev_cat")))
                      .count())

        print(f"类目切换总次数   = {n_switch}")
        print(f"多类目商品个数   = {len(bad_ids)}（只是样本的前 20 个）")
        print("判读：切换次数 ≈ 商品个数 → 每件只换一次 = 「中途改类目」(真实)")
        print("      切换次数 >> 商品个数 → 同一件来回跳 = 「错乱」(脏)")

    mark("完成")

finally:
    spark.stop()