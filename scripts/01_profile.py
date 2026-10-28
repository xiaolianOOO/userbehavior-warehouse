'''
一句话：它不加工数据，只给数据“验货”。

你去菜市场拉回一车 1 亿斤菜，这段代码就是门口的过磅 + 翻检：数了多少斤、有几种、哪几筐烂了、生产日期印的对不对、每筐重量加起来是不是等于总重、有没有重复装的。它一个菜都不改，只出一张验收单。

八段各自回答一个问题：

段	干了什么	回答的问题
1	三个 countDistinct	有多少不同的人 / 商品 / 类目
2	groupBy(behavior_type)	看、加购、收藏、买，各占多少
3	逐列数 NULL	有没有缺胳膊少腿的行,
4	反向筛选	有没有第 5 种野生行为
5	min/max，两种口径	时间跨度多大（全量 vs 窗口内）
6	按北京日分组 + 断言	9 天各多少行，加起来对不对
7	按 UTC 日分组	换个时区切，会多出哪天
8	dropDuplicates().count()	有多少行是完全重复的





段 1、2（规模、构成）
段 3、4、5（干净度）
段 6、7、8（口径、去重）



段 1｜三个去重数
做了什么：对 user_id、item_id、category_id 各做一次“去掉重复之后再数”。
为什么做：因为“1 亿行”不等于“100 万人”。一个人点了 90 次，就占 90 行。要分清“行数”和“人数”，这是所有分析的第一步。
结果为什么是这样：
users = 987,994      ≈ 99 万人
items = 4,162,024    ≈ 416 万件商品
cats  = 9,439        ≈ 9400 个类目
用户 ID 的范围是 1 ~ 999,999（你从首行和末行能看到），去重后 98.8 万个 → 说明这份数据的用户编号是 1~100 万连续编的，几乎每个号都出现过。
它是一份“广撒网”的数据：不是少数重度用户刷出来的。
商品 416 万，远多于用户 99 万。顺带算一个数：89,716,264 ÷ 4,162,024 ≈ 21.6 —— 平均每件商品只被看过 21 次。
这就是典型的长尾：绝大多数商品没人看，少数爆款撑起大半流量。
为什么这很重要？ 因为如果你只有 1000 件商品，那“商品维度分析”意义不大；416 万件商品，你才值得做“商品×日”这个粒度——这就是 DWS 三层里为什么要有一层是商品的依据。



段 2｜行为分布
做了什么：按 behavior_type 分成四堆，数每堆多少行。
为什么做：知道这份数据主要记录了什么。如果 pv 占 99.9%，那它只能做流量分析，做不了交易分析。
结果为什么是这样：
pv    89,716,264   89.58%
cart   5,530,446    5.52%
fav    2,888,258    2.88%
buy    2,015,839    2.01%
这是电商漏斗的自然形状：看的人多，买的人少。
这里有个坑，你必须现在就记住
这是“行”的占比，不是“人”的转化率。
buy 占 2.01%，绝不等于“2% 的访客下了单”。要算真正的购买转化率，得用去重用户：有多少个不同的人有 buy 行为 ÷ 有多少个不同的人。
而且你看：buy 有 201 万行，但用户总共才 98.8 万人——说明下单的人平均不止下一单。
这就是“行数思维”和“人数思维”的分界线。 面试官非常爱考这个：给你一个 “2% 转化率”，看你会不会直接用。


段 3｜空值检查
做了什么：逐列数有多少个 NULL（判断 → 转成 0/1 → 求和）。
为什么做：NULL 是静默杀手。
sum / avg 碰到 NULL 会跳过它——你以为算的是 100 个数，其实只算了 90 个；
join 时 NULL 不等于任何值，包括它自己——带 NULL 的行在关联时会被悄悄丢掉。
它不报错，只是让你的数慢慢歪掉。
结果为什么是全 0：这份数据是从日志系统导出来的，五个字段都是必填的。
但“这次是 0”不等于“以后也是 0”，这道检查要一直留着。



段 4｜非法行为
做了什么：反向筛选——挑出“不属于 pv/cart/fav/buy”的行。
为什么做：如果哪天冒出第 5 种行为（比如 click），groupBy 会把它当成正常值算进去，你的 DWS 里就凭空多一类，而且没人会报错。
结果为什么是 0：这四类是闭集（业务定义死的）。0 说明数据源没有夹带私货。



段 5｜时间范围（全量 vs 窗口内）
做了什么：算最小、最大时间戳，两个口径都算：全量的、和窗口内的。
为什么做：时间范围决定你能做什么分析。
9 天 → 只能做次日留存和3 日留存；
你要是写“月留存”，就是编数据（9 天里哪来的一个月）。
结果为什么差了一百年：
全量：    −2,134,949,234  ~  2,122,867,355   ← 1902 年到 2037 年
窗口内：   1,511,539,200  ~  1,512,316,799   ← 正好卡在 11-25 ~ 12-03
全量那两个数是被几十颗沙子带偏的（负数 318 行、2037 年 30 行）。你要是只看全量，会得出“我的数据跨了 135 年”这种荒谬结论。
这就是为什么要两个口径并排看：只看全量会被骗，只看窗口内会看不见毛边有多厚。



段 6｜按北京日切 + 对账
做了什么：把时间戳翻译成“北京时间的年月日”，按天分组，数每天多少行，再把 9 天加起来对账。
为什么做：所有“日活”“日趋势”“次日留存”，全都挂在“日”这个字上。日的定义错了，上面全错。
结果为什么是这样：
代码块
2017-11-25  10,420,015   ← 起点
...
2017-11-28   9,878,190   ← 全期最低
2017-12-02  13,777,869   ← 峰值
2017-12-03  13,662,159
合计 = 100,095,231   [OK]
[OK] 的意思是：9 天加起来 = 窗口内总行数，一行不多一行不少。
这叫“守恒律”：同一堆东西，不管你怎么切（按行为切、按天切、按人切），加起来都必须等于总量。切法会变，总量不会变。 这是数仓里最便宜、最有效的一道体检。
那 12-02、12-03 为什么高 31%？ 这八个数回答不了。要知道是“人多了”还是“每人点多了”，得算每天的去重用户数——那是 DWS 的活。记住这个问题，它是你 DWS 的第一道真题。



段 7｜按 UTC 日切
做了什么：把时间戳减 8 小时，再按“日”分组——等于按 UTC 的日历日切。
为什么做：为了演示“日的定义会改变结果”。同一堆数据，换个时区，天数就变了。
结果为什么是 10 天、而且第一天特别少：
2017-11-24   1,181,431   ← 只有 8 小时的数据
2017-11-25  10,511,605
...
北京比 UTC 快 8 小时。北京时间的 11-25 00:00 ~ 08:00，在 UTC 那边还是 11-24。所以：
按北京切 → 9 天（每天完整）
按 UTC 切 → 10 天（第一天只有 8 小时）
为什么这 8 小时只有 118 万行，而不是“一天的 1/3（约 350 万）”？
因为那 8 小时正好是北京时间凌晨 0 点到 8 点——人在睡觉。
算给你看：
全天平均：100,095,231 ÷ 777,600 秒 ≈ 129 行/秒
那 8 小时：1,181,431 ÷ 28,800 秒 ≈ 41 行/秒
只有平均值的 1/3。
这不是 bug，是证据。 它证明这份数据是按真实作息分布的，不是机器刷出来的均匀流量。面试时你可以主动说这一句——它比任何“我做了数据清洗”都更能证明你真的看过数据。


段 8｜重复行率
做了什么：把 (user_id, item_id, behavior_type, ts) 四个字段完全一样的行，只算一次，然后数少了多少。
为什么做：数仓的事实表有个基本要求——一行 = 一次行为。有重复行，你的 PV、UV、转化率就都会虚高。
结果为什么只有 49 行：
原始   = 100,150,807
去重后 = 100,150,758
重复   = 49  (0.000049%)
两个原因：
这份公开数据集本身已经去过重了（天池发布的清洗版）；
你的去重键很严——要求四个字段全同才算重复。
换个键，这个数会天翻地覆。 改成 (user_id, ts)（同一个人同一秒的所有行为只留一条），能抓出一大批；改成 (user_id)，能“去重”到只剩 98 万行——那叫把数据毁了，不叫去重。
所以“重复率是多少”这个问题本身没有答案，必须先说“按什么算重复”。




'''

from pyspark.sql import SparkSession, functions as F
import sys
sys.stdout.reconfigure(encoding="utf-8")

# ============ 验收基准（写死。数据换了才改这里，别散落在代码里）============
# 为什么非要单独提出来？ 因为这些是验收标准。
# 如果散落在代码里，改一次要翻八个地方，漏一个就出现“一个 [OK] 一个 ✘”。
# 1511539200 这个数不是拍脑袋的，它 = 北京时间 2017-11-25 00:00:00 换算成 UTC 秒。1512316800 = 北京时间 2017-12-04 00:00:00
# （注意是 12-04，不是 12-03——因为区间是左闭右开，< 1512316800 才能把 12-03 一整天包进来）。
TOTAL_EXPECTED     = 100150807   # 全量行数（已双路校验）
IN_WINDOW_EXPECTED = 100095231   # 窗口内行数 = 全量 - 55,576 条越界行（已双路校验）
WINDOW_DAYS        = 9
# 业务窗口：北京时间 2017-11-25 00:00 ~ 2017-12-04 00:00 对应的 UTC 秒
WINDOW_LO = 1511539200
WINDOW_HI = 1512316800



'''
.appName(...)：给这次任务起个名，Spark 网页监控里显示的就是它。
.master("local[4]")：用本机 4 个线程跑。local[*] 是“有几个核用几个”，你机器 8 核，内存又只剩 2G，开 8 个线程等于8 个人抢一张桌子吃饭，反而更慢甚至崩。
    降到 4 是给内存留活路。
.config("spark.driver.memory", "2g")：默认只给 1G，显式给 2G。
.config("spark.sql.shuffle.partitions", "4")：洗牌分几堆。默认 200 堆，对 1 亿行的小任务来说，200 个小堆的调度开销比干活还大。4 堆刚好。
.config("spark.sql.session.timeZone", "Asia/Shanghai")：★ 最关键的一行。它决定了 F.from_unixtime(ts) 把时间戳渲染成哪个时区的墙上时间。
    不锁死，换台机器、换个系统语言，你段 6 的日期就变了。同一条 SQL 在不同机器上跑出不同结果，是最难查的一类 bug。
.getOrCreate()：有就复用，没有就新建。
setLogLevel("ERROR")：让 WARN 闭嘴。不是 WARN 不重要，是它太吵，会把你真正的错误埋掉。'''
spark = (SparkSession.builder
         .appName("step0_profile")
         .master("local[4]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")



try:
    df = (spark.read
          .option("header", "false")
          .schema("user_id long, item_id long, category_id long, behavior_type string, ts long")
          .csv("file:///F:/data_warehouse/data/raw/UserBehavior.csv"))



    # 窗口内：后续 5/6/7 段统一用这个，口径只有一份
    in_win = df.filter((F.col("ts") >= WINDOW_LO) & (F.col("ts") < WINDOW_HI))



    print("=== 1. 三个去重记数 ===")
    df.agg(F.countDistinct("user_id").alias("users"),
           F.countDistinct("item_id").alias("items"),
           F.countDistinct("category_id").alias("cats")).show()


# groupBy = 按行为分堆（pv 一堆、cart 一堆……）。
# .count() = 每堆有多少行。
# .orderBy(F.desc("count")) = 从多到少排。注意 orderBy 放在 count 后面——先聚合再排序，排序的只有 4 行；你要是先排序再聚合，排的是 1 亿行。
    print("=== 2. 行为分布 ===")
    df.groupBy("behavior_type").count().orderBy(F.desc("count")).show()
    print("=== 2b. 各行为的去重用户数 ===")
    (df.groupBy("behavior_type")
       .agg(F.countDistinct("user_id").alias("uv"))
       .orderBy(F.col("uv").desc())
       .show())

    
    print("=== 3. 空值检查（全 0 才算干净）===")
    cols = ["user_id", "item_id", "category_id", "behavior_type", "ts"]
    df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).show()



    print("=== 4. 有没有第 5 种行为（正常应为 0）===")
    # 注意：~isin 对 NULL 不会返回 true，NULL 行会被静默放过，所以先显式判 NULL
    bad_cond = (F.col("behavior_type").isNull()
                | ~F.col("behavior_type").isin("pv", "cart", "fav", "buy"))
    print("非法行为行数 =", df.filter(bad_cond).count())



    print("=== 5. 时间戳范围（全量 vs 窗口内，两个都要看）===")
    df.agg(F.min("ts").alias("min_all"), F.max("ts").alias("max_all")).show()
    in_win.agg(F.min("ts").alias("min_win"), F.max("ts").alias("max_win"),
               F.count("*").alias("cnt_win")).show()


    print("=== 6. 按北京日（窗口内）===")
    rows = (in_win.withColumn("dt_bj", F.date_format(F.from_unixtime("ts"), "yyyy-MM-dd"))
                  .groupBy("dt_bj").count().orderBy("dt_bj").collect())
    for r in rows:
        print(f"  {r['dt_bj']}  {r['count']:>12,}")
    total = sum(r["count"] for r in rows)
    print(f"天数={len(rows)}  合计={total:,}")
    assert len(rows) == WINDOW_DAYS, f"天数应为 {WINDOW_DAYS}，实际 {len(rows)}"
    assert total == IN_WINDOW_EXPECTED, f"窗口内合计应为 {IN_WINDOW_EXPECTED:,}，实际 {total:,}"
    print("对账 = [OK]")


    print("=== 7. 按 UTC 日（看多出来的那一天）===")
    (in_win.withColumn("dt_utc", F.date_format(F.from_unixtime(F.col("ts") - 8 * 3600), "yyyy-MM-dd"))
           .groupBy("dt_utc").count().orderBy("dt_utc").show(20, truncate=False))


    print("=== 8. 重复行率（DWD 去重的验收标准）===")
    DEDUP_KEY = ["user_id", "item_id", "behavior_type", "ts"]
    dedup_cnt = df.dropDuplicates(DEDUP_KEY).count()
    dup = TOTAL_EXPECTED - dedup_cnt
    print(f"  原始行数 = {TOTAL_EXPECTED:,}")
    print(f"  去重后   = {dedup_cnt:,}")
    print(f"  重复行   = {dup:,}  ({dup / TOTAL_EXPECTED * 100:.6f}%)")

finally:
    spark.stop()