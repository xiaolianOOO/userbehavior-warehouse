
# “把那个 3.4G、没有表头的 1 亿行 CSV，按我指定的五列格式读进来，数一共多少行，并抽 5 行看看长什么样。”
# 就这一件事。不是清洗、不是建表、不是分析，只是“打开并确认”。




# spark 这个程序很大，不是一次全搬进内存的。 
# 这行只是从仓库（pyspark 这个安装包）里，把“总开关”这一个零件取出来。
# SparkSession 你可以理解成整个 Spark 的遥控器——后面所有事都从它这儿发指令。
from pyspark.sql import SparkSession


# 先解释外面那对括号：这不是 Spark 的语法，是 Python 的。不加括号，Python 到第 3 行末尾就以为语句结束了；加了括号，它才知道“我还在同一句话里，往下读”。纯语法问题，跟大数据无关。
# .builder	开始组装遥控器。下面是按顺序往遥控器上加按钮 
# .appName("step0_row_count")	给这次任务起个名字。作用是事后在日志/监控里认得出是谁干的。像给快递包裹写收件人
# .master("local[*]")	在哪儿跑。local = 就在你这台电脑上跑，不连集群；[*] = 你有几个核全用上。你机器 8 核，就等于开 8 个工人
# .config("spark.sql.shuffle.partitions", "8")	“洗牌时切成几份”。Spark 做 groupBy/join 时要打乱数据重分布（洗牌），默认切 200 份。200 个人干 8 个人能干的活，调度开销比干活还大。 诚实说：这一行对今天的 count 其实没用（count 不洗牌），它是给你后面 DWS 做汇总时用的。写在这儿是提前立规矩
# .getOrCreate()	按上面这些按钮，真把遥控器造出来。叫“get or create”是因为：已经有现成的就复用，没有再新建。你在脚本里多次调用不会造出两个 Spark
# 这一段跑完，Spark 其实还没干活，只是把“发动机”点着了。
spark = (SparkSession.builder
         .appName("step0_row_count")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())



# 关键认知：这一段执行完，3.4G 的文件一个字节都还没读。
# Spark 是“先画图纸、后施工”。这几行只是往图纸上画：“我要从那个文件读、按这个格式、这五列”。
# df 就是那张图纸，不是数据本身。这叫惰性执行（lazy）——面试高频词，你必须能脱口而出。
# .option("header", "false")：告诉它“第一行也是数据，别当表头”。 
# 这行是最容易吃大亏的一行：如果写成 true，Spark 会把第一行 1,2268318,... 当列名吃掉——你的行数就从 100,150,807 变成 100,150,806，只少一行，屏幕上根本看不出来。
# 所以哪怕默认值就是 false，也要显式写出来，等于给自己贴张便签。
# .schema("user_id long, item_id long, category_id long, behavior_type string, ts long")：你的数据是没表头的纯数字文本，Spark 不知道哪列是什么。
#  这行就是你亲手给五列“贴标签 + 定类型”。
# 为什么用 long（整数）而不是 string：数字才能做 sum/max/比大小，而且省内存。
# behavior_type 是 pv/buy 这种文字，只能 string。
# CSV 是“按位置”对列的，不认名字。 
# 你把 schema 里 user_id 和 item_id 的顺序写反，Spark 不会报错，它会安静地把用户 ID 当成商品 ID 存下去——这是最阴的错，只有靠对账才能发现。
# 为什么不用 option("inferSchema","true") 让它自己猜：猜就要先把 3.4G 整个读一遍，等于白读一次；而且猜出来的类型不稳定（id 可能被猜成 string，后面 join 会慢一倍）。
# .csv("file:///F:/data_warehouse/data/raw/UserBehavior.csv")：真正指定读哪个文件。 
# file:/// 拆开看：file: 是“协议”（告诉 Spark 这是个本地文件，不是 HDFS、不是 S3），
# 后面两个斜杠表示“没有远程主机”，第三个斜杠才是盘的根。Windows 路径的 \ 在这要写成 /，写成 F:\... 是新手最常见的报错。
df = (spark.read
      .option("header", "false")
      .schema("user_id long, item_id long, category_id long, behavior_type string, ts long")
      .csv("file:///F:/data_warehouse/data/raw/UserBehavior.csv"))





# count() 这一句，Spark 才第一次真的去读那个 3.4G。 这就是“施工”：图纸攒了一堆，有人喊了一声“开工”，工人才动。
# count() 是行动（action）。它没别的巧办法，只能一行一行数过去，所以慢，要几十秒到两分钟。
# show(5) 又是一次行动，又把全表扫一遍。所以你这脚本总共读了两遍 3.4G。 👉 这就是“多一个 action 就多一次全表扫”的直观例子。 以后写代码前先想：我是不是在反复扫同一份数据？
# truncate=False：默认 Spark 会把长字符串截断成 23333...，加这个就完整显示。看真实数据时一定要加。
print("总行数 =", df.count())
df.show(5, truncate=False)


# Spark 后台是个 Java 进程，不关它会一直挂在内存里，多跑几次你的电脑就卡了。养成写完就 stop 的习惯。
spark.stop()