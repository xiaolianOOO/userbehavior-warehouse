# ============================================================================
# scripts/ai_eval.py —— G8 三十条评测集（问数层的回归测试）
#
# 三组各 12 / 12 / 6：
#   can_answer    能答的必须答出来（拒答 = 漏服务）
#   should_reject 答不了的必须拒（答了 = 编造）
#   nail          答出来还不够，关键数字必须对（数字错 = 最危险）
# ============================================================================

import importlib.util

from ai_nl2sql import NotSupported, ask


# ---------------------------------------------------------------------------
# 一、题目
# ---------------------------------------------------------------------------
CAN_ANSWER = [
    "每天有多少人来？",
    "每天有多少人买了东西？",
    "这九天一共浏览了多少次？",
    "购买率怎么样？",
    "加购率是多少？",
    "人均看几个页面？",
    "次日留存情况如何？",
    "有多少人没买过东西？",
    "人群分层各有多少人？",
    "哪种产品卖得最多？",
    "哪个类目最火？",
    "转化率是多少？",
]

SHOULD_REJECT = [
    "昨天的天气怎么样？",
    "竞品的销量是多少？",
    "退款率是多少？",
    "用户用的是安卓还是苹果？",
    "商品的平均评分是多少？",
    "物流平均几天能到？",
    "根据已有数据推测明天谁会买？",
    "买得最多的那个用户叫什么名字？",
    "这场大促带来了多少 GMV？",
    "用户的年龄分布是怎样的？",
    "客服平均响应时长是多少？",
    "优惠券的核销率是多少？",
]

# 题干 + 答案里必须出现的数字串
NAILS = [
    ("这九天一共来了多少人？",      "987,991"),      # 需要下面新增的 uv_9d 指标
    ("这九天一共有多少人买过东西？", "672,404"),      # buyers_9d
    ("这九天一共浏览了多少次？",     "89,660,671"),
    ("有多少人没买过东西？",         "315,587"),
    ("人群分层各有多少人？",         "987,991"),
    ("哪种产品卖得最多？",           "【明细】"),     # 排名题必须给清单，不能只给一个数
]


# ---------------------------------------------------------------------------
# 二、跑一条
# ---------------------------------------------------------------------------
def run_one(con, q):
    """返回 (状态, 结果或异常)。状态 ∈ answered / rejected / error。"""
    try:
        r = ask(q, con)
    except NotSupported:
        return "rejected", None
    except Exception as e:                     # 拼 SQL / 执行 / 出话阶段的任何异常
        return "error", f"{type(e).__name__}: {e}"
    return "answered", r


# ---------------------------------------------------------------------------
# 三、判分
# ---------------------------------------------------------------------------
def judge(kind, status, payload, nail=None):
    if status == "error":
        return False, f"崩了 -> {payload}"
    if kind == "can_answer":
        return (status == "answered"), "答了" if status == "answered" else "该答却拒了"
    if kind == "should_reject":
        return (status == "rejected"), "拒了" if status == "rejected" else "该拒却答了"
    # nail
    if status != "answered":
        return False, "没答出来"
    ok = nail in payload["answer"]
    return ok, "数字对" if ok else f"答案里找不到 {nail}"


# ---------------------------------------------------------------------------
# 四、自测
# ---------------------------------------------------------------------------
def _boot():
    """14_ai_bootstrap.py 文件名以数字开头，不能直接 import。"""
    spec = importlib.util.spec_from_file_location(
        "boot", r"F:\data_warehouse\scripts\14_ai_bootstrap.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


if __name__ == "__main__":
    con = _boot().connect()

    cases = ([(q, "can_answer", None) for q in CAN_ANSWER]
             + [(q, "should_reject", None) for q in SHOULD_REJECT]
             + [(q, "nail", n) for q, n in NAILS])

    score = {"can_answer": [0, 0], "should_reject": [0, 0], "nail": [0, 0]}
    fails = []

    for i, (q, kind, nail) in enumerate(cases, 1):
        status, payload = run_one(con, q)
        ok, why = judge(kind, status, payload, nail)
        score[kind][1] += 1
        if ok:
            score[kind][0] += 1
        else:
            fails.append((i, q, why))

        print(f"{i:>2}. [{'PASS' if ok else 'FAIL'}] {kind:<14}{q}")
        if not ok:
            print(f"           -> {why}")

    total = sum(v[0] for v in score.values())
    n = sum(v[1] for v in score.values())

    print("\n" + "=" * 60)
    print(f"能答组      {score['can_answer'][0]:>2}/{score['can_answer'][1]}")
    print(f"该拒组      {score['should_reject'][0]:>2}/{score['should_reject'][1]}")
    print(f"数字钉子组  {score['nail'][0]:>2}/{score['nail'][1]}")
    print(f"总分        {total:>2}/{n}   准确率 {total / n:.1%}")

    if fails:
        print("\n待修清单：")
        for i, q, why in fails:
            print(f"  #{i} {q}  ->  {why}")

    print("\n判定：", "[OK] 全部通过" if total == n else f"[WARN] 有 {n - total} 条没过")