# scripts/ai_fewshot.py —— L2 示例层（few-shot library）
# 作用：给模型看「问题 → 指标计划」的样例，纠正它选指标/选维度的习惯。
# 边界：这里只放样例和检索，不执行任何 SQL。执行仍在 ai_nl2sql.py。
#
# 为什么要检索而不是全塞：
#   22 条全塞进提示词约 1200 字，每次多花 token，而且不相关的样例会干扰判断。
#   检索只塞 6 条（4 条最相关 + 2 条拒答示范），既省 token 又更准。
#   面试可以讲成「我做了示例检索，对齐 Uber QueryGPT 的做法」。

import json

# ---------------------------------------------------------------------------
# 一、示例库：问题 → 指标计划
#     plan 用字符串 "NOT_SUPPORTED" 表示该拒答。
# ---------------------------------------------------------------------------

FEWSHOTS = [
    # ---- 日活 / 流量 ----
    {"q": "每天有多少人来？",
     "tags": ["每天", "多少人", "人来", "用户", "活跃", "日活"],
     "plan": {"metric": "uv", "dims": ["dt"]}},

    {"q": "这九天一共浏览了多少次？",
     "tags": ["一共", "浏览", "次数", "pv", "浏览量"],
     "plan": {"metric": "pv_total", "dims": []}},

    {"q": "人均每天看几个页面？",
     "tags": ["人均", "每人", "几个页面", "浏览页数"],
     "plan": {"metric": "pv_per_user", "dims": ["dt"]}},

    {"q": "每个人平均看多少页？",
     "tags": ["平均", "多少页", "人均浏览"],
     "plan": {"metric": "pv_per_user", "dims": []}},

    # ---- 购买 ----
    {"q": "每天有多少人买了东西？",
     "tags": ["每天", "买了", "买家", "购买人数", "下单人"],
     "plan": {"metric": "buy_users", "dims": ["dt"]}},

    {"q": "一共成交了多少次？",
     "tags": ["一共", "成交", "多少单", "购买次数"],
     "plan": {"metric": "buy_actions", "dims": []}},

    {"q": "九天一共有多少人买过东西？",
     "tags": ["九天", "一共", "多少人买过", "累计买家"],
     "plan": {"metric": "buyers_9d", "dims": []}},

    {"q": "有多少人没买过东西？",
     "tags": ["没买过", "没下单", "未购买", "多少人没"],
     "plan": {"metric": "non_buyers", "dims": []}},

    # ---- 比率类 ----
    {"q": "购买率怎么样？",
     "tags": ["购买率", "买的人占比"],
     "plan": {"metric": "buy_rate", "dims": ["dt"]}},

    {"q": "加购率是多少？",
     "tags": ["加购率", "购物车"],
     "plan": {"metric": "cart_rate", "dims": ["dt"]}},

    {"q": "次日留存如何？",
     "tags": ["留存", "次日", "回访"],
     "plan": {"metric": "d1_rate", "dims": ["dt"]}},

    {"q": "转化率是多少？",
     "tags": ["转化率", "转化", "成交率"],
     "plan": {"metric": "conv_browse_to_buy", "dims": ["dt"]}},

    # ---- 分层 ----
    {"q": "人群分层各有多少人？",
     "tags": ["分层", "人群", "rf", "几格", "客户分群"],
     "plan": {"metric": "rf_users", "dims": ["r_seg", "f_seg"]}},

    {"q": "R3F3 有多少人？",
     "tags": ["r3f3", "高价值", "分层里"],
     "plan": {"metric": "rf_users", "dims": ["r_seg", "f_seg"]}},

    # ---- 排名类 ----
    {"q": "哪种产品卖得最多？",
     "tags": ["哪种产品", "卖得最多", "卖的最好", "销量", "爆款"],
     "plan": {"metric": "item_buy_rank", "dims": ["item_id"],
              "order_by": "item_buy_rank", "order": "desc", "limit": 10}},

    {"q": "哪个商品最火？",
     "tags": ["最火", "热门", "看的人最多", "商品浏览排名"],
     "plan": {"metric": "item_pv_rank", "dims": ["item_id"],
              "order_by": "item_pv_rank", "order": "desc", "limit": 10}},

    {"q": "哪个类目最火？",
     "tags": ["类目", "哪个类目", "品类"],
     "plan": {"metric": "category_pv_rank", "dims": ["category_id"],
              "order_by": "category_pv_rank", "order": "desc", "limit": 10}},

    {"q": "卖得最好的那个商品卖了多少次？",
     "tags": ["那个商品", "卖了多少", "第一名"],
     "plan": {"metric": "item_buy_rank", "dims": ["item_id"],
              "order_by": "item_buy_rank", "order": "desc", "limit": 1}},

    # ---- 明细型 ----
    {"q": "商品被看了多少次？",
     "tags": ["商品", "被看", "浏览多少次"],
     "plan": {"metric": "item_pv", "dims": []}},

    {"q": "每个类目每天多少人看？",
     "tags": ["类目", "多少人看", "类目人数"],
     "plan": {"metric": "category_uv", "dims": ["dt"]}},
]

# ---------------------------------------------------------------------------
# 二、拒答示范：永远塞进提示词，让模型学会「什么该拒绝」
#     只放 2 条——多了会诱导模型过度拒答（上一轮就吃过这个亏）。
# ---------------------------------------------------------------------------

REJECT_EXAMPLES = [
    {"q": "昨天的天气怎么样？",
     "tags": ["天气"],
     "plan": "NOT_SUPPORTED"},
    {"q": "根据已有数据推测明天谁会买？",
     "tags": ["推测", "预测", "明天"],
     "plan": "NOT_SUPPORTED"},
]

# ---------------------------------------------------------------------------
# 三、输出契约补充说明（拼在提示词最后，紧挨着示例）
# ---------------------------------------------------------------------------

PROMPT_ADDON = """
【输出字段说明】只允许下面这几个字段，多一个都不要：
- metric   必填，字符串，只能是上面指标目录里的英文名
- dims     可选，字符串数组，只能是该指标允许的维度；不写就是不分天、不分商品，只算一个总数
- limit    可选，整数，不超过 500
- order_by 可选，字符串，必须是 metric 本身
- order    可选，"desc" 或 "asc"
- filters  禁止输出。日期范围由程序统一加，你不需要写 where。
- sql      禁止输出。

如果问题问的是某一天（例如「12-02 有多少人」），用 dims=["dt"] 让它按天返回，
在答案里自然能看到那一天。不要试图自己过滤日期。
"""

# ---------------------------------------------------------------------------
# 四、检索：按关键词命中数排序，取最相关的几条
# ---------------------------------------------------------------------------

def _score(item, q: str) -> int:
    """命中一个关键词得 1 分。中文没空格，所以用「关键词是否出现在问题里」判断。"""
    low = q.lower()
    return sum(1 for t in item["tags"] if t.lower() in low)


def pick_fewshots(q: str, k: int = 6) -> list:
    """
    返回要拼进提示词的示例列表。
    规则：命中的按分数降序取前 (k - 拒答条数) 条，再补上拒答示范。
    一条都没命中时，只给拒答示范——宁可少给样例，也不塞不相关的。
    """
    scored = [(_score(it, q), i, it) for i, it in enumerate(FEWSHOTS)]
    scored.sort(key=lambda x: (-x[0], x[1]))          # 分数降序，同分保持原顺序
    hit = [it for s, _, it in scored if s > 0]
    n_slot = max(0, k - len(REJECT_EXAMPLES))
    return hit[:n_slot] + REJECT_EXAMPLES


def render_fewshots(q: str, k: int = 6) -> str:
    """把选中的示例渲染成提示词片段。"""
    lines = ["【示例：问题 → 指标计划】"]
    for it in pick_fewshots(q, k):
        plan = it["plan"]
        ans = plan if isinstance(plan, str) else json.dumps(plan, ensure_ascii=False)
        lines.append(f"问：{it['q']}")
        lines.append(f"答：{ans}")
    lines.append(PROMPT_ADDON)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 五、自检：示例里不能出现字典里没有的指标 / 不允许的维度
#     这一步比样例本身更重要——示例写错，模型会照着错的学。
# ---------------------------------------------------------------------------

def self_check() -> None:
    from ai_metrics import METRICS
    from ai_semantic import dims_of

    errs = []
    for it in FEWSHOTS:
        plan = it["plan"]
        if not isinstance(plan, dict):
            continue
        m = plan.get("metric")
        if m not in METRICS:
            errs.append(f"示例「{it['q']}」用了字典里没有的指标：{m}")
            continue
        allowed = set(dims_of(m))
        for d in plan.get("dims", []):
            if d not in allowed:
                errs.append(f"示例「{it['q']}」用了 {m} 不允许的维度：{d}（允许：{sorted(allowed)}）")
        if plan.get("limit", 0) > 500:
            errs.append(f"示例「{it['q']}」limit 超过 500")

    if errs:
        print("[FAIL] 示例自检不通过：")
        for e in errs:
            print("   -", e)
    else:
        n = len(FEWSHOTS) + len(REJECT_EXAMPLES)
        print(f"[OK] 示例自检通过：{len(FEWSHOTS)} 条正例 + {len(REJECT_EXAMPLES)} 条拒答示范 = {n} 条")


if __name__ == "__main__":
    self_check()
    print("\n" + "=" * 62)
    for q in ["每天有多少人买了东西？", "哪种产品卖得最多？", "昨天的天气怎么样？"]:
        print(f"\n【问题】{q}")
        print("【命中的样例】")
        for it in pick_fewshots(q):
            plan = it["plan"]
            print("   -", it["q"], "->",
                  plan if isinstance(plan, str) else json.dumps(plan, ensure_ascii=False))
