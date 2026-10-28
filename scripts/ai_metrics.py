# scripts/ai_metrics.py —— 指标口径字典（Metric Dictionary）
# 作用：穷举"允许被计算的东西"，让 NL2SQL 从"开放生成"降级为"封闭选择"。
# 每个指标必须写清：来源表 / 用到哪些列 / 怎么算 / 单位 / 粒度 / 不能怎么用。

METRICS = {
    "uv": {
        "cn": "日活用户数",
        "table": "dws_user_day",
        "cols": [],
        "expr": "COUNT(*)",
        "unit": "人",
        "grain": "日",
        "note": "dws_user_day 已按 (dt,user_id) 去重，一行=一个人一天，所以行数就是 UV。单日 UV 不可跨天相加。",
    },
    "pv_total": {
        "cn": "浏览次数",
        "table": "dws_user_day",
        "cols": ["pv_cnt"],
        "expr": "SUM(pv_cnt)",
        "unit": "次",
        "grain": "日",
        "note": "pv 是页面打开次数，不是点击数。可跨天相加。",
    },
    "buy_users": {
        "cn": "购买人数",
        "table": "dws_user_day",
        "cols": ["has_buy"],
        "expr": "SUM(has_buy)",
        "unit": "人",
        "grain": "日",
        "note": "has_buy 是 0/1 标记，SUM 出来就是人数。跨天求总人数必须用 COUNT(DISTINCT user_id)。",
    },
    "buy_actions": {
        "cn": "购买次数",
        "table": "dws_user_day",
        "cols": ["buy_cnt"],
        "expr": "SUM(buy_cnt)",
        "unit": "次",
        "grain": "日",
        "note": "可跨天相加。购买次数 ÷ 购买人数 = 人均下单次数。",
    },
    "pv_per_user": {
        "cn": "人均浏览次数",
        "table": "dws_user_day",
        "cols": ["pv_cnt"],
        "expr": "SUM(pv_cnt) / COUNT(*)",
        "unit": "次/人",
        "grain": "日",
        "note": "分母是当天 UV。跨天算要用加权平均，不能把每天的人均值再平均。",
    },
    "cart_rate": {
        "cn": "加购率",
        "table": "dws_user_day",
        "cols": ["has_cart"],
        "expr": "SUM(has_cart) / COUNT(*)",
        "unit": "比率",
        "grain": "日",
        "note": "分子是加购人数、分母是当天 UV。这是筛选样本，33% 远高于真实电商，只能做相对比较。",
    },
    "buy_rate": {
        "cn": "购买率（日口径）",
        "table": "dws_user_day",
        "cols": ["has_buy"],
        "expr": "SUM(has_buy) / COUNT(*)",
        "unit": "比率",
        "grain": "日",
        "note": "日口径 18%~21%；9 天累计口径是 68.06%。两者都对，但必须说明周期，差了 3.5 倍。",
    },
    "d1_rate": {
        "cn": "次日留存率",
        "table": "ads_retention",
        "cols": ["d1_rate"],
        "expr": "AVG(d1_rate)",
        "unit": "比率",
        "grain": "日",
        "note": "只有 8 天有效，12-03 是 NULL（没有 12-04），不能当 0。约 78%~80%，远高于真实电商。",
    },
    "conv_browse_to_buy": {
        "cn": "浏览→购买转化率（日口径）",
        "table": "ads_funnel",
        "cols": ["r_total"],
        "expr": "AVG(r_total)",
        "unit": "比率",
        "grain": "日",
        "note": "日口径 7.7%~9.0%，9 天累计口径 53.69%。这是集合漏斗（不问先后），只问当天做没做过。",
    },
    "buyers_9d": {
        "cn": "9 天累计买家人数",
        "table": "dws_user_day",
        "cols": ["has_buy", "user_id"],
        "expr": "COUNT(DISTINCT CASE WHEN has_buy = 1 THEN user_id END)",
        "unit": "人",
        "grain": "全期",
        "note": "必须去重。直接把每天购买人数相加会重复计算，得出大于真实值的数。",
    },
    "item_pv": {
        "cn": "单商品浏览次数",
        "table": "dws_item_day",
        "cols": ["pv_cnt"],
        "expr": "SUM(pv_cnt)",
        "unit": "次",
        "grain": "日",
        "note": "商品表没有 has_pv 列；要判断'当天被浏览过'请用 pv_cnt > 0。",
    },
    "category_uv": {
        "cn": "类目日活用户数（人次需去重）",
        "table": "dws_category_day",
        "cols": ["uv"],
        "expr": "SUM(uv)",
        "unit": "人次",
        "grain": "日",
        "note": "这是各类目 uv 之和，同一人可能出现在多个类目，所以是'人次'不是'人数'，加起来会大于总 UV。",
    },


        "item_buy_rank": {
        "cn": "商品销量排名",
        "table": "dws_item_day",
        "cols": ["item_id", "buy_cnt"],
        "expr": "SUM(buy_cnt)",
        "unit": "次",
        "grain": "全期",
        "dims": ["item_id"],
        "note": "按 9 天购买次数排名。商品没有金额，所以这是「卖得最多」的唯一算法。"
                "排名靠前不等于赚钱多——没有价格就算不出营收。",
        "synonyms": ["哪种产品卖得最多", "卖得最好的商品", "爆款商品", "销量排名"],
    },

    "item_pv_rank": {
        "cn": "商品浏览排名",
        "table": "dws_item_day",
        "cols": ["item_id", "pv_cnt"],
        "expr": "SUM(pv_cnt)",
        "unit": "次",
        "grain": "全期",
        "dims": ["item_id"],
        "note": "按 9 天浏览次数排名。头部极平：第一名只占全站浏览 0.034%，"
                "所以要按分位数分段看，别只盯 TOP10。",
        "synonyms": ["哪个商品最火", "看的人最多的商品", "热门商品"],
    },

    "category_pv_rank": {
        "cn": "类目浏览排名",
        "table": "dws_category_day",
        "cols": ["category_id", "pv_cnt"],
        "expr": "SUM(pv_cnt)",
        "unit": "次",
        "grain": "全期",
        "dims": ["category_id"],
        "note": "类目只有 ID 没有名称。类目层比商品层集中得多：TOP10 类目占 28%，"
                "而 TOP10 商品只占 0.23%。",
        "synonyms": ["哪个类目最火", "最热的类目", "类目排名"],
    },


    "non_buyers": {
        "cn": "未购买人数",
        "table": "dws_user_day",
        "cols": ["user_id", "has_buy"],
        "expr": ("COUNT(DISTINCT user_id) - "
                 "COUNT(DISTINCT CASE WHEN has_buy = 1 THEN user_id END)"),
        "unit": "人",
        "grain": "全期",
        "note": "9 天里一次都没买过的去重人数。不能写成 WHERE has_buy = 0——那是「某一天没买」，"
                "会把每天都买的人也算进去。与 9 天累计买家相加应等于 987,991。",
        "synonyms": ["没买过的人", "没下单的人", "零购买用户", "哪些人没买过", "有多少人没买过"],
    },



    "uv_9d": {
        "cn": "全期去重用户数",
        "table": "dws_user_day",
        "cols": ["user_id"],
        "expr": "COUNT(DISTINCT user_id)",
        "unit": "人",
        "grain": "全期",
        "note": "九天里出现过至少一次的人。单日 UV 相加得到的是人次，不是这个数——"
                "差在「同一个人被数了几次」。",
        "synonyms": ["九天一共来了多少人", "总人数", "去重用户数", "全期用户"],
    },




    "rf_users": {
        "cn": "RF 分层人数",
        "table": "ads_rf_segment",
        "cols": ["r_seg", "f_seg", "uv"],
        "expr": "SUM(uv)",
        "unit": "人",
        "grain": "全期",
        "dims": ["r_seg", "f_seg"],
        "note": "R 是距最后一次购买天数、F 是 9 天购买次数，都在买家内部三分。"
                "没有 M（金额），因为数据无金额字段。RN 是 9 天一次都没买的人。",
        "synonyms": ["人群分层", "RF 分层", "客户分群", "买家分几类"],
    },
    
    "non_buyers": {
        "cn": "未购买人数",
        "table": "dws_user_day",
        "cols": ["user_id", "has_buy"],
        "expr": ("COUNT(DISTINCT user_id) - "
                 "COUNT(DISTINCT CASE WHEN has_buy = 1 THEN user_id END)"),
        "unit": "人",
        "grain": "全期",
        "note": "9 天里一次都没买过的去重人数。不能写成 WHERE has_buy = 0——那是「某一天没买」，"
                "会把每天都买的人也算进去。与 9 天累计买家相加应等于 987,991。",
        "synonyms": ["没买过的人", "没下单的人", "零购买用户", "哪些人没买过", "有多少人没买过"],
    },

    "rf_users": {
        "cn": "RF 分层人数",
        "table": "ads_rf_segment",
        "cols": ["r_seg", "f_seg", "uv"],
        "expr": "SUM(uv)",
        "unit": "人",
        "grain": "全期",
        "dims": ["r_seg", "f_seg"],
        "note": "R 是距最后一次购买天数、F 是 9 天购买次数，都在买家内部三分。"
                "没有 M（金额），因为数据无金额字段。RN 是 9 天一次都没买的人。",
        "synonyms": ["人群分层", "RF 分层", "客户分群", "买家分几类"],
    },
}


DIMENSIONS = {
    "dt": {"cn": "日期（北京时区）", "type": "DATE", "range": "2017-11-25 ~ 2017-12-03，共 9 天"},
    "user_id": {"cn": "用户 ID", "type": "BIGINT", "note": "脱敏流水号，不是真人"},
    "item_id": {"cn": "商品 ID", "type": "BIGINT", "note": "没有商品名称"},
    "category_id": {"cn": "类目 ID", "type": "BIGINT", "note": "没有类目名称"},
    "r_seg": {"cn": "R 档（最近购买）", "type": "VARCHAR", "note": "R1/R2/R3/RN"},
    "f_seg": {"cn": "F 档（购买频次）", "type": "VARCHAR", "note": "F1/F2/F3/F0"},
}





def verify(con):
    """
    自检：每个指标用的列必须真实存在于它的来源表里。
    写错列名会立刻报错，而不是等到 AI 生成 SQL 时才炸。
    """
    schemas = {}
    for table in {m["table"] for m in METRICS.values()}:
        schemas[table] = {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}

    bad = []
    for name, m in METRICS.items():
        have = schemas[m["table"]]
        for c in m["cols"]:
            if c not in have:
                bad.append(f"{name}: {m['table']} 里没有列 {c}")

    assert not bad, "口径字典自检失败：\n  " + "\n  ".join(bad)
    print(f"[OK] 口径字典自检通过：{len(METRICS)} 个指标 / {len(DIMENSIONS)} 个维度，列名全部存在")


def render():
    """把字典渲染成一段纯文本，G5 会把它塞进模型的提示词（prompt）里。"""
    lines = ["可用指标（只能从这些里选，不要自己发明计算方式）："]
    for name, m in METRICS.items():
        lines.append(
            f"- {name}（{m['cn']}｜{m['unit']}｜{m['grain']}）"
            f" = {m['expr']}  FROM {m['table']}"
            f"  注意：{m['note']}"
        )
    lines.append("")
    lines.append("可用维度：")
    for name, d in DIMENSIONS.items():
        extra = d.get("range") or d.get("note") or ""
        lines.append(f"- {name}（{d['cn']}｜{d['type']}）{extra}")
    return "\n".join(lines)


if __name__ == "__main__":
    import duckdb
    from importlib import import_module

    boot = import_module("ai_bootstrap") if False else None  # 占位，实际直接调 connect

    con = duckdb.connect()
    DATA = "F:/data_warehouse/data"
    for name, pattern in {
        "dws_user_day":     f"{DATA}/dws/user_day/dt=*/*.parquet",
        "dws_item_day":     f"{DATA}/dws/item_day/dt=*/*.parquet",
        "dws_category_day": f"{DATA}/dws/category_day/dt=*/*.parquet",
        "ads_retention":    f"{DATA}/ads/ads_retention/*.parquet",
        "ads_funnel":       f"{DATA}/ads/ads_funnel/*.parquet",
    }.items():
        con.execute(
            f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true)"
        )

    verify(con)
    print("\n" + "=" * 60)
    print(render())