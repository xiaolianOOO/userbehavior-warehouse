# ============================================================================
# scripts/ai_report.py —— G7 指标简报（带 evidence 的一段话）
#
# 铁律：正文里每个阿拉伯数字都必须来自 evidence；
#       结构性数量用中文写（九天 / 九格 / 一格），否则校验器会把 "R3_F3" 里的 3 当成数字。
# ============================================================================

from ai_evidence import check_text, collect, render


# ---------------------------------------------------------------------------
# 一、取数：唯一入口，取不到就报错，绝不返回默认值
# ---------------------------------------------------------------------------
def _ev(evs, metric, key):
    """按 指标 + 标签关键词 找一条 evidence。"""
    for e in evs:
        if e["metric"] == metric and key in e["label"]:
            return e
    raise KeyError(f"evidence 里没有 {metric} / {key}")


def _s(evs, metric, key):
    """只要显示串（带千分位 / 保留四位小数）。"""
    return _ev(evs, metric, key)["shown"]


# ---------------------------------------------------------------------------
# 二、写简报：全是占位符，没有一个手打的数字
# ---------------------------------------------------------------------------
def build(evs) -> str:
    total  = _s(evs, "rf_users",   "全期合计")   # 987,991
    buyers = _s(evs, "buyers_9d",  "全期合计")   # 672,404
    nobody = _s(evs, "non_buyers", "全期合计")   # 315,587

    uv0    = _s(evs, "uv", "首日")
    uv1    = _s(evs, "uv", "末日")
    uvmax  = _s(evs, "uv", "最高")

    pv     = _s(evs, "pv_total", "全期合计")

    br_lo  = _s(evs, "buy_rate", "最低")
    br_hi  = _s(evs, "buy_rate", "最高")

    d1_0   = _s(evs, "d1_rate", "首日")
    d1_hi  = _s(evs, "d1_rate", "最高")

    cv_0   = _s(evs, "conv_browse_to_buy", "首日")
    cv_hi  = _s(evs, "conv_browse_to_buy", "最高")

    r3f3   = _s(evs, "rf_users", "R3_F3")
    rn     = _s(evs, "rf_users", "RN")

    return "\n".join([
        f"【一句话】九天里一共 {total} 个人来过，其中 {buyers} 人买过东西，{nobody} 人一次都没买。",
        f"【流量】日活从第一天的 {uv0} 人涨到最后一天的 {uv1} 人，峰值 {uvmax} 人。",
        f"【浏览】九天累计浏览 {pv} 次。",
        f"【购买】日购买率在 {br_lo} 到 {br_hi} 之间波动。",
        f"【留存】次日留存率第一天是 {d1_0}，最高 {d1_hi}。",
        f"【转化】浏览到购买的日转化率第一天 {cv_0}，最高 {cv_hi}。",
        f"【分层】买过多次但最近没再来的那一格有 {r3f3} 人；一次没买的 RN 有 {rn} 人。",
    ])


# ---------------------------------------------------------------------------
# 三、自测
# ---------------------------------------------------------------------------
def _boot():
    """14_ai_bootstrap.py 文件名以数字开头，不能直接 import。"""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "boot", r"F:\data_warehouse\scripts\14_ai_bootstrap.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


if __name__ == "__main__":
    evs = collect(_boot().connect())
    body = build(evs)

    print("=" * 72)
    print("指标体系简报（每个数字都能溯源）")
    print("=" * 72)
    print(body)

    ok, bad, hit = check_text(body, evs)
    used = len({e["id"] for e in hit})

    print("\n=== 校验 ===")
    print(f"正文数字全部有出处：{'[OK]' if ok else '[FAIL]'}   无出处的数字 = {bad}")
    print(f"引用了 {used} 条 evidence（总共 {len(evs)} 条）")

    fake = body + "另外还有个数字 999,999。"
    ok2, bad2, _ = check_text(fake, evs)
    print(f"故意塞一个假数字：{'[OK]' if bad2 else '[FAIL]'}   抓到的 = {bad2}")

    print("\n判定：", "[OK] 简报可用" if (ok and bad2) else "[FAIL] 简报有问题")

    print("\n=== 附录：evidence 清单 ===")
    print(render(evs))