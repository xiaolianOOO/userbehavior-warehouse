# scripts/12_export_ads_csv.py
# 把 4 张 ADS 小表从 Parquet 导出成 CSV，给看板用（共 37 行，不到 15 KB）
import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import pandas as pd

ADS = "F:/data_warehouse/data/ads"
OUT = "F:/data_warehouse/data/ads_csv"
TABLES = ["ads_traffic", "ads_retention", "ads_funnel", "ads_rf_segment"]
EXPECT = {"ads_traffic": 9, "ads_retention": 9, "ads_funnel": 9, "ads_rf_segment": 10}

os.makedirs(OUT, exist_ok=True)

total = 0
for name in TABLES:
    df = pd.read_parquet(f"{ADS}/{name}")          # pyarrow 直接读 Spark 写的目录
    assert len(df) == EXPECT[name], f"{name} 行数 {len(df)} != {EXPECT[name]}"
    df.to_csv(f"{OUT}/{name}.csv", index=False, encoding="utf-8-sig")   # utf-8-sig：Excel 打开不乱码
    total += len(df)
    print(f"[OK] {name:16s} {len(df):3d} 行  ->  {name}.csv")

assert total == 37, f"合计 {total} != 37"
print(f"\n[OK] 合计 {total} 行，对账通过")