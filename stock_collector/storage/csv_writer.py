from pathlib import Path
from typing import Dict, List

import pandas as pd


# CSV 输出列定义
CSV_COLUMNS = [
    "trade_date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]

# 需要强制转为文本的列
TEXT_COLUMNS = {
    "ts_code",
    "symbol",
    "trade_date",
}


# 写入单个股票的 CSV 文件
def write_symbol_csv(
    base_dir: Path,
    trade_date: str,
    symbol: str,
    rows: List[Dict],
):
    # 生成当日目录
    day_dir = base_dir / trade_date
    day_dir.mkdir(parents=True, exist_ok=True)

    # 生成输出文件路径
    path = day_dir / f"{symbol}.csv"

    # 构建 DataFrame
    if rows:
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(columns=CSV_COLUMNS)
    # 按列顺序重排
    df = df.reindex(columns=CSV_COLUMNS)
    # 强制文本列类型
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    # 写入 CSV 文件
    df.to_csv(path, index=False, encoding="utf-8-sig")


# 写入汇总 CSV 文件
def write_summary_csv(
    base_dir: Path,
    trade_date: str,
    summary_rows: List[Dict],
):
    # 生成汇总文件路径
    path = base_dir / trade_date / "_summary.csv"

    # 处理空汇总场景
    if not summary_rows:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8-sig")
        return

    # 构建 DataFrame 并转换文本列
    df = pd.DataFrame(summary_rows)
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    # 写入 CSV 文件
    df.to_csv(path, index=False, encoding="utf-8-sig")
