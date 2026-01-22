from pathlib import Path
from typing import Dict, List

import pandas as pd


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

TEXT_COLUMNS = {
    "ts_code",
    "symbol",
    "trade_date",
}


def write_symbol_csv(
    base_dir: Path,
    trade_date: str,
    symbol: str,
    rows: List[Dict],
):
    day_dir = base_dir / trade_date
    day_dir.mkdir(parents=True, exist_ok=True)

    path = day_dir / f"{symbol}.csv"

    if rows:
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(columns=CSV_COLUMNS)
    df = df.reindex(columns=CSV_COLUMNS)
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def write_summary_csv(
    base_dir: Path,
    trade_date: str,
    summary_rows: List[Dict],
):
    path = base_dir / trade_date / "_summary.csv"

    if not summary_rows:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8-sig")
        return

    df = pd.DataFrame(summary_rows)
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
    df.to_csv(path, index=False, encoding="utf-8-sig")
