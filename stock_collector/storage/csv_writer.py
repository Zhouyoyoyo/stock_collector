from pathlib import Path
import csv
from typing import List, Dict


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


def write_symbol_csv(
    base_dir: Path,
    trade_date: str,
    symbol: str,
    rows: List[Dict],
):
    """
    强制规则：
    - 每个 symbol 一个 CSV
    - 即使 rows 为空，也必须生成 CSV（只写 header）
    """
    day_dir = base_dir / trade_date
    day_dir.mkdir(parents=True, exist_ok=True)

    path = day_dir / f"{symbol}.csv"

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in CSV_COLUMNS})


def write_summary_csv(
    base_dir: Path,
    trade_date: str,
    summary_rows: List[Dict],
):
    path = base_dir / trade_date / "_summary.csv"

    if not summary_rows:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
        return

    columns = list(summary_rows[0].keys())

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for r in summary_rows:
            writer.writerow(r)
