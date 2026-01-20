import csv
from pathlib import Path

from stock_collector.storage.schema import DailyBar


def export_daily_bars(path: str, bars: list[DailyBar]) -> None:
    """导出日线数据到 CSV。"""
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.writer(file_handle)
        writer.writerow(
            [
                "symbol",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "change",
                "change_pct",
                "volume",
                "amplitude_pct",
                "turnover_pct",
                "amount",
                "price_type",
                "source",
                "updated_at",
            ]
        )
        for bar in bars:
            writer.writerow(
                [
                    bar.symbol,
                    bar.trade_date,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.change,
                    bar.change_pct,
                    bar.volume,
                    bar.amplitude_pct,
                    bar.turnover_pct,
                    bar.amount,
                    bar.price_type,
                    bar.source,
                    bar.updated_at,
                ]
            )
