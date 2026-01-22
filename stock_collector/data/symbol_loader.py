import pandas as pd


def load_tradeable_a_share_symbols(trade_date: str) -> list[str]:
    import tushare as ts

    pro = ts.pro_api()

    df = pro.daily_basic(
        trade_date=trade_date.replace("-", ""),
        fields="ts_code,trade_status",
    )

    df = df[df["trade_status"] == "交易"]
    symbols = df["ts_code"].astype(str).tolist()

    if len(symbols) < 4000:
        raise RuntimeError(
            f"A_SHARE_SYMBOLS_INCOMPLETE: only {len(symbols)} tradeable stocks on {trade_date}"
        )

    return symbols
