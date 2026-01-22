import pandas as pd


def load_tradeable_a_share_symbols(trade_date: str) -> list[str]:
    """
    返回指定交易日「可交易的全量 A 股股票代码」
    - 不包含停牌
    - 不包含退市
    """
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
