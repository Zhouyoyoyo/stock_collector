from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache


@lru_cache(maxsize=4)
def _get_xshg_calendar():
    import exchange_calendars as xcals

    # XSHG = Shanghai Stock Exchange calendar
    return xcals.get_calendar("XSHG")


def is_trading_day(d: date | datetime) -> bool:
    """
    严格交易日判断（XSHG 交易所日历）
    """
    if isinstance(d, datetime):
        d = d.date()

    cal = _get_xshg_calendar()
    # exchange_calendars uses pandas.Timestamp
    import pandas as pd

    ts = pd.Timestamp(d)
    return bool(cal.is_session(ts))


def ensure_trading_day_or_raise(d: date | datetime):
    """
    若非交易日，直接抛出 RuntimeError，让主流程跳过执行（不会制造 missing 噪音）。
    """
    if not is_trading_day(d):
        raise RuntimeError("NOT_TRADING_DAY")
