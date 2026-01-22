from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache


@lru_cache(maxsize=4)
def _get_xshg_calendar():
    import exchange_calendars as xcals

    # XSHG = Shanghai Stock Exchange calendar
    return xcals.get_calendar("XSHG")


def is_calendar_trading_day(date_value: str) -> bool:
    """
    仅基于交易所日历判断是否开市
    不允许读取 summary / 执行结果
    """
    cal = _get_xshg_calendar()
    import pandas as pd
    from datetime import date

    d = date.fromisoformat(date_value)
    ts = pd.Timestamp(d)

    # 交易所日历边界
    first = cal.sessions[0].date()
    last = cal.sessions[-1].date()

    # 早于最早日历：肯定是调用方 bug
    if d < first:
        raise RuntimeError(f"DATE_TOO_EARLY: {d}")

    try:
        return bool(cal.is_session(ts))
    except Exception:
        # 日历未覆盖到“今天/最近日期”时：
        # 直接认为是交易日，让采集阶段用“是否有数据”来裁决
        return True


def is_trading_day(d: date | datetime) -> bool:
    """
    严格交易日判断（XSHG 交易所日历）
    """
    if isinstance(d, datetime):
        d = d.date()

    return is_calendar_trading_day(d.isoformat())


def ensure_trading_day_or_raise(d: date | datetime):
    """
    若非交易日，直接抛出 RuntimeError，让主流程跳过执行（不会制造 missing 噪音）。
    """
    if not is_trading_day(d):
        raise RuntimeError("NOT_TRADING_DAY")
