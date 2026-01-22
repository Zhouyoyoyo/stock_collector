from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache


@lru_cache(maxsize=4)
def _get_xshg_calendar():
    import exchange_calendars as xcals

    return xcals.get_calendar("XSHG")


def is_calendar_trading_day(date_value: str) -> bool:
    cal = _get_xshg_calendar()
    import pandas as pd
    from datetime import date

    d = date.fromisoformat(date_value)
    ts = pd.Timestamp(d)

    first = cal.sessions[0].date()
    last = cal.sessions[-1].date()

    if d < first:
        raise RuntimeError(f"DATE_TOO_EARLY: {d}")

    try:
        return bool(cal.is_session(ts))
    except Exception:
        return True


def is_trading_day(d: date | datetime) -> bool:
    if isinstance(d, datetime):
        d = d.date()

    return is_calendar_trading_day(d.isoformat())


def ensure_trading_day_or_raise(d: date | datetime):
    if not is_trading_day(d):
        raise RuntimeError("NOT_TRADING_DAY")
