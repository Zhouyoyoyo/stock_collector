from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache


# 获取上交所交易日历（带缓存）
@lru_cache(maxsize=4)
def _get_xshg_calendar():
    import exchange_calendars as xcals

    return xcals.get_calendar("XSHG")


# 判断指定日期是否为交易日（基于交易日历）
def is_calendar_trading_day(date_value: str) -> bool:
    # 获取日历对象
    cal = _get_xshg_calendar()
    import pandas as pd
    from datetime import date

    # 解析日期并转换为时间戳
    d = date.fromisoformat(date_value)
    ts = pd.Timestamp(d)

    first = cal.sessions[0].date()
    last = cal.sessions[-1].date()

    # 过早日期直接报错
    if d < first:
        raise RuntimeError(f"DATE_TOO_EARLY: {d}")

    # 判断是否为交易日，异常时回退为 True
    try:
        return bool(cal.is_session(ts))
    except Exception:
        return True


# 判断日期对象是否为交易日
def is_trading_day(d: date | datetime) -> bool:
    # 统一到 date 类型
    if isinstance(d, datetime):
        d = d.date()

    return is_calendar_trading_day(d.isoformat())


# 确保为交易日，否则抛错
def ensure_trading_day_or_raise(d: date | datetime):
    if not is_trading_day(d):
        raise RuntimeError("NOT_TRADING_DAY")
