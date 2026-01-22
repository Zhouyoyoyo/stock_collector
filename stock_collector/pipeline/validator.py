from stock_collector.storage.schema import DailyBar


class MissingBarError(RuntimeError):
    def __init__(self, symbol: str, trade_date: str, message: str) -> None:
        super().__init__(f"{message} (symbol={symbol}, date={trade_date}, source=sina)")


def validate_bar(bar: DailyBar) -> list[str]:
    errors: list[str] = []
    if bar.open <= 0 or bar.high <= 0 or bar.low <= 0 or bar.close <= 0:
        errors.append("价格必须为正数")
    if bar.high < bar.low:
        errors.append("最高价低于最低价")
    if not (bar.low <= bar.open <= bar.high):
        errors.append("开盘价不在高低区间")
    if not (bar.low <= bar.close <= bar.high):
        errors.append("收盘价不在高低区间")
    if bar.volume < 0:
        errors.append("成交量为负")
    return errors
