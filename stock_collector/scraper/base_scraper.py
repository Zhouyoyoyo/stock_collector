from dataclasses import dataclass


# 抓取结果数据模型
@dataclass
class ScrapeResult:
    # 股票代码
    symbol: str
    # 交易日期
    trade_date: str
    # 抓取的原始数据载荷
    payload: dict
