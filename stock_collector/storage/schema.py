from dataclasses import dataclass, field
from datetime import datetime


# 日线行情数据模型
@dataclass
class DailyBar:
    # 股票代码
    symbol: str
    # 交易日期
    trade_date: str
    # 开盘价
    open: float
    # 最高价
    high: float
    # 最低价
    low: float
    # 收盘价
    close: float
    # 涨跌额
    change: float
    # 涨跌幅
    change_pct: float
    # 成交量
    volume: int
    # 振幅
    amplitude_pct: float
    # 换手率
    turnover_pct: float
    # 成交额（可选）
    amount: float | None = None
    # 价格类型标识
    price_type: str = "raw"
    # 数据来源
    source: str = "sina"
    # 更新时间（UTC）
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


# 采集状态数据模型
@dataclass
class CollectStatus:
    # 交易日期
    trade_date: str
    # 股票代码
    symbol: str
    # 状态描述
    status: str
    # 重试次数
    retry_count: int
    # 最近错误信息
    last_error: str
    # 更新时间（UTC）
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
