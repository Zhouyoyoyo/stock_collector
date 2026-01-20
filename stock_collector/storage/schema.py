from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class DailyBar:
    """日线数据结构。"""

    symbol: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    change: float
    change_pct: float
    volume: int
    amplitude_pct: float
    turnover_pct: float
    amount: float | None = None
    price_type: str = "raw"
    source: str = "sina"
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class CollectStatus:
    """采集状态结构。"""

    trade_date: str
    symbol: str
    status: str
    retry_count: int
    last_error: str
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
