from datetime import date
from pathlib import Path

import yaml
from dateutil import tz

CACHE_PATH = Path("stock_collector/meta/cache/trading_days.json")
SCHEDULE_CONFIG = "stock_collector/config/schedule.yaml"


def _market_timezone():
    with open(SCHEDULE_CONFIG, "r", encoding="utf-8") as file_handle:
        config = yaml.safe_load(file_handle)
    return tz.gettz(config.get("timezone_market", "Asia/Shanghai"))


def is_trading_day(target_date: date) -> bool:
    """判断是否交易日（默认工作日）。"""
    # 预留缓存接口
    if CACHE_PATH.exists():
        try:
            content = CACHE_PATH.read_text(encoding="utf-8")
            if target_date.isoformat() in content:
                return True
        except Exception:
            pass
    # 简化策略：周一至周五视为交易日
    return target_date.weekday() < 5
