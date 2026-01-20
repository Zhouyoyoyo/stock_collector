import requests
from datetime import datetime


def fetch_daily_bar_from_sina_api(symbol: str, trade_date: str) -> dict:
    """
    主路径：使用新浪行情 JSON 接口抓取日线数据
    返回口径与 DOM 抓取完全一致（volume=股数）
    """
    url = "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketData.getKLineData"
    params = {
        "symbol": symbol,
        "scale": 240,
        "ma": "no",
        "datalen": 1,
    }

    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()

    if not data:
        raise RuntimeError("API_EMPTY")

    bar = data[0]
    if bar.get("day") != trade_date:
        raise RuntimeError("API_DATE_MISMATCH")

    return {
        "symbol": symbol,
        "trade_date": bar["day"],
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"]),
        "volume": int(float(bar["volume"])),  # API 本身就是股数
        "change": float(bar["close"]) - float(bar["open"]),
        "change_pct": (float(bar["close"]) - float(bar["open"])) / float(bar["open"]) * 100,
        "source": "sina_api",
    }
