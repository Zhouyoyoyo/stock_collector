import requests


def fetch_daily_bar_from_sina_api(symbol: str, trade_date: str) -> dict:
    """
    主路径：新浪 JSON 行情接口
    口径统一：
    - volume = 成交股数
    - 字段能拿的全部拿
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

    open_p = float(bar["open"])
    close_p = float(bar["close"])

    return {
        "symbol": symbol,
        "trade_date": bar["day"],
        "open": open_p,
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": close_p,
        "volume": int(float(bar["volume"])),  # 股数（API 本身已是股）
        "amount": float(bar.get("amount", 0)),
        "pre_close": float(bar.get("preclose", 0)),
        "change": close_p - open_p,
        "change_pct": (close_p - open_p) / open_p * 100 if open_p else 0,
        "source": "sina_api",
    }
