import requests


def _session() -> requests.Session:
    """
    带重试的 Session（最小侵入，不引入新库）
    """
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=50,
        pool_maxsize=50,
        max_retries=3,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _safe_float(v) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def fetch_daily_bar_from_sina_api(symbol: str, trade_date: str) -> dict:
    """
    主路径：新浪 JSON K 线接口
    - 若返回空/日期不匹配/字段不全：抛 RuntimeError(API_MISSING)
    - 其他错误：原样抛出（会进入 api_failed）
    """
    url = "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketData.getKLineData"
    params = {"symbol": symbol, "scale": 240, "ma": "no", "datalen": 1}

    s = _session()
    r = s.get(url, params=params, timeout=10)
    r.raise_for_status()

    data = r.json()
    if not data:
        raise RuntimeError("API_MISSING")

    bar = data[0]
    day = bar.get("day")
    if day != trade_date:
        raise RuntimeError("API_MISSING")

    # 必需字段（OHLCV）缺失则视为 missing
    for k in ("open", "high", "low", "close", "volume"):
        if k not in bar or bar[k] in (None, "", "--"):
            raise RuntimeError("API_MISSING")

    open_p = float(bar["open"])
    close_p = float(bar["close"])

    return {
        "symbol": symbol,
        "trade_date": day,
        "open": open_p,
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": close_p,
        # API volume 本身通常是股数；统一转 int
        "volume": int(float(bar["volume"])),
        "amount": _safe_float(bar.get("amount")),
        "pre_close": _safe_float(bar.get("preclose")),
        "change": close_p - open_p,
        "change_pct": (close_p - open_p) / open_p * 100 if open_p else 0.0,
        "source": "sina_api",
    }
