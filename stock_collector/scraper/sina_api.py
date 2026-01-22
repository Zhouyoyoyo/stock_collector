import json
from threading import Lock

import requests

from stock_collector.config.settings import get_url
from stock_collector.ops.debug_bundle import DEBUG_DIR

_SESSION = None
_SESSION_LOCK = Lock()


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    with _SESSION_LOCK:
        if _SESSION is not None:
            return _SESSION
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=50,
            pool_maxsize=50,
            max_retries=3,
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _SESSION = s
        return _SESSION


def _safe_float(v) -> float:
    try:
        if v is None:
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _maybe_write_raw_first_error(
    symbol: str,
    url: str,
    params: dict,
    response: requests.Response | None,
    exc: Exception,
) -> None:
    path = DEBUG_DIR / "raw_first_error.json"
    if path.exists():
        return
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "symbol": symbol,
        "url": url,
        "params": params,
        "status_code": response.status_code if response is not None else None,
        "response_text": response.text[:2048] if response is not None else None,
        "exception": repr(exc),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_daily_bar_from_sina_api(symbol: str, trade_date: str) -> dict:
    url = get_url("sina_kline_api")
    params = {"symbol": symbol, "scale": 240, "ma": "no", "datalen": 1}

    s = _session()
    response = None
    try:
        response = s.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        if not data:
            raise RuntimeError("API_MISSING")

        bar = data[0]
        day = bar.get("day")
        if day != trade_date:
            raise RuntimeError("API_MISSING")

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
            "volume": int(float(bar["volume"])),
            "amount": _safe_float(bar.get("amount")),
            "pre_close": _safe_float(bar.get("preclose")),
            "change": close_p - open_p,
            "change_pct": (close_p - open_p) / open_p * 100 if open_p else 0.0,
            "source": "sina_api",
        }
    except Exception as exc:
        _maybe_write_raw_first_error(symbol, url, params, response, exc)
        raise
