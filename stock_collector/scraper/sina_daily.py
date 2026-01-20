import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from playwright.sync_api import Page

from stock_collector.storage.schema import DailyBar


@dataclass
class SinaScrapeError(Exception):
    symbol: str
    trade_date: str
    source: str
    message: str

    def __str__(self) -> str:
        return f"{self.message} (symbol={self.symbol}, date={self.trade_date}, source={self.source})"


@dataclass
class SinaMissingError(Exception):
    symbol: str
    trade_date: str
    source: str
    message: str

    def __str__(self) -> str:
        return f"{self.message} (symbol={self.symbol}, date={self.trade_date}, source={self.source})"


def _parse_jsonp(text: str) -> Any:
    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1:
        raise ValueError("无法解析 JSONP 响应")
    payload = text[start : end + 1]
    return json.loads(payload)


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def fetch_daily_bar(page: Page, symbol: str, trade_date: str) -> DailyBar:
    """抓取新浪单日行情。"""
    symbol_lower = symbol.lower()
    source = "sina"
    url = f"https://finance.sina.com.cn/realstock/company/{symbol_lower}/nc.shtml"

    try:
        page.goto(url, wait_until="domcontentloaded")
    except Exception as exc:
        raise SinaScrapeError(symbol, trade_date, source, f"页面加载失败: {exc}")

    api_url = (
        "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_kline="
        f"/CN_MarketData.getKLineData?symbol={symbol_lower}&scale=240&ma=no&datalen=1"
    )

    try:
        raw_text = page.evaluate(
            """
            async (targetUrl) => {
              const response = await fetch(targetUrl);
              return await response.text();
            }
            """,
            api_url,
        )
        payload = _parse_jsonp(raw_text)
        if not payload:
            raise ValueError("行情数据为空")
        latest = payload[0]
    except Exception:
        latest = None

    if not latest:
        try:
            latest = page.evaluate(
                """
                () => {
                  const rows = Array.from(document.querySelectorAll('table tr'));
                  for (const row of rows) {
                    const cells = Array.from(row.querySelectorAll('td')).map(td => td.innerText.trim());
                    if (cells.length >= 6 && /\d{4}-\d{2}-\d{2}/.test(cells[0])) {
                      return {
                        day: cells[0],
                        open: cells[1],
                        high: cells[2],
                        low: cells[3],
                        close: cells[4],
                        volume: cells[5],
                      };
                    }
                  }
                  return null;
                }
                """
            )
        except Exception as exc:
            raise SinaScrapeError(symbol, trade_date, source, f"DOM 解析失败: {exc}")

    if not latest:
        raise SinaScrapeError(symbol, trade_date, source, "无法获取日线数据")

    day_value = latest.get("day") or latest.get("date") or ""
    if day_value and day_value != trade_date:
        raise SinaMissingError(symbol, trade_date, source, f"日期不匹配: {day_value}")

    updated_at = datetime.utcnow().isoformat()
    return DailyBar(
        symbol=symbol,
        trade_date=trade_date,
        open=_to_float(latest.get("open")),
        high=_to_float(latest.get("high")),
        low=_to_float(latest.get("low")),
        close=_to_float(latest.get("close")),
        change=0.0,
        change_pct=0.0,
        volume=_to_int(latest.get("volume")),
        amplitude_pct=0.0,
        turnover_pct=0.0,
        amount=_to_float(latest.get("amount")) if latest.get("amount") is not None else None,
        price_type="raw",
        source=source,
        updated_at=updated_at,
    )
