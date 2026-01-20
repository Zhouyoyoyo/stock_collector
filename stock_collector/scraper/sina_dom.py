from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from playwright.async_api import Page

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


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    return value.replace("\u3000", " ").strip()


def _parse_number(value: str | None) -> float:
    if not value:
        return 0.0
    text = _normalize_text(value)
    if text in {"--", "-", ""}:
        return 0.0
    multiplier = 1.0
    if "亿" in text:
        multiplier = 1e8
        text = text.replace("亿", "")
    if "万" in text:
        multiplier = 1e4
        text = text.replace("万", "")
    text = text.replace(",", "").replace("%", "")
    text = text.replace("手", "").replace("股", "")
    try:
        return float(text) * multiplier
    except Exception:
        return 0.0


def _parse_int(value: str | None) -> int:
    return int(round(_parse_number(value)))


def _normalize_date(value: str | None) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    text = text.replace("/", "-")
    return text[:10]


def _pick_first(details: dict[str, str], keys: list[str]) -> str:
    for key in keys:
        if key in details and details[key]:
            return details[key]
    return ""


async def fetch_daily_bar_from_sina_dom(page: Page, symbol: str) -> DailyBar:
    """抓取新浪 DOM 行情数据。"""
    symbol_lower = symbol.lower()
    source = "sina"
    url = f"https://finance.sina.com.cn/realstock/company/{symbol_lower}/nc.shtml"

    try:
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_selector("#price")
    except Exception as exc:
        raise SinaScrapeError(symbol, "", source, f"页面加载失败: {exc}")

    try:
        price_text = _normalize_text(await page.locator("#price").inner_text())
        change_text = _normalize_text(await page.locator("#change").inner_text())
        change_pct_text = _normalize_text(await page.locator("#changeP").inner_text())
        details: dict[str, str] = await page.evaluate(
            """
            () => {
              const table = document.querySelector('#hqDetails table');
              if (!table) {
                return {};
              }
              const rows = Array.from(table.querySelectorAll('tr'));
              const result = {};
              for (const row of rows) {
                const cells = Array.from(row.querySelectorAll('th, td')).map(
                  cell => cell.textContent.trim()
                );
                for (let index = 0; index + 1 < cells.length; index += 2) {
                  const key = cells[index];
                  const value = cells[index + 1];
                  if (key) {
                    result[key] = value;
                  }
                }
              }
              return result;
            }
            """
        )
    except Exception as exc:
        raise SinaScrapeError(symbol, "", source, f"DOM 解析失败: {exc}")

    trade_date = _normalize_date(_pick_first(details, ["日期", "日期:"]))
    if not trade_date:
        trade_date = datetime.now().strftime("%Y-%m-%d")

    open_text = _pick_first(details, ["今开", "开盘"])
    high_text = _pick_first(details, ["最高", "最高价"])
    low_text = _pick_first(details, ["最低", "最低价"])
    close_text = price_text or _pick_first(details, ["收盘", "最新价"])

    open_price = _parse_number(open_text) or _parse_number(price_text)
    high_price = _parse_number(high_text) or open_price
    low_price = _parse_number(low_text) or open_price
    close_price = _parse_number(close_text) or open_price

    volume_value = _parse_int(_pick_first(details, ["成交量", "总成交量"]))
    amount_value = _parse_number(_pick_first(details, ["成交额", "总成交额"]))
    amplitude_pct = _parse_number(_pick_first(details, ["振幅"]))
    turnover_pct = _parse_number(_pick_first(details, ["换手率"]))

    return DailyBar(
        symbol=symbol,
        trade_date=trade_date,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        change=_parse_number(change_text),
        change_pct=_parse_number(change_pct_text),
        volume=volume_value,
        amplitude_pct=amplitude_pct,
        turnover_pct=turnover_pct,
        amount=amount_value if amount_value else None,
        price_type="raw",
        source=source,
        updated_at=datetime.utcnow().isoformat(),
    )
