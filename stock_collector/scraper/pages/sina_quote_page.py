import re
from datetime import datetime


class SinaQuotePage:
    """
    Page Object：新浪股票行情页
    负责：
    - 页面打开
    - DOM 定位
    - 数据解析
    """

    URL_TMPL = "https://finance.sina.com.cn/realstock/company/{symbol}/nc.shtml"

    def __init__(self, page):
        self.page = page

    async def open(self, symbol: str):
        url = self.URL_TMPL.format(symbol=symbol)
        await self.page.goto(url, wait_until="networkidle")

    # ---------- 基础工具 ----------

    @staticmethod
    def _clean(text: str) -> str:
        return (
            text.replace("\xa0", "")
            .replace("：", "")
            .replace(":", "")
            .strip()
        )

    @staticmethod
    def _parse_num(text: str) -> float:
        if not text:
            return 0.0
        t = text.strip()
        if "万" in t:
            return float(re.findall(r"[\d.]+", t)[0]) * 10000
        if "%" in t:
            return float(t.replace("%", ""))
        return float(re.findall(r"[-\d.]+", t)[0])

    # ---------- 状态判断 ----------

    async def is_suspended(self) -> bool:
        return await self.page.locator("#closed").count() > 0

    # ---------- 核心字段 ----------

    async def read_price_block(self) -> dict:
        close_price = float(self._clean(await self.page.locator("#price").inner_text()))
        change_val = self._parse_num(await self.page.locator("#change").inner_text())
        change_pct = self._parse_num(await self.page.locator("#changeP").inner_text())
        return {
            "close": close_price,
            "change": change_val,
            "change_pct": change_pct,
        }

    async def read_details_table(self) -> dict:
        """
        解析 hqDetails 表格，返回规范化后的 kv
        """
        kv = {}
        rows = self.page.locator("#hqDetails table tbody tr")
        rc = await rows.count()

        for i in range(rc):
            row = rows.nth(i)
            ths = row.locator("th")
            tds = row.locator("td")
            n = min(await ths.count(), await tds.count())
            for j in range(n):
                k = self._clean(await ths.nth(j).inner_text())
                v = self._clean(await tds.nth(j).inner_text())
                kv[k] = v
        return kv

    # ---------- 聚合输出 ----------

    async def to_daily_bar(self, symbol: str) -> dict:
        if await self.is_suspended():
            raise RuntimeError("STOCK_SUSPENDED")

        price = await self.read_price_block()
        kv = await self.read_details_table()

        trade_date = datetime.now().strftime("%Y-%m-%d")

        return {
            "symbol": symbol,
            "trade_date": trade_date,
            "open": self._parse_num(kv.get("开", "0")),
            "high": self._parse_num(kv.get("高", "0")),
            "low": self._parse_num(kv.get("低", "0")),
            "close": price["close"],
            "change": price["change"],
            "change_pct": price["change_pct"],
            "volume": int(self._parse_num(kv.get("成交量", "0"))),
            "amplitude_pct": self._parse_num(kv.get("振幅", "0")),
            "turnover_pct": self._parse_num(kv.get("换手率", "0")),
            "source": "sina_dom",
        }
