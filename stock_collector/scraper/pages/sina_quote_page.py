import re
from datetime import datetime

from stock_collector.config.settings import get_url


# 新浪行情页面解析器
class SinaQuotePage:
    # 页面 URL 模板
    URL_TMPL = get_url("sina_quote_page")

    # 初始化页面对象
    def __init__(self, page):
        self.page = page

    # 打开行情页面
    async def open(self, symbol: str):
        url = self.URL_TMPL.format(symbol=symbol)
        await self.page.goto(url, wait_until="networkidle")

    # 清洗文本中的特殊字符
    @staticmethod
    def _clean(text: str) -> str:
        return (
            text.replace("\xa0", "")
            .replace("：", "")
            .replace(":", "")
            .strip()
        )

    # 解析字符串数值（包含中文单位）
    @staticmethod
    def _parse_num(text: str) -> float:
        if not text:
            return 0.0

        t = text.strip()

        # 处理“万手”单位
        if "万手" in t:
            num = float(re.findall(r"[\d.]+", t)[0])
            return num * 10000 * 100

        # 处理“手”单位
        if t.endswith("手"):
            num = float(re.findall(r"[\d.]+", t)[0])
            return num * 100

        # 处理百分比
        if "%" in t:
            return float(t.replace("%", ""))

        # 处理普通数值
        return float(re.findall(r"[-\d.]+", t)[0])

    # 判断是否停牌
    async def is_suspended(self) -> bool:
        return await self.page.locator("#closed").count() > 0

    # 读取价格块数据
    async def read_price_block(self) -> dict:
        close_price = float(self._clean(await self.page.locator("#price").inner_text()))
        change_val = self._parse_num(await self.page.locator("#change").inner_text())
        change_pct = self._parse_num(await self.page.locator("#changeP").inner_text())
        return {
            "close": close_price,
            "change": change_val,
            "change_pct": change_pct,
        }

    # 读取详情表格的键值对
    async def read_details_table(self) -> dict:
        kv = {}
        rows = self.page.locator("#hqDetails table tbody tr")
        rc = await rows.count()

        # 逐行解析表格
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

    # 汇总为日线数据字典
    async def to_daily_bar(self, symbol: str) -> dict:
        # 停牌直接抛错
        if await self.is_suspended():
            raise RuntimeError("STOCK_SUSPENDED")

        # 读取价格和详情信息
        price = await self.read_price_block()
        kv = await self.read_details_table()

        # 使用当前日期作为交易日
        trade_date = datetime.now().strftime("%Y-%m-%d")

        # 组装日线数据
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
