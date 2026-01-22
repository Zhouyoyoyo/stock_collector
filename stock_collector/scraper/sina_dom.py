from stock_collector.scraper.pages.sina_quote_page import SinaQuotePage


# 使用 DOM 页面抓取日线行情
async def fetch_daily_bar_from_sina_dom(page, symbol: str) -> dict:
    # 初始化页面对象
    po = SinaQuotePage(page)
    # 打开目标股票页面
    await po.open(symbol)
    # 解析为日线数据
    return await po.to_daily_bar(symbol)
