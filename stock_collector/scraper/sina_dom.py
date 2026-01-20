from stock_collector.scraper.pages.sina_quote_page import SinaQuotePage


async def fetch_daily_bar_from_sina_dom(page, symbol: str) -> dict:
    po = SinaQuotePage(page)
    await po.open(symbol)
    return await po.to_daily_bar(symbol)
