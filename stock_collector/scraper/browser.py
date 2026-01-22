from dataclasses import dataclass
from typing import Any

import yaml
from playwright.async_api import Browser, BrowserContext, Page, async_playwright


SCRAPER_CONFIG_PATH = "stock_collector/config/scraper.yaml"


@dataclass
class BrowserSession:
    playwright: Any
    browser: Browser
    context: BrowserContext

    async def new_page(self) -> Page:
        return await self.context.new_page()

    async def close(self) -> None:
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()


async def create_browser(config_path: str = SCRAPER_CONFIG_PATH) -> BrowserSession:
    with open(config_path, "r", encoding="utf-8") as file_handle:
        config = yaml.safe_load(file_handle)
    browser_config = config["browser"]
    timeout_seconds = config.get("timeout", {}).get("page_load_seconds", 25)

    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=browser_config.get("headless", True))
    context = await browser.new_context(
        viewport=browser_config.get("viewport"),
        user_agent=browser_config.get("user_agent"),
    )
    context.set_default_timeout(timeout_seconds * 1000)
    context.set_default_navigation_timeout(timeout_seconds * 1000)
    return BrowserSession(playwright=playwright, browser=browser, context=context)
