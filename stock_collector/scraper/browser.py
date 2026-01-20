from dataclasses import dataclass
from typing import Any

import yaml
from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright


SCRAPER_CONFIG_PATH = "stock_collector/config/scraper.yaml"


@dataclass
class BrowserSession:
    playwright: Any
    browser: Browser
    context: BrowserContext

    def new_page(self) -> Page:
        return self.context.new_page()

    def close(self) -> None:
        self.context.close()
        self.browser.close()
        self.playwright.stop()


def create_browser(config_path: str = SCRAPER_CONFIG_PATH) -> BrowserSession:
    """创建 Playwright 浏览器实例。"""
    with open(config_path, "r", encoding="utf-8") as file_handle:
        config = yaml.safe_load(file_handle)
    browser_config = config["browser"]
    timeout_seconds = config.get("timeout", {}).get("page_load_seconds", 25)

    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=browser_config.get("headless", True))
    context = browser.new_context(
        viewport=browser_config.get("viewport"),
        user_agent=browser_config.get("user_agent"),
    )
    context.set_default_timeout(timeout_seconds * 1000)
    context.set_default_navigation_timeout(timeout_seconds * 1000)
    return BrowserSession(playwright=playwright, browser=browser, context=context)
