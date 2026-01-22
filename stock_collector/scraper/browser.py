from dataclasses import dataclass
from typing import Any

import yaml
from playwright.async_api import Browser, BrowserContext, Page, async_playwright


# 默认爬虫配置路径
SCRAPER_CONFIG_PATH = "stock_collector/config/scraper.yaml"


# 浏览器会话封装
@dataclass
class BrowserSession:
    # Playwright 运行时对象
    playwright: Any
    # 浏览器实例
    browser: Browser
    # 浏览器上下文
    context: BrowserContext

    # 新建页面
    async def new_page(self) -> Page:
        return await self.context.new_page()

    # 关闭浏览器会话
    async def close(self) -> None:
        await self.context.close()
        await self.browser.close()
        await self.playwright.stop()


# 创建并配置浏览器会话
async def create_browser(config_path: str = SCRAPER_CONFIG_PATH) -> BrowserSession:
    # 读取配置
    with open(config_path, "r", encoding="utf-8") as file_handle:
        config = yaml.safe_load(file_handle)
    browser_config = config["browser"]
    timeout_seconds = config.get("timeout", {}).get("page_load_seconds", 25)

    # 启动 Playwright
    playwright = await async_playwright().start()
    # 启动浏览器
    browser = await playwright.chromium.launch(headless=browser_config.get("headless", True))
    # 创建浏览器上下文
    context = await browser.new_context(
        viewport=browser_config.get("viewport"),
        user_agent=browser_config.get("user_agent"),
    )
    # 设置超时
    context.set_default_timeout(timeout_seconds * 1000)
    context.set_default_navigation_timeout(timeout_seconds * 1000)
    # 返回会话对象
    return BrowserSession(playwright=playwright, browser=browser, context=context)
