from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


# 默认应用配置文件路径
DEFAULT_APP_CONFIG_PATH = Path(__file__).with_name("app.yaml")


# 加载应用配置（带缓存）
@lru_cache
def load_app_config(config_path: str | None = None) -> dict[str, Any]:
    # 解析配置文件路径
    path = Path(config_path) if config_path else DEFAULT_APP_CONFIG_PATH
    # 读取 YAML 配置
    with path.open("r", encoding="utf-8") as file_handle:
        return yaml.safe_load(file_handle) or {}


# 从配置中获取路径
def get_path(key: str) -> Path:
    # 读取配置
    config = load_app_config()
    value = config.get("paths", {}).get(key)
    if not value:
        raise KeyError(f"Missing paths.{key} in app config")
    # 返回路径对象
    return Path(value)


# 从配置中获取 URL
def get_url(key: str) -> str:
    # 读取配置
    config = load_app_config()
    value = config.get("urls", {}).get(key)
    if not value:
        raise KeyError(f"Missing urls.{key} in app config")
    # 返回 URL 字符串
    return value
