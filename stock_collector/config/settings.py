from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


DEFAULT_APP_CONFIG_PATH = Path(__file__).with_name("app.yaml")


@lru_cache
def load_app_config(config_path: str | None = None) -> dict[str, Any]:
    path = Path(config_path) if config_path else DEFAULT_APP_CONFIG_PATH
    with path.open("r", encoding="utf-8") as file_handle:
        return yaml.safe_load(file_handle) or {}


def get_path(key: str) -> Path:
    config = load_app_config()
    value = config.get("paths", {}).get(key)
    if not value:
        raise KeyError(f"Missing paths.{key} in app config")
    return Path(value)


def get_url(key: str) -> str:
    config = load_app_config()
    value = config.get("urls", {}).get(key)
    if not value:
        raise KeyError(f"Missing urls.{key} in app config")
    return value
