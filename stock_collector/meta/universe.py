import json
from pathlib import Path
from typing import Any
from urllib.request import urlopen

import yaml

from stock_collector.config.settings import get_url


# 默认股票池配置路径
DEFAULT_CONFIG_PATH = "stock_collector/config/stocks.yaml"


# 读取股票池配置文件
def _read_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    # 读取 YAML 配置
    with open(config_path, "r", encoding="utf-8") as file_handle:
        return yaml.safe_load(file_handle)


# 读取股票池列表（优先缓存）
def load_universe(config: dict[str, Any]) -> list[str]:
    # 缓存文件路径
    cache_path = Path(config["universe_cache"])
    if cache_path.exists():
        try:
            # 读取缓存内容
            with cache_path.open("r", encoding="utf-8") as file_handle:
                payload = json.load(file_handle)
            if isinstance(payload, list):
                symbols = payload
            else:
                symbols = payload.get("symbols", [])
            if symbols:
                return [symbol.upper() for symbol in symbols]
        except Exception as exc:
            print(f"[universe] 读取缓存失败，使用默认列表: {exc}")
    # 返回默认列表
    return [symbol.upper() for symbol in config.get("default_symbols", [])]


# 刷新股票池缓存
def refresh_universe_cache(config_path: str = DEFAULT_CONFIG_PATH) -> list[str]:
    # 读取配置并准备缓存目录
    config = _read_config(config_path)
    cache_path = Path(config["universe_cache"])
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    default_symbols = [symbol.upper() for symbol in config.get("default_symbols", [])]
    symbols: list[str] = []

    # 通过公开接口抓取股票池
    url = get_url("sina_stock_list")
    try:
        with urlopen(url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        data = payload.get("result", {}).get("data", [])
        for item in data:
            raw = item.get("symbol") or item.get("code") or ""
            if not raw:
                continue
            raw_upper = raw.upper()
            if raw_upper.startswith("SH") or raw_upper.startswith("SZ"):
                symbols.append(raw_upper)
        if not symbols:
            raise ValueError("抓取结果为空")
        print(f"[universe] 从公开接口抓取股票池: {len(symbols)}")
    except Exception as exc:
        # 抓取失败时回退到默认列表
        print(f"[universe] 抓取股票池失败，使用默认列表: {exc}")
        symbols = default_symbols

    # 写入缓存文件
    with cache_path.open("w", encoding="utf-8") as file_handle:
        json.dump({"symbols": symbols}, file_handle, ensure_ascii=False, indent=2)
    # 返回最终股票池
    return symbols
