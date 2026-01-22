from __future__ import annotations

import json
import os
import platform
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional

# 调试信息输出目录
DEBUG_DIR = Path("stock_collector/data/debug_bundle")


# 调试包数据结构
@dataclass
class DebugBundle:
    # 目标日期
    target_date: str
    # 阶段标识
    stage: str
    # 是否交易日
    is_trading_day: Optional[bool]
    # 总股票数
    total_symbols: int
    # 成功数量
    success_count: int
    # 缺失数量
    missing_count: int
    # 失败数量
    failed_count: int
    # 首个错误信息
    first_error: Optional[str]
    # 备注信息
    note: str
    # 环境快照
    env: Dict[str, Any]


# 获取安全的环境快照
def safe_env_snapshot() -> Dict[str, Any]:
    # 仅记录关键环境变量
    keys = [
        "GITHUB_ACTIONS", "GITHUB_RUN_ID", "GITHUB_REF", "GITHUB_SHA",
        "PYTHON_VERSION",
    ]
    return {
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cwd": os.getcwd(),
        "env_keys": {k: ("SET" if os.getenv(k) else "MISSING") for k in keys},
    }


# 写入调试包到文件
def write_bundle(b: DebugBundle) -> Path:
    # 确保输出目录存在
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    path = DEBUG_DIR / f"{b.target_date}.{b.stage}.json"
    # 序列化数据
    payload = asdict(b)
    payload["env"] = b.env
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
