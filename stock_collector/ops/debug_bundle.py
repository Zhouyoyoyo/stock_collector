from __future__ import annotations

import json
import os
import platform
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional

DEBUG_DIR = Path("stock_collector/data/debug_bundle")


@dataclass
class DebugBundle:
    target_date: str
    stage: str
    is_trading_day: Optional[bool]
    total_symbols: int
    success_count: int
    missing_count: int
    failed_count: int
    first_error: Optional[str]
    note: str
    env: Dict[str, Any]


def safe_env_snapshot() -> Dict[str, Any]:
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


def write_bundle(b: DebugBundle) -> Path:
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    path = DEBUG_DIR / f"{b.target_date}.{b.stage}.json"
    payload = asdict(b)
    payload["env"] = b.env
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
