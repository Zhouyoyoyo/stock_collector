import json
from pathlib import Path
from typing import Any


SUMMARY_DIR = Path("stock_collector/data/summary")


def compute_level(success_rate: float, consecutive_error_days: int, thresholds: dict[str, Any]) -> str:
    """根据成功率和连续错误天数计算告警级别。"""
    if consecutive_error_days >= thresholds.get("critical_consecutive_error_days", 2):
        return "CRITICAL"
    if success_rate < thresholds.get("warn_success_rate", 0.95):
        return "ERROR"
    if success_rate < thresholds.get("info_success_rate", 0.98):
        return "WARN"
    return "INFO"


def _read_summary_level(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8") as file_handle:
            return json.load(file_handle).get("level", "INFO")
    except Exception:
        return "INFO"


def get_consecutive_error_days() -> int:
    """读取历史 summary 计算连续 ERROR/CRITICAL 天数。"""
    if not SUMMARY_DIR.exists():
        return 0
    files = sorted(SUMMARY_DIR.glob("*.json"), key=lambda path: path.stem, reverse=True)
    count = 0
    for file_path in files:
        level = _read_summary_level(file_path)
        if level in {"ERROR", "CRITICAL"}:
            count += 1
        else:
            break
    return count


def compute_human_required(summary: dict[str, Any], rules: dict[str, Any]) -> bool:
    """判断是否需要人工介入。"""
    failed_over = rules.get("failed_over", 0)
    missing_over = rules.get("missing_over", 0)
    same_symbol_missing_days = rules.get("same_symbol_missing_days", 0)

    if summary.get("failed", 0) >= failed_over:
        return True
    if summary.get("missing", 0) >= missing_over:
        return True
    if summary.get("same_symbol_missing_days", 0) >= same_symbol_missing_days:
        return True
    return False
