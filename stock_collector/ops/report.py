import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


SUMMARY_DIR = Path("stock_collector/data/summary")


def build_summary(
    date_value: str,
    expected: int,
    success: int,
    failed: int,
    missing: int,
    skipped: int,
    retry_success: int,
    duration_seconds: float,
    source: str,
    runner: str,
    human_required: bool,
    level: str,
    errors: list[str],
) -> dict[str, Any]:
    """生成 summary 并写入文件。"""
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    top_errors = Counter(errors).most_common(5)
    summary = {
        "date": date_value,
        "expected": expected,
        "total_symbols": expected,
        "success": success,
        "success_symbols": success,
        "failed": failed,
        "failed_symbols": failed,
        "missing": missing,
        "missing_symbols": missing,
        "skipped": skipped,
        "skipped_symbols": skipped,
        "retry_success": retry_success,
        "duration_seconds": duration_seconds,
        "source": source,
        "runner": runner,
        "human_required": human_required,
        "level": level,
        "top_errors": [{"error": msg, "count": count} for msg, count in top_errors],
        "generated_at": datetime.utcnow().isoformat(),
    }
    total = summary["total_symbols"]
    counted = (
        summary.get("success_symbols", 0)
        + summary.get("missing_symbols", 0)
        + summary.get("skipped_symbols", 0)
        + summary.get("failed_symbols", 0)
    )
    if counted != total:
        raise RuntimeError(f"Summary invariant broken: total={total}, counted={counted}")
    summary_path = SUMMARY_DIR / f"{date_value}.json"
    with summary_path.open("w", encoding="utf-8") as file_handle:
        json.dump(summary, file_handle, ensure_ascii=False, indent=2)
    return summary


def load_summary(date_value: str) -> dict[str, Any] | None:
    """
    只允许读取该 date 的 summary
    若不存在，返回 None
    严禁 fallback
    """
    summary_path = SUMMARY_DIR / f"{date_value}.json"
    if not summary_path.exists():
        return None
    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return None
