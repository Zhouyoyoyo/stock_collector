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
        "success": success,
        "failed": failed,
        "missing": missing,
        "retry_success": retry_success,
        "duration_seconds": duration_seconds,
        "source": source,
        "runner": runner,
        "human_required": human_required,
        "level": level,
        "top_errors": [{"error": msg, "count": count} for msg, count in top_errors],
        "generated_at": datetime.utcnow().isoformat(),
    }
    summary_path = SUMMARY_DIR / f"{date_value}.json"
    with summary_path.open("w", encoding="utf-8") as file_handle:
        json.dump(summary, file_handle, ensure_ascii=False, indent=2)
    return summary
