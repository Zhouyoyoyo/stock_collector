import json
from collections import Counter
from datetime import datetime
from typing import Any

from stock_collector.config.settings import get_path

# 汇总文件目录
SUMMARY_DIR = get_path("summary_dir")


# 构建并写入汇总信息
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
    # 确保输出目录存在
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    # 统计最常见错误
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
    # 校验统计一致性
    total = summary["total_symbols"]
    counted = (
        summary.get("success_symbols", 0)
        + summary.get("missing_symbols", 0)
        + summary.get("skipped_symbols", 0)
        + summary.get("failed_symbols", 0)
    )
    if counted != total:
        raise RuntimeError(f"Summary invariant broken: total={total}, counted={counted}")
    # 写入汇总文件
    summary_path = SUMMARY_DIR / f"{date_value}.json"
    with summary_path.open("w", encoding="utf-8") as file_handle:
        json.dump(summary, file_handle, ensure_ascii=False, indent=2)
    return summary


# 读取指定日期的汇总信息
def load_summary(date_value: str) -> dict[str, Any] | None:
    summary_path = SUMMARY_DIR / f"{date_value}.json"
    if not summary_path.exists():
        return None
    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return None
