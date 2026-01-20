import json
import os
import random
import time
from datetime import datetime
from pathlib import Path

import pytz
import yaml

from stock_collector.meta.universe import load_universe
from stock_collector.ops import alerting, backup, notifier_email, report
from stock_collector.pipeline import trading_calendar, validator
from stock_collector.scraper.browser import create_browser
from stock_collector.scraper.sina_daily import SinaMissingError, SinaScrapeError, fetch_daily_bar
from stock_collector.storage.schema import CollectStatus
from stock_collector.storage.sqlite_store import DEFAULT_DB_PATH, fetch_statuses, init_db, now_iso
from stock_collector.storage.writer import open_db, write_daily_bar, write_status


SCHEDULE_CONFIG = "stock_collector/config/schedule.yaml"
SCRAPER_CONFIG = "stock_collector/config/scraper.yaml"
STOCKS_CONFIG = "stock_collector/config/stocks.yaml"


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file_handle:
        return yaml.safe_load(file_handle)


def _within_window(now_market: datetime, schedule: dict) -> bool:
    start_hm = schedule["run_not_before_hm"]
    end_hm = schedule["run_not_after_hm"]
    start = now_market.replace(
        hour=int(start_hm.split(":")[0]),
        minute=int(start_hm.split(":")[1]),
        second=0,
        microsecond=0,
    )
    end = now_market.replace(
        hour=int(end_hm.split(":")[0]),
        minute=int(end_hm.split(":")[1]),
        second=0,
        microsecond=0,
    )
    return start <= now_market <= end


def _write_skipped_summary(trade_date: str, reason: str) -> dict:
    summary = report.build_summary(
        date_value=trade_date,
        expected=0,
        success=0,
        failed=0,
        missing=0,
        retry_success=0,
        duration_seconds=0.0,
        source="sina",
        runner=_runner_name(),
        human_required=False,
        level="INFO",
        errors=[reason],
    )
    summary["success_rate"] = 0.0
    summary["skipped_reason"] = reason
    summary["same_symbol_missing_days"] = 0
    summary_path = Path("stock_collector/data/summary") / f"{trade_date}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _runner_name() -> str:
    return "github-actions" if os.getenv("GITHUB_ACTIONS") else "local"


def run() -> int:
    """收盘后主流程入口。"""
    schedule = _load_yaml(SCHEDULE_CONFIG)
    stocks_config = _load_yaml(STOCKS_CONFIG)
    scraper_config = _load_yaml(SCRAPER_CONFIG)

    market_tz = pytz.timezone(schedule["timezone_market"])
    now_market = datetime.now(market_tz)
    trade_date = now_market.strftime("%Y-%m-%d")

    if not _within_window(now_market, schedule):
        summary = _write_skipped_summary(trade_date, "未到执行窗口")
        notifier_email.send_email(summary, [])
        backup.create_backup_bundle(trade_date)
        return 0

    if not trading_calendar.is_trading_day(now_market.date()):
        summary = _write_skipped_summary(trade_date, "非交易日")
        notifier_email.send_email(summary, [])
        backup.create_backup_bundle(trade_date)
        return 0

    symbols = load_universe(stocks_config)
    init_db(DEFAULT_DB_PATH)

    start_time = time.time()
    errors: list[str] = []
    success_symbols: set[str] = set()
    failed_symbols: set[str] = set()
    missing_symbols: set[str] = set()
    retry_success = 0

    rate_limit = scraper_config.get("rate_limit", {})
    delay_ms = rate_limit.get("per_symbol_delay_ms", 200)
    jitter_ms = rate_limit.get("random_jitter_ms", 120)

    browser = create_browser()
    try:
        with open_db(DEFAULT_DB_PATH) as conn:
            current_status = fetch_statuses(conn, trade_date)

            def record_status(symbol: str, status: str, retry_count: int, last_error: str = "") -> None:
                status_obj = CollectStatus(
                    trade_date=trade_date,
                    symbol=symbol,
                    status=status,
                    retry_count=retry_count,
                    last_error=last_error,
                    updated_at=now_iso(),
                )
                write_status(conn, status_obj)

            for symbol in symbols:
                try:
                    page = browser.new_page()
                    bar = fetch_daily_bar(page, symbol, trade_date)
                    page.close()
                    validate_errors = validator.validate_bar(bar)
                    if validate_errors:
                        message = ";".join(validate_errors)
                        error_text = f"{symbol} {trade_date} source=sina {message}"
                        errors.append(error_text)
                        failed_symbols.add(symbol)
                        record_status(symbol, "failed", 0, error_text)
                        continue
                    write_daily_bar(conn, bar)
                    success_symbols.add(symbol)
                    record_status(symbol, "success", 0, "")
                except SinaMissingError as exc:
                    missing_symbols.add(symbol)
                    record_status(symbol, "missing", 0, str(exc))
                    errors.append(str(exc))
                except SinaScrapeError as exc:
                    failed_symbols.add(symbol)
                    record_status(symbol, "failed", 0, str(exc))
                    errors.append(str(exc))
                except Exception as exc:
                    failed_symbols.add(symbol)
                    error_text = f"{symbol} {trade_date} source=sina 未知异常: {exc}"
                    record_status(symbol, "failed", 0, error_text)
                    errors.append(error_text)
                time.sleep((delay_ms + random.randint(0, jitter_ms)) / 1000)

            rounds = schedule.get("retry", {}).get("rounds", 3)
            backoffs = schedule.get("retry", {}).get("backoff_seconds", [2, 5, 10])
            for round_index in range(1, rounds):
                if not failed_symbols:
                    break
                time.sleep(backoffs[min(round_index - 1, len(backoffs) - 1)])
                retry_targets = list(failed_symbols)
                failed_symbols.clear()
                for symbol in retry_targets:
                    try:
                        page = browser.new_page()
                        bar = fetch_daily_bar(page, symbol, trade_date)
                        page.close()
                        validate_errors = validator.validate_bar(bar)
                        if validate_errors:
                            message = ";".join(validate_errors)
                            error_text = f"{symbol} {trade_date} source=sina {message}"
                            errors.append(error_text)
                            failed_symbols.add(symbol)
                            record_status(symbol, "failed", round_index, error_text)
                            continue
                        write_daily_bar(conn, bar)
                        success_symbols.add(symbol)
                        retry_success += 1
                        record_status(symbol, "success", round_index, "")
                    except SinaMissingError as exc:
                        missing_symbols.add(symbol)
                        record_status(symbol, "missing", round_index, str(exc))
                        errors.append(str(exc))
                    except SinaScrapeError as exc:
                        failed_symbols.add(symbol)
                        record_status(symbol, "failed", round_index, str(exc))
                        errors.append(str(exc))
                    except Exception as exc:
                        failed_symbols.add(symbol)
                        error_text = f"{symbol} {trade_date} source=sina 未知异常: {exc}"
                        record_status(symbol, "failed", round_index, error_text)
                        errors.append(error_text)
                    time.sleep((delay_ms + random.randint(0, jitter_ms)) / 1000)

    finally:
        browser.close()

    duration_seconds = time.time() - start_time
    expected = len(symbols)
    success = len(success_symbols)
    failed = len(failed_symbols)
    missing = len(missing_symbols)
    success_rate = success / expected if expected else 0.0

    consecutive_error_days = alerting.get_consecutive_error_days()
    level = alerting.compute_level(success_rate, consecutive_error_days, schedule["thresholds"])

    summary = report.build_summary(
        date_value=trade_date,
        expected=expected,
        success=success,
        failed=failed,
        missing=missing,
        retry_success=retry_success,
        duration_seconds=duration_seconds,
        source="sina",
        runner=_runner_name(),
        human_required=False,
        level=level,
        errors=errors,
    )
    summary["success_rate"] = success_rate
    summary["same_symbol_missing_days"] = 0

    human_required = alerting.compute_human_required(summary, schedule["human_required"])
    summary["human_required"] = human_required
    summary_path = Path("stock_collector/data/summary") / f"{trade_date}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    notifier_email.send_email(summary, sorted(missing_symbols))
    backup.create_backup_bundle(trade_date)
    backup.cleanup_backups()

    if level in {"ERROR", "CRITICAL"}:
        return 2
    return 0
