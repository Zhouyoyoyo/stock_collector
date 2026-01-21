import asyncio
import json
import os
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pytz
import yaml

from stock_collector.meta.universe import load_universe
from stock_collector.ops import alerting, backup, notifier_email, report
from stock_collector.ops.notifier_email import send_sms_via_email_once_per_day
from stock_collector.pipeline import validator
from stock_collector.pipeline.validator import MissingBarError
from stock_collector.pipeline.trading_calendar import is_calendar_trading_day
from stock_collector.scraper.browser import create_browser
from stock_collector.scraper.sina_api import fetch_daily_bar_from_sina_api
from stock_collector.scraper.sina_dom import fetch_daily_bar_from_sina_dom
from stock_collector.storage.schema import CollectStatus, DailyBar
from stock_collector.storage.sqlite_store import DEFAULT_DB_PATH, fetch_statuses, init_db, now_iso
from stock_collector.storage.writer import open_db, write_daily_bar, write_status


SCHEDULE_CONFIG = "stock_collector/config/schedule.yaml"
SCRAPER_CONFIG = "stock_collector/config/scraper.yaml"
STOCKS_CONFIG = "stock_collector/config/stocks.yaml"
API_WORKERS = 16
DOM_WORKERS = 4


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


def _runner_name() -> str:
    return "github-actions" if os.getenv("GITHUB_ACTIONS") else "local"


def _build_daily_bar(raw: dict) -> DailyBar:
    return DailyBar(
        symbol=raw["symbol"],
        trade_date=raw["trade_date"],
        open=float(raw.get("open", 0.0)),
        high=float(raw.get("high", 0.0)),
        low=float(raw.get("low", 0.0)),
        close=float(raw.get("close", 0.0)),
        change=float(raw.get("change", 0.0)),
        change_pct=float(raw.get("change_pct", 0.0)),
        volume=int(raw.get("volume", 0)),
        amplitude_pct=float(raw.get("amplitude_pct", 0.0)),
        turnover_pct=float(raw.get("turnover_pct", 0.0)),
        amount=float(raw["amount"]) if raw.get("amount") is not None else None,
        price_type="raw",
        source=raw.get("source", "sina"),
        updated_at=now_iso(),
    )


async def _run_async(trade_date: str) -> int:
    """收盘后主流程入口。"""
    log = logging.getLogger(__name__)
    schedule = _load_yaml(SCHEDULE_CONFIG)
    stocks_config = _load_yaml(STOCKS_CONFIG)
    scraper_config = _load_yaml(SCRAPER_CONFIG)

    symbols = load_universe(stocks_config)
    init_db(DEFAULT_DB_PATH)

    start_time = time.time()
    errors: list[str] = []
    success_symbols: set[str] = set()
    failed_symbols: set[str] = set()
    missing_symbols: set[str] = set()
    skipped_symbols: set[str] = set()
    api_failed_symbols: list[str] = []
    retry_success = 0

    rate_limit = scraper_config.get("rate_limit", {})
    delay_ms = rate_limit.get("per_symbol_delay_ms", 200)
    jitter_ms = rate_limit.get("random_jitter_ms", 120)

    with open_db(DEFAULT_DB_PATH) as conn:
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

        def record_success(symbol: str, retry_count: int = 0, source: str = "api") -> None:
            success_symbols.add(symbol)
            record_status(symbol, "success", retry_count, "")

        def record_skipped(symbol: str, reason: str, retry_count: int = 0) -> None:
            skipped_symbols.add(symbol)
            record_status(symbol, "skipped", retry_count, reason)

        def record_api_failure(symbol: str, error: str) -> None:
            record_status(symbol, "api_failed", 0, error)
            errors.append(error)

        def record_failure(symbol: str, error: str) -> None:
            failed_symbols.add(symbol)
            record_status(symbol, "failed", 0, error)
            errors.append(error)

        def record_missing(symbol: str, reason: str) -> None:
            missing_symbols.add(symbol)
            record_status(symbol, "missing", 0, reason)

        def already_collected(symbol: str, date_value: str) -> bool:
            if symbol in success_symbols:
                return True
            cursor = conn.execute(
                "SELECT 1 FROM daily_bar WHERE symbol = ? AND trade_date = ? LIMIT 1",
                (symbol, date_value),
            )
            return cursor.fetchone() is not None

        def validate_bar(bar: DailyBar) -> None:
            if bar.trade_date != trade_date:
                raise MissingBarError(bar.symbol, trade_date, f"日期不匹配: {bar.trade_date}")
            validate_errors = validator.validate_bar(bar)
            if validate_errors:
                raise RuntimeError(";".join(validate_errors))

        def store_bar(bar: DailyBar) -> None:
            if already_collected(bar.symbol, bar.trade_date):
                return
            write_daily_bar(conn, bar)

        current_status = fetch_statuses(conn, trade_date)
        todo_symbols: list[str] = []
        for symbol in symbols:
            status = current_status.get(symbol)
            if status and status.status == "success":
                success_symbols.add(symbol)
                continue
            if already_collected(symbol, trade_date):
                record_success(symbol, source="existing")
                continue
            todo_symbols.append(symbol)

        log.info("todo_symbols=%s for %s", len(todo_symbols), trade_date)

        if not todo_symbols:
            duration_seconds = time.time() - start_time
            expected = len(symbols)
            summary = report.build_summary(
                date_value=trade_date,
                expected=expected,
                success=expected,
                failed=0,
                missing=0,
                skipped=len(skipped_symbols),
                retry_success=0,
                duration_seconds=duration_seconds,
                source="sina",
                runner=_runner_name(),
                human_required=False,
                level="INFO",
                errors=[],
            )
            summary["success_rate"] = 1.0 if expected else 0.0
            summary["same_symbol_missing_days"] = 0
            summary["human_required"] = False
            summary_path = get_path("summary_dir") / f"{trade_date}.json"
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            notifier_email.send_email(summary, sorted(missing_symbols))
            backup.create_backup_bundle(trade_date)
            backup.cleanup_backups()
            return 0

        api_missing_symbols: list[str] = []

        def _api_task(sym: str):
            return sym, fetch_daily_bar_from_sina_api(sym, trade_date)

        with ThreadPoolExecutor(max_workers=API_WORKERS) as ex:
            future_to_symbol: dict = {}
            for symbol in todo_symbols:
                future = ex.submit(_api_task, symbol)
                future_to_symbol[future] = symbol

            for fu in as_completed(future_to_symbol):
                symbol = future_to_symbol[fu]
                try:
                    _, raw_bar = fu.result()
                    bar = _build_daily_bar(raw_bar)
                    validate_bar(bar)
                    store_bar(bar)
                    record_success(symbol, source="api")
                except RuntimeError as exc:
                    if str(exc) == "API_MISSING":
                        record_missing(symbol, reason="api_missing")
                        api_missing_symbols.append(symbol)
                    else:
                        record_api_failure(symbol, str(exc))
                        api_failed_symbols.append(symbol)
                except Exception as exc:
                    record_api_failure(symbol, str(exc))
                    api_failed_symbols.append(symbol)

        if api_failed_symbols:
            browser = await create_browser()
            try:
                pages = [await browser.context.new_page() for _ in range(DOM_WORKERS)]

                for idx, symbol in enumerate(api_failed_symbols):
                    page = pages[idx % DOM_WORKERS]
                    try:
                        if already_collected(symbol, trade_date):
                            record_success(symbol, source="existing")
                            continue

                        raw_bar = await fetch_daily_bar_from_sina_dom(page, symbol)
                        bar = _build_daily_bar(raw_bar)
                        validate_bar(bar)
                        store_bar(bar)

                        record_success(symbol, source="dom")
                        retry_success += 1
                    except MissingBarError as exc:
                        record_missing(symbol, reason=str(exc))
                    except RuntimeError as exc:
                        if str(exc) == "STOCK_SUSPENDED":
                            record_skipped(symbol, reason="suspended")
                        else:
                            record_failure(symbol, str(exc))
                    except Exception as exc:
                        record_failure(symbol, str(exc))
                    await asyncio.sleep((delay_ms + random.randint(0, jitter_ms)) / 1000)
            finally:
                if "pages" in locals():
                    for p in pages:
                        await p.close()
                await browser.close()

    duration_seconds = time.time() - start_time
    expected = len(symbols)
    success = len(success_symbols)
    failed = len(failed_symbols)
    missing = len(missing_symbols)
    success_rate = success / expected if expected else 0.0

    initial_level = alerting.compute_level(success_rate, 0, schedule["thresholds"])
    summary = report.build_summary(
        date_value=trade_date,
        expected=expected,
        success=success,
        failed=failed,
        missing=missing,
        skipped=len(skipped_symbols),
        retry_success=retry_success,
        duration_seconds=duration_seconds,
        source="sina",
        runner=_runner_name(),
        human_required=False,
        level=initial_level,
        errors=errors,
    )
    summary["success_rate"] = success_rate
    summary["same_symbol_missing_days"] = 0

    human_required = alerting.compute_human_required(summary, schedule["human_required"])
    summary["human_required"] = human_required

    consecutive_error_days = alerting.get_consecutive_error_days()
    thresholds = schedule["thresholds"]
    level = initial_level
    if consecutive_error_days >= thresholds.get("critical_consecutive_error_days", 2):
        if initial_level in {"ERROR", "CRITICAL"}:
            level = "CRITICAL"

    # level 如有变化则更新
    if level != initial_level:
        summary["level"] = level

    # ✅ 关键修复：无条件把最终 summary 写回磁盘（覆盖 report.build_summary 的初始文件）
    summary_path = get_path("summary_dir") / f"{trade_date}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    notifier_email.send_email(summary, sorted(missing_symbols))
    if summary.get("level") == "CRITICAL":
        sms_text = (
            "A股采集 CRITICAL\n"
            f"{summary.get('date')} "
            f"{summary.get('success')}/{summary.get('expected')}\n"
            f"missing={summary.get('missing')}"
        )
        send_sms_via_email_once_per_day(sms_text)
    backup.create_backup_bundle(trade_date)
    backup.cleanup_backups()

    if level in {"ERROR", "CRITICAL"}:
        return 2
    return 0


def should_collect(date_value: str) -> bool:
    if not is_calendar_trading_day(date_value):
        return False

    summary = report.load_summary(date_value)
    if summary is None:
        return True

    if summary.get("expected") == 0 and summary.get("success") == 0 and summary.get("failed") == 0 and summary.get("missing") == 0:
        return True

    if summary.get("level") == "CRITICAL":
        return True

    if summary.get("failed", 0) > 0:
        return True

    if summary.get("missing", 0) > 0:
        return True

    if summary.get("success", 0) < summary.get("expected", 0):
        return True

    return False


def run_collection(target_date: str) -> int:
    return asyncio.run(_run_async(target_date))


def run_after_close(target_date: str) -> int:
    log = logging.getLogger(__name__)
    schedule = _load_yaml(SCHEDULE_CONFIG)
    market_tz = pytz.timezone(schedule["timezone_market"])
    now_market = datetime.now(market_tz)

    if not _within_window(now_market, schedule):
        log.info("[SKIP] %s outside run window", target_date)
        return 0

    if not should_collect(target_date):
        log.info("[SKIP] %s no collection needed", target_date)
        return 0

    first_code = run_collection(target_date)
    summary = report.load_summary(target_date)
    if summary is None:
        log.warning("summary missing after first run: %s", target_date)
        return 2

    log.info(
        "first run summary for %s: success=%s failed=%s missing=%s",
        target_date,
        summary.get("success", 0),
        summary.get("failed", 0),
        summary.get("missing", 0),
    )

    if summary.get("missing", 0) > 0 or summary.get("failed", 0) > 0:
        log.info("triggering second run for %s to repair missing/failed", target_date)
        second_code = run_collection(target_date)
        second_summary = report.load_summary(target_date)
        if second_summary is None:
            log.warning("summary missing after second run: %s", target_date)
            return 2
        log.info(
            "second run summary for %s: success=%s failed=%s missing=%s",
            target_date,
            second_summary.get("success", 0),
            second_summary.get("failed", 0),
            second_summary.get("missing", 0),
        )
        return second_code

    return first_code


def run() -> int:
    schedule = _load_yaml(SCHEDULE_CONFIG)
    market_tz = pytz.timezone(schedule["timezone_market"])
    target_date = datetime.now(market_tz).strftime("%Y-%m-%d")
    return run_after_close(target_date)
from stock_collector.config.settings import get_path
