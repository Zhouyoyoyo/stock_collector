import asyncio
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pytz
import yaml

from stock_collector.config.settings import get_path
from stock_collector.meta.universe import load_universe
from stock_collector.ops import alerting, backup, notifier_email, report
from stock_collector.ops.notifier_email import send_sms_via_email_once_per_day
from stock_collector.ops.debug_bundle import DebugBundle, safe_env_snapshot, write_bundle
from stock_collector.pipeline import validator
from stock_collector.pipeline.validator import MissingBarError
from stock_collector.pipeline.trading_calendar import is_calendar_trading_day
from stock_collector.scraper.browser import create_browser
from stock_collector.scraper.sina_api import fetch_daily_bar_from_sina_api
from stock_collector.scraper.sina_dom import fetch_daily_bar_from_sina_dom
from stock_collector.storage.schema import CollectStatus, DailyBar
from stock_collector.storage.csv_writer import write_summary_csv, write_symbol_csv
from stock_collector.storage.sqlite_store import DEFAULT_DB_PATH, fetch_statuses, init_db, now_iso
from stock_collector.storage.writer import open_db, write_daily_bar, write_status


SCHEDULE_CONFIG = "stock_collector/config/schedule.yaml"
SCRAPER_CONFIG = "stock_collector/config/scraper.yaml"
STOCKS_CONFIG = "stock_collector/config/stocks.yaml"
API_WORKERS = 16
DOM_WORKERS = 4
CSV_BASE_DIR = Path("stock_collector/data/csv")


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file_handle:
        return yaml.safe_load(file_handle)


def _runner_name() -> str:
    return "github-actions" if os.getenv("GITHUB_ACTIONS") else "local"


def _write_skip_summary(trade_date: str, reason: str) -> None:
    summary = report.build_summary(
        date_value=trade_date,
        expected=0,
        success=0,
        failed=0,
        missing=0,
        skipped=0,
        retry_success=0,
        duration_seconds=0.0,
        source="sina",
        runner=_runner_name(),
        human_required=False,
        level="INFO",
        errors=[],
    )
    summary["success_rate"] = 0.0
    summary["same_symbol_missing_days"] = 0
    summary["human_required"] = False
    summary["skip_reason"] = reason

    write_summary_csv(
        base_dir=CSV_BASE_DIR,
        trade_date=trade_date,
        summary_rows=summary.get("symbols", []),
    )
    summary_path = get_path("summary_dir") / f"{trade_date}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


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

    trade_date_str = trade_date
    is_trading_day = is_calendar_trading_day(trade_date)
    success_count = 0
    missing_count = 0
    failed_count = 0
    first_error = None  # dict or None
    write_bundle(DebugBundle(
        target_date=trade_date_str,
        stage="start",
        is_trading_day=is_trading_day,
        total_symbols=0,
        success_count=success_count,
        missing_count=missing_count,
        failed_count=failed_count,
        first_error=first_error,
        note="pipeline started",
        env=safe_env_snapshot(),
    ))

    symbols = load_universe(stocks_config)
    write_bundle(DebugBundle(
        target_date=trade_date_str,
        stage="after_symbols_loaded",
        is_trading_day=is_trading_day,
        total_symbols=len(symbols),
        success_count=success_count,
        missing_count=missing_count,
        failed_count=failed_count,
        first_error=first_error,
        note="symbols loaded",
        env=safe_env_snapshot(),
    ))
    write_bundle(DebugBundle(
        target_date=trade_date_str,
        stage="trading_day_checked",
        is_trading_day=is_trading_day,
        total_symbols=len(symbols),
        success_count=success_count,
        missing_count=missing_count,
        failed_count=failed_count,
        first_error=first_error,
        note="trading day decided",
        env=safe_env_snapshot(),
    ))
    init_db(DEFAULT_DB_PATH)

    start_time = time.time()
    errors: list[str] = []
    success_symbols: set[str] = set()
    failed_symbols: set[str] = set()
    missing_symbols: set[str] = set()
    skipped_symbols: set[str] = set()
    failed_event_symbols: set[str] = set()
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
            nonlocal success_count
            if symbol not in success_symbols:
                success_count += 1
            success_symbols.add(symbol)
            record_status(symbol, "success", retry_count, "")

        def record_skipped(symbol: str, reason: str, retry_count: int = 0) -> None:
            skipped_symbols.add(symbol)
            record_status(symbol, "skipped", retry_count, reason)
            write_symbol_csv(
                base_dir=CSV_BASE_DIR,
                trade_date=trade_date,
                symbol=symbol,
                rows=[],
            )

        def record_api_failure(symbol: str, error: str) -> None:
            nonlocal failed_count, first_error
            record_status(symbol, "api_failed", 0, error)
            errors.append(error)
            if symbol not in failed_event_symbols:
                failed_count += 1
                failed_event_symbols.add(symbol)
            if first_error is None:
                first_error = {
                    "type": "failed",
                    "symbol": symbol,
                    "exception": repr(error),
                }
            write_symbol_csv(
                base_dir=CSV_BASE_DIR,
                trade_date=trade_date,
                symbol=symbol,
                rows=[],
            )

        def record_failure(symbol: str, error: str) -> None:
            nonlocal failed_count, first_error
            if symbol not in failed_event_symbols:
                failed_count += 1
                failed_event_symbols.add(symbol)
            failed_symbols.add(symbol)
            record_status(symbol, "failed", 0, error)
            errors.append(error)
            if first_error is None:
                first_error = {
                    "type": "failed",
                    "symbol": symbol,
                    "exception": repr(error),
                }
            write_symbol_csv(
                base_dir=CSV_BASE_DIR,
                trade_date=trade_date,
                symbol=symbol,
                rows=[],
            )

        def record_missing(symbol: str, reason: str) -> None:
            nonlocal missing_count, first_error
            if symbol not in missing_symbols:
                missing_count += 1
            missing_symbols.add(symbol)
            record_status(symbol, "missing", 0, reason)
            if first_error is None:
                first_error = {
                    "type": "missing",
                    "symbol": symbol,
                    "exception": repr(reason),
                }
            write_symbol_csv(
                base_dir=CSV_BASE_DIR,
                trade_date=trade_date,
                symbol=symbol,
                rows=[],
            )

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
            write_symbol_csv(
                base_dir=CSV_BASE_DIR,
                trade_date=bar.trade_date,
                symbol=bar.symbol,
                rows=[
                    {
                        "trade_date": bar.trade_date,
                        "symbol": bar.symbol,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "amount": bar.amount,
                    }
                ],
            )

        current_status = fetch_statuses(conn, trade_date)
        todo_symbols: list[str] = []
        for symbol in symbols:
            status = current_status.get(symbol)
            if status and status.status == "success":
                if symbol not in success_symbols:
                    success_count += 1
                success_symbols.add(symbol)
                continue
            if already_collected(symbol, trade_date):
                record_success(symbol, source="existing")
                continue
            todo_symbols.append(symbol)

        log.info("todo_symbols=%s for %s", len(todo_symbols), trade_date)

        if not todo_symbols:
            write_bundle(DebugBundle(
                target_date=trade_date,
                stage="after_fetch",
                is_trading_day=is_trading_day,
                total_symbols=len(symbols),
                success_count=success_count,
                missing_count=missing_count,
                failed_count=failed_count,
                first_error=first_error,
                note="fetch finished",
                env=safe_env_snapshot(),
            ))
            if is_trading_day and success_count == 0:
                raise RuntimeError(
                    f"TRADING_DAY_NO_DATA: {trade_date} 是交易日，但未获取到任何行情数据"
                )
            if is_trading_day:
                if success_count != len(symbols) or missing_count != 0 or failed_count != 0:
                    write_bundle(DebugBundle(
                        target_date=trade_date_str,
                        stage="fatal",
                        is_trading_day=is_trading_day,
                        total_symbols=len(symbols),
                        success_count=success_count,
                        missing_count=missing_count,
                        failed_count=failed_count,
                        first_error=first_error,
                        note="FATAL: trading day requires full success with zero missing/failed",
                        env=safe_env_snapshot(),
                    ))
                    raise RuntimeError(
                        "FATAL: 交易日出现缺失/失败，违反业务公理。"
                        f" total={len(symbols)} success={success_count}"
                        f" missing={missing_count} failed={failed_count}"
                        f" first_error={first_error}"
                    )
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
            write_summary_csv(
                base_dir=CSV_BASE_DIR,
                trade_date=trade_date,
                summary_rows=summary.get("symbols", []),
            )
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

    write_bundle(DebugBundle(
        target_date=trade_date_str,
        stage="after_fetch",
        is_trading_day=is_trading_day,
        total_symbols=len(symbols),
        success_count=success_count,
        missing_count=missing_count,
        failed_count=failed_count,
        first_error=first_error,
        note="fetch finished",
        env=safe_env_snapshot(),
    ))
    if is_trading_day and success_count == 0:
        raise RuntimeError(
            f"TRADING_DAY_NO_DATA: {trade_date} 是交易日，但未获取到任何行情数据"
        )
    if is_trading_day:
        if success_count != len(symbols) or missing_count != 0 or failed_count != 0:
            write_bundle(DebugBundle(
                target_date=trade_date_str,
                stage="fatal",
                is_trading_day=is_trading_day,
                total_symbols=len(symbols),
                success_count=success_count,
                missing_count=missing_count,
                failed_count=failed_count,
                first_error=first_error,
                note="FATAL: trading day requires full success with zero missing/failed",
                env=safe_env_snapshot(),
            ))
            raise RuntimeError(
                "FATAL: 交易日出现缺失/失败，违反业务公理。"
                f" total={len(symbols)} success={success_count}"
                f" missing={missing_count} failed={failed_count}"
                f" first_error={first_error}"
            )

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
    write_summary_csv(
        base_dir=CSV_BASE_DIR,
        trade_date=trade_date,
        summary_rows=summary.get("symbols", []),
    )

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
    # ✅ 无论如何先创建目录，保证 artifact path 一定存在
    CSV_BASE_DIR.mkdir(parents=True, exist_ok=True)
    get_path("summary_dir").mkdir(parents=True, exist_ok=True)

    try:
        return asyncio.run(_run_async(target_date))
    except Exception as e:
        # ✅ 写 debug bundle，保证你能下载到崩溃证据
        write_bundle(DebugBundle(
            target_date=target_date,
            stage="exception",
            is_trading_day=None,
            total_symbols=0,
            success_count=0,
            missing_count=0,
            failed_count=0,
            first_error={"type": "exception", "exception": repr(e)},
            note="pipeline crashed before producing summary",
            env=safe_env_snapshot(),
        ))

        # ✅ 写一个 CRITICAL summary.json，避免“runner 成功但无数据/无 summary”
        _write_skip_summary(target_date, reason=f"exception:{type(e).__name__}:{e}")
        raise


def run_after_close(target_date: str) -> int:
    """
    规则（不可变）：
    - 非交易日：允许 skip
    - 交易日：必须采集
      - 拿到数据：success
      - 拿不到数据：fail
    """
    if not is_calendar_trading_day(target_date):
        _write_skip_summary(
            target_date,
            reason="non_trading_day",
        )
        return 0

    return run_collection(target_date)


def run() -> int:
    schedule = _load_yaml(SCHEDULE_CONFIG)
    market_tz = pytz.timezone(schedule["timezone_market"])
    target_date = datetime.now(market_tz).strftime("%Y-%m-%d")
    return run_after_close(target_date)
