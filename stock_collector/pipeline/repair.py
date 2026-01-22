from stock_collector.storage.sqlite_store import DEFAULT_DB_PATH, fetch_statuses, init_db
from stock_collector.storage.writer import open_db


def plan_repairs_from_summary(summary: dict) -> list[str]:
    trade_date = summary.get("date")
    if not trade_date:
        return []

    init_db(DEFAULT_DB_PATH)
    with open_db(DEFAULT_DB_PATH) as conn:
        current_status = fetch_statuses(conn, trade_date)

    eligible = {"missing", "failed", "api_failed"}
    return [symbol for symbol, status in current_status.items() if status.status in eligible]
