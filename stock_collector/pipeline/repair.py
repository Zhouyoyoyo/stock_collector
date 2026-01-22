from stock_collector.storage.sqlite_store import DEFAULT_DB_PATH, fetch_statuses, init_db
from stock_collector.storage.writer import open_db


# 根据汇总信息生成补采计划
def plan_repairs_from_summary(summary: dict) -> list[str]:
    # 读取交易日期
    trade_date = summary.get("date")
    if not trade_date:
        return []

    # 初始化数据库并读取状态
    init_db(DEFAULT_DB_PATH)
    with open_db(DEFAULT_DB_PATH) as conn:
        current_status = fetch_statuses(conn, trade_date)

    # 仅挑选需要修复的状态
    eligible = {"missing", "failed", "api_failed"}
    return [symbol for symbol, status in current_status.items() if status.status in eligible]
