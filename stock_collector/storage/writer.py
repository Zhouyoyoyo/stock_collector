import sqlite3
from contextlib import contextmanager
from pathlib import Path

from stock_collector.storage.schema import CollectStatus, DailyBar
from stock_collector.storage.sqlite_store import DEFAULT_DB_PATH, init_db, upsert_collect_status, upsert_daily_bar


@contextmanager
def open_db(db_path: str = DEFAULT_DB_PATH):
    init_db(db_path)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def write_daily_bar(conn: sqlite3.Connection, bar: DailyBar) -> None:
    upsert_daily_bar(conn, bar)


def write_status(conn: sqlite3.Connection, status: CollectStatus) -> None:
    upsert_collect_status(conn, status)
