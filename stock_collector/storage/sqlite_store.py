import sqlite3
from datetime import datetime
from pathlib import Path

from stock_collector.storage.schema import CollectStatus, DailyBar

DEFAULT_DB_PATH = "stock_collector/data/stock_daily.db"


def _ensure_daily_bar_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(daily_bar)")
    existing = {row[1] for row in cursor.fetchall()}
    additions = {
        "change": "REAL NOT NULL DEFAULT 0",
        "change_pct": "REAL NOT NULL DEFAULT 0",
        "amplitude_pct": "REAL NOT NULL DEFAULT 0",
        "turnover_pct": "REAL NOT NULL DEFAULT 0",
    }
    for column, definition in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE daily_bar ADD COLUMN {column} {definition}")
    conn.commit()


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """初始化数据库表结构。"""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_bar (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                change REAL NOT NULL,
                change_pct REAL NOT NULL,
                volume INTEGER NOT NULL,
                amplitude_pct REAL NOT NULL,
                turnover_pct REAL NOT NULL,
                amount REAL,
                price_type TEXT NOT NULL,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, trade_date)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_collect_status (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                status TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, trade_date)
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_bar_trade_date
            ON daily_bar (trade_date)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_collect_status_trade_date
            ON daily_collect_status (trade_date)
            """
        )
        conn.commit()
        _ensure_daily_bar_columns(conn)


def upsert_daily_bar(conn: sqlite3.Connection, bar: DailyBar) -> None:
    """幂等写入日线数据。"""
    conn.execute(
        """
        INSERT INTO daily_bar (
            symbol, trade_date, open, high, low, close,
            change, change_pct, volume, amplitude_pct, turnover_pct,
            amount, price_type, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, trade_date) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            change=excluded.change,
            change_pct=excluded.change_pct,
            volume=excluded.volume,
            amplitude_pct=excluded.amplitude_pct,
            turnover_pct=excluded.turnover_pct,
            amount=excluded.amount,
            price_type=excluded.price_type,
            source=excluded.source,
            updated_at=excluded.updated_at
        """,
        (
            bar.symbol,
            bar.trade_date,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.change,
            bar.change_pct,
            bar.volume,
            bar.amplitude_pct,
            bar.turnover_pct,
            bar.amount,
            bar.price_type,
            bar.source,
            bar.updated_at,
        ),
    )


def upsert_collect_status(conn: sqlite3.Connection, status: CollectStatus) -> None:
    """幂等写入采集状态。"""
    conn.execute(
        """
        INSERT INTO daily_collect_status (
            symbol, trade_date, status, retry_count, last_error, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, trade_date) DO UPDATE SET
            status=excluded.status,
            retry_count=excluded.retry_count,
            last_error=excluded.last_error,
            updated_at=excluded.updated_at
        """,
        (
            status.symbol,
            status.trade_date,
            status.status,
            status.retry_count,
            status.last_error,
            status.updated_at,
        ),
    )


def fetch_statuses(conn: sqlite3.Connection, trade_date: str) -> dict[str, CollectStatus]:
    """读取指定日期的状态。"""
    cursor = conn.execute(
        """
        SELECT symbol, trade_date, status, retry_count, last_error, updated_at
        FROM daily_collect_status
        WHERE trade_date = ?
        """,
        (trade_date,),
    )
    rows = cursor.fetchall()
    result: dict[str, CollectStatus] = {}
    for symbol, date_value, status, retry_count, last_error, updated_at in rows:
        result[symbol] = CollectStatus(
            trade_date=date_value,
            symbol=symbol,
            status=status,
            retry_count=retry_count,
            last_error=last_error or "",
            updated_at=updated_at,
        )
    return result


def now_iso() -> str:
    return datetime.utcnow().isoformat()
