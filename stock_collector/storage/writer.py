import sqlite3
from contextlib import contextmanager
from pathlib import Path

from stock_collector.storage.schema import CollectStatus, DailyBar
from stock_collector.storage.sqlite_store import DEFAULT_DB_PATH, init_db, upsert_collect_status, upsert_daily_bar


# 打开数据库连接的上下文管理器
@contextmanager
def open_db(db_path: str = DEFAULT_DB_PATH):
    # 初始化数据库结构
    init_db(db_path)
    # 确保数据库目录存在
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    # 建立连接
    conn = sqlite3.connect(db_path)
    try:
        # 将连接交给调用方
        yield conn
        # 提交事务
        conn.commit()
    finally:
        # 关闭连接
        conn.close()


# 写入或更新日线行情数据
def write_daily_bar(conn: sqlite3.Connection, bar: DailyBar) -> None:
    # 使用 upsert 方式写入
    upsert_daily_bar(conn, bar)


# 写入或更新采集状态
def write_status(conn: sqlite3.Connection, status: CollectStatus) -> None:
    # 使用 upsert 方式写入
    upsert_collect_status(conn, status)
