import pandas as pd


# 加载可交易 A 股股票代码列表
def load_tradeable_a_share_symbols(trade_date: str) -> list[str]:
    # 延迟导入 tushare 以避免无关环境问题
    import tushare as ts

    # 初始化 tushare 接口
    pro = ts.pro_api()

    # 获取交易日基础数据
    df = pro.daily_basic(
        trade_date=trade_date.replace("-", ""),
        fields="ts_code,trade_status",
    )

    # 筛选可交易股票
    df = df[df["trade_status"] == "交易"]
    # 提取股票代码列表
    symbols = df["ts_code"].astype(str).tolist()

    # 简单校验数量是否合理
    if len(symbols) < 4000:
        raise RuntimeError(
            f"A_SHARE_SYMBOLS_INCOMPLETE: only {len(symbols)} tradeable stocks on {trade_date}"
        )

    # 返回股票代码列表
    return symbols
