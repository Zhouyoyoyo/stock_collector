import argparse
import sys
from pathlib import Path

# 计算项目根目录路径，确保本地模块可导入
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    # 将根目录插入到模块搜索路径
    sys.path.insert(0, str(ROOT_DIR))

from stock_collector.meta.universe import refresh_universe_cache
from stock_collector.pipeline.run_after_close import run


# 解析命令行参数
def parse_args() -> argparse.Namespace:
    # 初始化参数解析器
    parser = argparse.ArgumentParser(description="A 股日线采集器")
    # 增加执行采集的参数
    parser.add_argument("--run", action="store_true", help="执行当日采集")
    # 增加刷新股票池的参数
    parser.add_argument("--refresh-universe", action="store_true", help="刷新股票池缓存")
    # 返回解析后的参数
    return parser.parse_args()


# 主入口逻辑
def main() -> int:
    # 读取命令行参数
    args = parse_args()
    # 根据参数选择执行逻辑
    if args.refresh_universe:
        # 刷新股票池缓存
        refresh_universe_cache()
        return 0
    # 执行采集流程
    return run()


# 作为脚本执行时的入口
if __name__ == "__main__":
    # 返回进程退出码
    sys.exit(main())
