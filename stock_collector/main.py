import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from stock_collector.meta.universe import refresh_universe_cache
from stock_collector.pipeline.run_after_close import run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A 股日线采集器")
    parser.add_argument("--run", action="store_true", help="执行当日采集")
    parser.add_argument("--refresh-universe", action="store_true", help="刷新股票池缓存")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.refresh_universe:
        refresh_universe_cache()
        return 0
    return run()


if __name__ == "__main__":
    sys.exit(main())
