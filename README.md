# Stock Collector（A 股收盘后日线采集）

## 目标

收盘后自动抓取 A 股全量日线 OHLCV（当前数据源：新浪），幂等落库 SQLite，生成 summary、告警、邮件通知，并在 GitHub Actions 定时跑且通过 Artifacts 备份。

## 一键运行（本地）

```bash
pip install -r stock_collector/requirements.txt
python -m playwright install --with-deps chromium
python stock_collector/main.py --run
```

## 配置说明

所有配置均在 `stock_collector/config/` 下：

- `schedule.yaml`：北京时间收盘后的调度窗口、重试轮数、阈值、人介入规则。
- `scraper.yaml`：浏览器参数、限速、超时。
- `stocks.yaml`：默认股票池、缓存文件路径、过滤规则。
- `notify.yaml`：邮件通知配置（仅使用环境变量）。

调度逻辑以北京时间（Asia/Shanghai）为准，默认运行环境时区为 Europe/Berlin。

### 环境变量（邮件通知）

必须通过环境变量或 GitHub Secrets 注入：

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASS`
- `ALERT_EMAIL_TO`
- `ALERT_EMAIL_FROM`

## GitHub Actions

已提供 `daily_collect` workflow，默认北京时间 15:10（UTC 07:10）触发，含手动触发。

需要在仓库 Secrets 配置 SMTP 相关变量，并在 workflow 中映射为运行时环境变量（例如 `SMTP_USERNAME`、`SMTP_PASSWORD`、`SMTP_FROM`、`SMTP_TO`）。Commit messages should not include secrets. Artifacts 会上传：

- `stock_collector/data/stock_daily.db`
- `stock_collector/data/summary/*.json`
- `stock_collector/data/backup/**`

## 数据结构说明

SQLite 表：

- `daily_bar`
  - `symbol`、`trade_date` 为主键
  - `open/high/low/close/volume/amount` 等字段
- `daily_collect_status`
  - `symbol`、`trade_date` 为主键
  - `status`、`retry_count`、`last_error` 等字段

## 失败重试、告警等级、人介入规则

- 失败重试轮数与退避秒数由 `schedule.yaml` 的 `retry` 配置。
- 成功率阈值由 `thresholds` 控制：INFO/WARN/ERROR。
- 连续 ERROR 天数达到阈值触发 CRITICAL。
- 人介入规则由 `human_required` 控制（失败数量/缺失数量/连续缺失天数）。

## 备份与恢复

每次运行后会生成 `stock_collector/data/backup/YYYY-MM-DD/` 目录，包含：

- `stock_daily.db`
- `YYYY-MM-DD.json`
- `manifest.json`

恢复方式：

1. 将备份目录中的 `stock_daily.db` 覆盖到 `stock_collector/data/stock_daily.db`。
2. 继续执行 `python stock_collector/main.py --run` 即可续跑且保证幂等写入。

## 自检记录

以下命令在仓库根目录执行：

- `python -c "import yaml, playwright, pytz"`
- `python stock_collector/main.py --run`

执行结果：

- `python -c "import yaml, playwright, pytz"`：失败，提示 `ModuleNotFoundError: No module named 'yaml'`（依赖安装受网络代理限制，无法从 PyPI 拉取 playwright）。
- `python stock_collector/main.py --run`：失败，同样因缺少 `yaml` 依赖而退出。
