# Stock Collector（A 股收盘后日线采集｜API 优先 + Playwright 兜底）

本项目在 A 股收盘后自动采集全量日线数据，落库 SQLite，并生成 summary/告警/邮件通知与备份包。

核心设计：  
- **主路径：接口抓取（新浪 JSON K 线接口）** → 快、稳、适合全量  
- **兜底路径：Playwright + PO 模式抓取新浪页面** → 仅对接口失败的股票补抓  
- **幂等写入**：同一 symbol + trade_date 只写一次  
- **统计语义正确**：missing / skipped / failed 分离，避免误报

---

## 功能清单

- 采集字段（尽可能全，接口/DOM 可得即写入）：
  - OHLC、涨跌额(change)、涨跌幅(change_pct)
  - 成交量(volume：**股数**)、成交额(amount)
  - 振幅(amplitude_pct)、换手率(turnover_pct)（DOM 可得）
- SQLite 落库：`stock_collector/data/stock_daily.db`
- summary 输出：`stock_collector/data/summary/YYYY-MM-DD.json`
- 备份包输出：`stock_collector/data/backup/YYYY-MM-DD/`
- 告警与通知：
  - Email（SMTP）
  - CRITICAL 时可选 “短信转邮件”（电信 189 网关：`${PHONE_NUM}@189.com`）
- GitHub Actions 定时运行 + artifacts 上传 + DB cache 恢复

---

## 配置文件

项目关键变量已集中到 `stock_collector/config/app.yaml` 管理，便于统一修改路径与外部 URL：

- `paths`：数据目录、SQLite DB、summary、backup 等相对路径
- `urls`：新浪股票池接口、行情页与 K 线接口等 URL 模板

此外，股票池/调度/通知等仍在原有 YAML 文件中维护：

- `stock_collector/config/stocks.yaml`：股票池缓存路径与默认股票列表
- `stock_collector/config/schedule.yaml`：采集与告警规则
- `stock_collector/config/notify.yaml`：邮件/短信告警配置

---

## 快速开始（本地）

### 1) 安装依赖

```bash
pip install -r stock_collector/requirements.txt
python -m playwright install --with-deps chromium
```

### 2) 初始化全量股票池（首次运行或新环境）

```bash
python stock_collector/main.py --refresh-universe
```
