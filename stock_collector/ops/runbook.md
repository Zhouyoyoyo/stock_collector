# 采集失败 Runbook

1. 检查 logs/ 目录与 summary.json 的 top_errors。
2. 优先确认网络与新浪页面是否可访问。
3. 如为单个 symbol 缺失，使用修复脚本重新抓取。
4. 如为大面积失败，检查 Playwright 依赖与 GitHub Actions 运行日志。
