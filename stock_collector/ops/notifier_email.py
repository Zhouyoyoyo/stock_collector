import os
import smtplib
from email.message import EmailMessage
from typing import Any

import yaml


CONFIG_PATH = "stock_collector/config/notify.yaml"


def _load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as file_handle:
        raw = file_handle.read()
    expanded = os.path.expandvars(raw)
    return yaml.safe_load(expanded)


def send_email(summary: dict[str, Any], missing_symbols: list[str]) -> None:
    """发送邮件通知。"""
    config = _load_config()["email"]
    if not config.get("enabled", True):
        print("[notify] 邮件通知已禁用")
        return

    smtp_host = config.get("smtp_host")
    smtp_port = int(config.get("smtp_port") or 0)
    smtp_user = config.get("smtp_user")
    smtp_pass = config.get("smtp_pass")
    mail_to = config.get("to")
    mail_from = config.get("from")

    subject = (
        f"[A股采集][{summary['date']}] {summary['level']} "
        f"{summary['success_rate']:.1%} ({summary['success']}/{summary['expected']}) "
        f"missing={summary['missing']}"
    )

    body_lines = [
        f"日期: {summary['date']}",
        f"成功率: {summary['success_rate']:.1%}",
        f"成功: {summary['success']}",
        f"失败: {summary['failed']}",
        f"缺失: {summary['missing']}",
        f"告警级别: {summary['level']}",
        f"人工介入: {summary['human_required']}",
        "",
    ]

    if summary.get("human_required"):
        body_lines.extend(
            [
                "建议动作（Runbook 简版）:",
                "1) 检查日志确认失败原因", 
                "2) 重新运行补缺任务", 
                "3) 必要时手动补录", 
                "",
            ]
        )

    if missing_symbols:
        body_lines.append("缺失股票前 30 只:")
        body_lines.extend(missing_symbols[:30])

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = mail_from
    message["To"] = mail_to
    message.set_content("\n".join(body_lines))

    if smtp_port == 465:
        server = smtplib.SMTP_SSL(smtp_host, smtp_port)
    else:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()

    with server:
        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)
        server.send_message(message)
    print("[notify] 邮件已发送")
