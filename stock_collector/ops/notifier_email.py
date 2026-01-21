import os
import smtplib
from datetime import date
from email.message import EmailMessage
from typing import Any

import yaml


CONFIG_PATH = "stock_collector/config/notify.yaml"


def _load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as file_handle:
        raw = file_handle.read()
    expanded = os.path.expandvars(raw)
    return yaml.safe_load(expanded)


def send_email(
    summary: dict[str, Any] | None = None,
    missing_symbols: list[str] | None = None,
    *,
    subject: str | None = None,
    body: str | None = None,
    to_addr: str | None = None,
) -> bool:
    """发送邮件通知。"""
    config = _load_config()["email"]
    if not config.get("enabled", True):
        print("[notify] 邮件通知已禁用")
        return False

    smtp_host = config.get("smtp_host")
    try:
        smtp_port = int(config.get("smtp_port") or 0)
    except Exception:
        smtp_port = 0
    smtp_user = config.get("smtp_user")
    smtp_pass = config.get("smtp_pass")
    mail_to = config.get("to")
    mail_from = config.get("from")

    # ✅ 修复：允许无认证 SMTP（本地 relay / IP 白名单），只在同时具备 user+pass 时才 login
    if (smtp_user and not smtp_pass) or (smtp_pass and not smtp_user):
        # 配置不一致：只给出警告，但仍尝试“无认证发送”
        print("[notify] SMTP 凭据不完整，将尝试无认证发送（跳过 login）")
    elif not smtp_user and not smtp_pass:
        # 完全未配置凭据：允许无认证发送
        print("[notify] 未配置 SMTP 凭据，将尝试无认证发送（跳过 login）")

    # ✅ 强制：邮箱配置不完整时必须降级，不允许中断主流程
    required_fields = {
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
        "to": mail_to,
        "from": mail_from,
    }
    missing = [k for k, v in required_fields.items() if not v or (k == "smtp_port" and int(v) <= 0)]
    if missing:
        print(f"[notify] 邮件配置缺失/非法，已跳过发送: missing={missing}")
        return False

    if subject is None or body is None:
        if summary is None:
            print("[notify] 邮件内容缺失，已跳过发送")
            return False

        missing_symbols = missing_symbols or []
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
        body = "\n".join(body_lines)

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = mail_from
    message["To"] = to_addr or mail_to
    message.set_content(body)

    if smtp_port == 465:
        server = smtplib.SMTP_SSL(smtp_host, smtp_port)
    else:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()

    try:
        with server:
            server.ehlo()

            # 严格要求：SMTP_USER / SMTP_PASS 必须同时存在
            if not smtp_user or not smtp_pass:
                print(
                    "[notify][WARN] SMTP_USER / SMTP_PASS 缺失，"
                    "当前 SMTP 服务器不支持匿名发送，已跳过邮件通知"
                )
                return False

            server.login(smtp_user, smtp_pass)
            server.send_message(message)

        print("[notify] 邮件已成功发送")
        return True

    except Exception as e:
        # ⚠️ 通知失败不允许影响主流程
        print(f"[notify][ERROR] 邮件发送失败（已吞异常，不影响主流程）: {e}")
        return False


# =========================
# SMS via Email (China Telecom 189)
# =========================

_SMS_SENT_FLAG = None  # 仅用于进程内防重复


def send_sms_via_email_once_per_day(message: str):
    """
    通过中国电信 Email-to-SMS 网关发送短信（189.com）。
    规则：
    - 仅 CRITICAL 调用
    - 同一自然日最多发送一次（进程级防抖）
    - 失败静默，不允许抛异常
    """
    global _SMS_SENT_FLAG

    today = date.today().isoformat()
    if _SMS_SENT_FLAG == today:
        print("[notify] SMS already sent today, skip")
        return

    phone = os.getenv("PHONE_NUM")
    if not phone:
        print("[notify] PHONE_NUM not set, skip SMS")
        return

    sms_addr = f"{phone}@189.com"

    try:
        subject = "系统告警"
        body = message[:120]  # 防止内容过长被运营商丢弃

        sent = send_email(
            to_addr=sms_addr,
            subject=subject,
            body=body,
        )

        if not sent:
            return
        _SMS_SENT_FLAG = today
        print(f"[notify] SMS via email sent to {sms_addr}")

    except Exception as e:
        # ❗短信失败不能影响主流程
        print(f"[notify] SMS via email failed (ignored): {e}")
