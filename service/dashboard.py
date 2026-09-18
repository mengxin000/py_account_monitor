"""Read-only terminal rendering, separate from diagnostics written to disk."""
from datetime import datetime
import unicodedata


def cell(value, width):
    text = str(value)
    size = sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)
    return text + " " * max(1, width-size)


def number(value, integer=False):
    if value is None or value == "-":
        return "—"
    return f"{float(value):,.0f}" if integer else f"{float(value):,.4f}"


def render(monitors, report_state):
    now = datetime.now()
    target = report_state.get("next_report")
    countdown = max(0, int((target-now).total_seconds())) if target else 0
    lines = ["Binance 多账户监控 | Ctrl+C 停止 | 每1秒刷新",
             f"当前时间 {now:%Y-%m-%d %H:%M:%S} | 换日 09:30 | 下次报告 {countdown//60}分{countdown%60}秒",
             "账户资产"]
    headers = ["账户", "PM权益", "现货权益", "合计权益", "实际损益", "MMR"]
    widths = [10, 17, 17, 17, 17, 14]
    lines.append(" | ".join(cell(x,w) for x,w in zip(headers,widths)))
    statuses = [m.status() for m in monitors]
    for s in statuses:
        values = [s["account_id"], number(s["actual_equity"]), number(s["spot_equity"]), number(s["total_equity"]), number(s["actual_profit"]), number(s["unimmr"], True)]
        lines.append(" | ".join(cell(x,w) for x,w in zip(values,widths)))

    lines.append("连接状态———————————————————————————————————————————————————————————————————————————————————————————————————")
    conn_headers = ["账户", "PM REST", "Spot REST", "PM私有流", "Spot私有流", "最近成交"]
    conn_widths = [10, 17, 17, 17, 17, 14]
    lines.append(" | ".join(cell(x, w) for x, w in zip(conn_headers, conn_widths)))
    for m,s in zip(monitors,statuses):
        def rest(key, at):
            if key in s["rest_errors"]: return "失败"
            if at is None: return "未就绪"
            age = (now-at).total_seconds()
            return f"{int(age)}s前" if age < max(30,m.config.rest_interval_seconds*3) else "过期"
        values = [s["account_id"], rest("pm",s["account_at"]), rest("spot",s["spot_at"]) if m.spot_stream else "禁用", s["private_streams"]["pm"], s["private_streams"]["spot"], s["trade_at"].strftime("%H:%M:%S") if s["trade_at"] else "—"]
        lines.append(" | ".join(cell(x, w) for x, w in zip(values, conn_widths)))
        for key,error in s["rest_errors"].items():
            lines.append(f"  {s['account_id']} {key}: {error}")
        for key,error in s["stream_errors"].items():
            lines.append(f"  {s['account_id']} {key}: {error}")
    market = monitors[0].market_collector if monitors else None
    if market:
        states = {k: market.connection_states.get(k,"CONNECTING") if k in market.market_tasks else "IDLE" for k in ("spot","futures")}
        lines.append(f"公共行情: Spot={states['spot']} Futures={states['futures']}")
    lines.append("运行日志：log/YYYYMMDDrun.txt")
    return "\n".join(lines)
