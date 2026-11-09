"""Read-only terminal rendering, separate from diagnostics written to disk."""
from datetime import datetime
import unicodedata

try:
    from ..reports.funding import configured_markets, EXCHANGE_NAMES, MARKET_NAMES
except ImportError:
    from reports.funding import configured_markets, EXCHANGE_NAMES, MARKET_NAMES


def display_width(value):
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in str(value))


def cell(value, width):
    text = str(value)
    return text + " " * max(0, width-display_width(text))


def number(value, integer=False):
    if value is None or value == "-":
        return "—"
    return f"{float(value):,.0f}" if integer else f"{float(value):,.4f}"


def leg_view(exchange, market, status, config):
    if exchange == "hyperliquid":
        return ("Hyperliquid", "hyperliquid", status.get("hyperliquid_equity"),
                status.get("hyperliquid_at"), status.get("hyperliquid_mmr"))
    if exchange == "binance":
        scope = {"spot": "spot", "usdm": "usdm", "pm_margin": "pm", "pm_um": "pm"}.get(market)
        name = "Binance " + MARKET_NAMES.get(market, market)
        at = status.get("spot_at") if scope == "spot" else status.get("account_at")
        mmr = status.get("unimmr") if scope in {"pm", "usdm"} else None
        if scope == "pm" and getattr(config, "futures_mode", "none") == "usdm":
            mmr = None  # Combined status carries USDM's ratio, not PM's.
        return name, scope, status.get(f"{scope}_equity"), at, mmr
    return EXCHANGE_NAMES.get(exchange, exchange) + " " + market, exchange, None, None, None


def render(monitors, report_state):
    now = datetime.now()
    target = report_state.get("next_report")
    countdown = max(0, int((target-now).total_seconds())) if target else 0
    lines = ["多账户监控 | Ctrl+C 停止 | 每1秒刷新 | 权益单位 U",
             f"当前时间 {now:%Y-%m-%d %H:%M:%S} | 换日 09:30 | 下次报告 {countdown//60}分{countdown%60}秒"]
    for monitor in monitors:
        status = monitor.status()
        legs = [leg_view(exchange, market, status, monitor.config)
                for exchange, market in configured_markets(monitor.config)]
        lines.extend(["", f"[{status['account_id']}] " + "─" * 64])
        headers, values, seen = [], [], set()
        for name, scope, equity, at, mmr in legs:
            shared = scope == "pm" and scope in seen
            headers.append("Binance PM共用资金" if shared else "Binance PM统一权益" if scope == "pm" else name + "权益")
            values.append("与前列共用" if shared else number(equity))
            seen.add(scope)
        headers.extend(["合计权益", "实际损益"])
        values.extend([number(status.get("total_equity")), number(status.get("actual_profit"))])
        widths = [max(20, display_width(h), display_width(v)) for h, v in zip(headers, values)]
        for row in (headers, values):
            lines.append(" | ".join(cell(x, w) for x, w in zip(row, widths)).rstrip())
        for name, scope, equity, at, mmr in legs:
            if scope in status.get("rest_errors", {}):
                rest = "失败"
            elif at is None:
                rest = "未就绪"
            else:
                age = max(0, int((now - at).total_seconds()))
                rest = f"{age}s前" if age < max(30, monitor.config.rest_interval_seconds * 3) else "过期"
            stream = status.get("private_streams", {}).get(scope, "未就绪")
            lines.append(f"  {name} | REST {rest} | 私有流 {stream} | MMR {number(mmr, True)}")
        trade = status.get("trade_at")
        lines.append("  最近成交 " + (trade.strftime("%H:%M:%S") if trade else "—"))
        for errors in (status.get("rest_errors", {}), status.get("stream_errors", {})):
            for key, error in errors.items():
                lines.append(f"  ! {key}: {error}")
    market = getattr(monitors[0], "market_collector", None) if monitors else None
    if market:
        names = {"spot": "Binance现货", "futures": "Binance期货", "hyperliquid": "Hyperliquid"}
        keys = set(market.connection_states) | set(market.market_tasks)
        states = [f"{names.get(key, key)}={market.connection_states.get(key, 'CONNECTING') if key in market.market_tasks else 'IDLE'}"
                  for key in names if key in keys]
        lines.append("\n公共行情: " + (" | ".join(states) if states else "暂无订阅"))
    lines.append("运行日志：log/YYYYMMDDrun.txt")
    return "\n".join(lines)
