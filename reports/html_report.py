from __future__ import annotations

import html
from pathlib import Path

from .report_data import ReportData


def _number(value: object) -> str:
    if isinstance(value, (int, float)):
        return f"{value:.10f}"
    return html.escape(str(value))


def build_html(data: ReportData, path: Path) -> Path:
    summary = data.profit_summary
    matched = int(summary["matched_count"])
    unmatched = int(summary["unmatched_count"])
    total = matched + unmatched
    total_quantity = float(summary["matched_quantity"]) + float(summary["unmatched_quantity"])
    ratio = matched / total * 100 if total else 0.0
    unmatched_ratio = unmatched / total * 100 if total else 0.0
    wins = sum(1 for row in data.matches if float(row.get("profit", 0) or 0) > 0)
    losses = sum(1 for row in data.matches if float(row.get("profit", 0) or 0) < 0)
    volatility = summary["volatility_profit"] if summary.get("volatility_available") else "不适用"
    symbol_rows = "".join(
        f"<tr><td>{html.escape(str(x['symbol']))}</td><td>{x['fill_count']}</td><td>{x['match_count']}</td>"
        f"<td>{x['matched_quantity']:.8f}</td><td>{x['profit']:.8f}</td><td>{x['unmatched_quantity']:.8f}</td></tr>"
        for x in data.symbol_summary()
    ) or "<tr><td colspan='6'>暂无成交记录</td></tr>"
    metrics = [
        ("实际损益", summary["actual_profit"]),
        ("理论损益", summary["theoretical_profit"]),
        ("损益差值", summary["profit_difference"]),
        ("交易损益", summary["trade_profit"]),
        ("费率损益", summary["fee_profit"]),
        ("波动损益", volatility),
        ("24h总成交量", total_quantity),
        ("总成交条数", total),
        ("配对条数", matched),
        ("配对比例(%)", ratio),
        ("未配对条数", unmatched),
        ("未配对比例(%)", unmatched_ratio),
        ("盈利条数", wins),
        ("亏损条数", losses),
        ("HK/US及市场挂单统计", "不适用"),
    ]
    metric_rows = "".join(f"<tr><th>{html.escape(str(label))}</th><td>{_number(value)}</td></tr>" for label, value in metrics)
    body = f"""<!doctype html><html><head><meta charset='utf-8'><style>
body{{font-family:Arial,'Microsoft YaHei',sans-serif;color:#111;margin:24px}}h2{{margin:0 0 18px}}table{{border-collapse:collapse;margin:12px 0 24px;min-width:680px}}th,td{{border:1px solid #808080;padding:7px 12px;text-align:center}}th{{background:#000;color:#fff;font-weight:700}}td{{background:#fff}}.loss{{background:#ffc8c8}}.section{{margin-top:22px}}
</style></head><body><h2>{html.escape(data.account_id)} 账户监控日报 - {html.escape(data.day)}</h2>
<div class='section'><table><tbody>{metric_rows}</tbody></table></div>
<div class='section'><h3>交易对汇总</h3><table><tr><th>交易对</th><th>成交事件</th><th>配对次数</th><th>配对数量</th><th>配对收益</th><th>未配对数量</th></tr>{symbol_rows}</table></div>
</body></html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path
