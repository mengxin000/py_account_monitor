from __future__ import annotations

import html
from pathlib import Path

from .report_data import ReportData


def _number(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.10f}"
    if isinstance(value, int):
        return str(value)
    return html.escape(str(value))


def build_html(data: ReportData, path: Path) -> Path:
    summary = data.profit_summary
    matched = int(summary["matched_count"])
    unmatched = int(summary["unmatched_count"])
    total_quantity = float(summary["total_volume"])
    wins = int(summary["profit_count"])
    losses = int(summary["loss_count"])
    symbol_rows = "".join(
        f"<tr><td>{html.escape(str(x['symbol']))}</td><td>{x['volume']:.8f}</td><td>{x['match_count']}</td>"
        f"<td>{x['profit_count']}</td><td>{x['loss_count']}</td><td>{x['unmatched_count']}</td>"
        f"<td>{x['profit']:.8f}</td><td>{x['exposure_profit']:.8f}</td></tr>"
        for x in data.symbol_summary()
    ) or "<tr><td colspan='8'>暂无成交记录</td></tr>"
    metrics = [
        ("实际损益", summary["actual_profit"]),
        ("交易损益", summary["trade_profit"]),
        ("费率损益", summary["fee_profit"]),
        ("总成交量(U)", total_quantity),
    ]
    stats = [
        ("总配对数", matched),
        ("盈利配对数", wins),
        ("亏损配对数", losses),
        ("未匹配条数", unmatched),
    ]
    metric_headers = "".join(f"<th>{html.escape(str(label))}</th>" for label, _ in metrics)
    metric_values = "".join(f"<td>{_number(value)}</td>" for _, value in metrics)
    stats_headers = "".join(f"<th>{html.escape(str(label))}</th>" for label, _ in stats)
    stats_values = "".join(f"<td>{_number(value)}</td>" for _, value in stats)
    body = f"""<!doctype html><html><head><meta charset='utf-8'><style>
body{{font-family:Arial,'Microsoft YaHei',sans-serif;color:#111;margin:24px}}h2{{margin:0 0 18px}}table{{border-collapse:collapse;margin:12px 0 24px;min-width:680px}}th,td{{border:1px solid #808080;padding:7px 12px;text-align:center;white-space:nowrap}}th{{background:#1F4E78;color:#fff;font-weight:700}}td{{background:#fff}}.section{{margin-top:22px}}.analysis{{padding:12px 16px;background:#F3F6F9;border-left:4px solid #1F4E78}}
</style></head><body><h2>{html.escape(data.account_id)} 账户监控日报 - {html.escape(data.day)}</h2>
<div class='section'><h3>收益与成交量</h3><table><tr>{metric_headers}</tr><tr>{metric_values}</tr></table></div>
<div class='section'><h3>配对统计</h3><table><tr>{stats_headers}</tr><tr>{stats_values}</tr></table></div>
<div class='section'><h3>基础币汇总</h3><table><tr><th>基础币</th><th>成交量(U)</th><th>配对数</th><th>盈利配对数</th><th>亏损配对数</th><th>未匹配条数</th><th>配对收益</th><th>Exposure配对收益</th></tr>{symbol_rows}</table></div>
</body></html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path
