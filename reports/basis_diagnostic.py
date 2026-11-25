"""Separate local HTML diagnostic; never included in the official report."""

import json
from decimal import Decimal
from html import escape

try:
    from ..core.basis_diagnostic import basis_rows
except ImportError:
    from core.basis_diagnostic import basis_rows


def _fmt(value):
    return f"{value:.10f}".rstrip("0").rstrip(".") if value else "0" if value == 0 else "—"


def _instrument_list(legs):
    names = {row.get("nativeSymbol") or row["symbol"] for leg in legs for row in leg}
    return " / ".join(sorted(names)) or "—"


def _cells(values):
    return "".join(f"<td>{escape(str(value))}</td>" for value in values)


def build_basis_diagnostic(state_path, output_path, account_id, trading_day, aliases):
    state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    baseline, latest = state.get("baselineBasisSnapshot"), state.get("latestBasisSnapshot")
    rows = basis_rows(baseline, latest, aliases)
    available = [row[4] for row in rows if row[4] is not None]
    if not baseline:
        status = "诊断基准尚未采集"
    elif not latest:
        status = "本轮双边持仓采集不完整"
    elif not rows:
        status = "两端均无持仓"
    elif not available:
        status = "无可计算标的"
    else:
        status = f"基差市值变动小计（{len(available)}/{len(rows)} 个标的）：{_fmt(sum(available, Decimal(0)))} U"
    lines = []
    for symbol, legs, start, end, change, error in rows:
        lines.append("<tr>" + _cells([symbol, _instrument_list(legs[::2]), _instrument_list(legs[1::2]),
            *(_fmt(v) for v in (start or [None] * 8)), *(_fmt(v) for v in (end or [None] * 8)),
            _fmt(change), error or "—"]) + "</tr>")
    headers = ["BN 持仓", "BN 标记价", "HL 持仓", "HL 推算标记价", "对冲量 H", "基差 S", "净敞口数量", "H×S"]
    headings = "".join(f"<th>{h}</th>" for h in headers * 2)
    start_at = escape(str((baseline or {}).get("sampledAt") or "—"))
    end_at = escape(str((latest or {}).get("sampledAt") or "—"))
    official_at = escape(str(state.get("baselineTime") or "—"))
    content = f"""<!doctype html><html lang="zh-CN"><meta charset="utf-8">
<title>双边底仓基差诊断</title><style>
body{{font:15px system-ui,sans-serif;margin:24px;color:#172435}}table{{border-collapse:collapse;white-space:nowrap}}
th,td{{border:1px solid #d4dce5;padding:7px;text-align:center}}th{{background:#edf3fb}}
main{{overflow:auto}}tbody tr:nth-child(even){{background:#f6f8fa}}
</style><body><h1>{escape(account_id)} · {escape(trading_day)} 双边底仓基差诊断</h1>
<p>诊断起点：{start_at}　｜　当前样本：{end_at}　｜　正式权益基准：{official_at}</p>
<p>诊断起点为本交易日首份满足条件的样本；若当天中途上线，不代表 09:30。价格与金额均以 U 计。
HL 标记价由持仓均价、未实现盈亏和带符号持仓量推算；首版只核算 Binance USDT 与 HL USDC 抵押的配对标记价。</p>
<p><strong>{escape(status)}</strong>。仅解释双边持仓的基差市值变动，不计入正式总账，也不等同于已实现收益。</p>
<main><table><thead><tr><th rowspan="2">标的</th><th rowspan="2">Binance 合约</th><th rowspan="2">HL 合约</th>
<th colspan="8">诊断基准</th><th colspan="8">当前样本</th><th rowspan="2">基差贡献 Δ (U)</th><th rowspan="2">说明</th></tr>
<tr>{headings}</tr></thead><tbody>{''.join(lines)}</tbody></table></main></body></html>"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return output_path
