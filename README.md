# Binance 多账户监控

这是一个只读监控程序，不进行下单操作。一个常驻进程同时监控三个子账户，持续保存成交 JSONL，并按固定间隔自动完成配对、权益/收益计算、Excel/HTML 生成和邮件发送。

## 配置

每个账户一个配置文件，凭证直接放在账户文件中：

```powershell
Copy-Item config/account_zdl.local.example.json config/account_zdl.local.json
Copy-Item config/account_mfx.local.example.json config/account_mfx.local.json
Copy-Item config/account_dh.local.example.json config/account_dh.local.json
```

分别编辑三个文件，填写 `api_key`、`secret_key`、`subaccount_email`。API Key 只需读取和用户数据权限，不需要交易、提现或转账权限。

创建多账户总配置和邮件配置：

```powershell
Copy-Item config/accounts.local.example.json config/accounts.local.json
Copy-Item config/email.local.example.json config/email.local.json
```

`accounts.local.json` 只负责列出三个账户和报告周期。日期不需要配置，程序每天自动使用当前日期创建目录，跨午夜会自动切换到新日期目录。订单直接配对以客户端ID/系统ID关联为准，不受交易对限制，因此 USDT 与 USDC 对冲不需要额外配置；ID匹配失败后的 Exposure 抵消仍只在同一基础币内进行。

## 启动

在项目目录执行一次即可常驻运行：

```powershell
cd F:\桌面\CalculateMonitor\python_binance
uv run binance-monitor
```

程序启动后会同时执行：

1. 每个账户每 5 秒 REST 获取账户权益和持仓，但只保留当日 09:30 基准与最新权益状态，不再逐次写 `account_info.jsonl`；
2. WebSocket 接收成交和订单回调，并按交易对写入 JSONL；
3. 在 `00:05、08:05、16:05` 查询一次已结算资金费率；
4. 每 30 分钟读取当天所有 JSONL，按照旧 `strategy.cpp` 规则配对；
5. 生成每个账户的 Excel 和 HTML；
6. 一封邮件发送三个账户的汇总 HTML 和三个 Excel 附件；
7. 网络断开自动重连，程序重启后仍从 JSONL 和 `equity.json` 重新计算，不依赖内存队列。

停止程序使用 `Ctrl+C`。不再需要单独运行采集、重放、报告或发邮件命令。

连接成功、断线、自动重连、账户采集、成交写入、报告生成和邮件发送都会追加到
`log/YYYYMMDDrun.txt`；终端只显示每秒刷新的状态面板和错误摘要。

## 数据目录

```text
runtime/
├─ zdl/YYYYMMDD/
│  ├─ trade_callbacks.jsonl
│  ├─ matches/AAVE.jsonl
│  ├─ matches/SUI.jsonl
│  ├─ unmatched.jsonl
│  ├─ exposure_matches.jsonl
│  ├─ exposure_remain.jsonl
│  ├─ funding.jsonl
│  └─ equity.json
├─ mfx/YYYYMMDD/
└─ dh/YYYYMMDD/
```

每个账户每天只有一个原始成交回调文件。报告回放时先按基础币分组；例如 `AAVEUSDT` 与 `AAVEUSDC` 都进入 AAVE 匹配器，普通配对结果写入 `matches/AAVE.jsonl`，Exposure也只在AAVE内部处理。

报告位于 `output/<account_id>/YYYYMMDD/`。cli为测试、重放和报告命令仍保留，供故障排查和历史数据补算使用，但日常运行不需要它们。

实际损益是 09:30 基准权益到当前权益的差值；交易损益来自配对成交；费率损益来自 Binance 资金费率历史；波动损益来自 09:30 基准持仓与当前标记价格的变化；理论损益 = 交易损益 + 费率损益 + 波动损益；损益差值用于校验实际与理论的偏差。
