# Binance 多账户监控

## 公共最优报价采集

使用原启动命令 `uv run binance-monitor`。`config/accounts.local.json` 的
`market_data` 配置控制共享采集器，默认启用，缓存30秒（每订阅最多10000条），
每次实际成交保存此前30秒及此后10秒的报价更新。所有账户共用市场/交易对订阅，
重叠成交窗口通过写入游标合并，不重复保存相同缓存记录。

账户09:30交易日目录中，`all_callbacks.jsonl`保存全部订单状态，
`trade_callbacks.jsonl`继续保存配对所需记录；分析文件不进入配对或成交量统计。

行情按本地接收时间、自然日和小时保存，例如：
`runtime/raw/20260909/spot/AAVEUSDC/15.jsonl`、
`runtime/raw/20260909/futures/AAVEUSDT/15.jsonl`。
字段为 `receivedTimeMs`、`eventTimeMs`、`transactionTimeMs`、`updateId`、
`sequence`、`bidPrice`、`bidQty`、`askPrice`、`askQty`，价格数量保留原始字符串精度。
接口没有提供交易所时间时保存null，不伪造时间。
窗口采用本地接收时刻对齐，交易所成交时间保留在 `windows.jsonl` 供后续分析。
该文件同时保存成交触发、实际可用缓存起点、连接和断线事件。
首次订阅之前、网络断开期间、缓存达到条数上限被淘汰的行情无法补回。

一个小时结束60秒后压缩为 `.jsonl.gz`，压缩/写入在后台进行。
仅行情 `runtime/raw` 保留今天及前两天，定时删除更早的自然日目录；
账户原始回调和收益文件不受清理影响。
所有账户在该市场/交易对均无挂单连续60秒，且成交保存窗口结束后，
关闭对应连接并释放缓存。有活跃挂单即使长时间没有回调也继续订阅。

启动及每60秒用统一账户REST校准挂单。现货保证金 `/papi/v1/margin/openOrders` 和
U 本位期货 `/papi/v1/um/openOrders` 均可不传 `symbol` 返回全部当前挂单，按返回结果发现并维护交易对；
成交回调仍会即时创建交易对订阅，因此不依赖历史 JSONL 扫描。首次部署且从未收到回调的既有挂单也能通过该接口发现。

现货使用一条 combined `bookTicker` WebSocket，期货使用另一条 combined WebSocket。
交易对变化时通过连接内的 `SUBSCRIBE` / `UNSUBSCRIBE` 消息动态更新，不重建连接；只有真正断线时才自动重连。
行情写入队列有上限，磁盘跟不上时淘汰最旧行情；
连接、重连、退订和存储异常记录到运行日志。成交窗口、连接状态和退订等关键事件使用独立的无淘汰队列，优先落盘。

接口依据：[现货WebSocket](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)、
[UM bookTicker](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams)、
[统一账户订单接口](https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/trade)。

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
