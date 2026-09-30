# 监控程序

本地部署按照`启动步骤`标题下的步骤实施即可

## 环境

python 3.11~3.13

## 功能：公共最优报价采集

在`config/accounts.local.json` 的
`market_data` 处配置控制共享采集器，默认启用，缓存30秒（每订阅最多10000条），
每次实际成交保存此前30秒及此后10秒的报价更新。所有账户共用市场/交易对订阅，
重叠成交窗口通过写入游标合并。

## 回调数据目录

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
`all_callbacks.jsonl`保存全部订单状态，
`trade_callbacks.jsonl`保存配对所需记录，`equity.jsonl`作为账户快照，保留当日9：30和当前的账户信息用于权益计算，
本项目设计成交单(成交，部分成交，部分撤回)分为matched，unmatched，exposures_matches和exposures_remain
对应`matches/*.jsonl`为各个交易对对冲配对记录，`unmatched.jsonl`为未配对记录，主要是程序对冲失败超时或者手动平敞口记录，
`exposure_matches.jsonl`是将未配对订单做二次配对，主要按照buy,sell方向撮合，`exposure_remain.jsonl`是二次撮合后剩余微小数量记录，
`funding.jsonl`则是账户资费记录，采用rest定时采集和wss实时推送相互验证，所以记录中可以看到两种方式的资费记录

行情按本地接收时间、自然日和小时保存，例如：
`runtime/raw/20260909/spot/AAVEUSDC/15.jsonl`、
`runtime/raw/20260909/futures/AAVEUSDT/15.jsonl`。
字段为 `receivedTimeMs`、`eventTimeMs`、`transactionTimeMs`、`updateId`、
`sequence`、`bidPrice`、`bidQty`、`askPrice`、`askQty`，精度保留6位8位或者10位。
窗口采用本地接收时刻对齐，交易所成交时间保存，供后续分析。
该文件同时保存成交触发、实际可用缓存起点、连接和断线事件，可以查看log/*run.txt进行排查。
首次订阅之前、网络断开期间、缓存达到条数上限被淘汰的行情无法补回。

一个小时结束60秒后压缩为 `.jsonl.gz`，压缩/写入在后台进行。
仅行情 `runtime/raw` 保留今天及前两天，定时删除更早的自然日目录；
账户原始回调和收益文件不受清理影响。
所有账户在该市场/交易对均无挂单连续60秒，且成交保存窗口结束后，
关闭对应连接并释放缓存。有活跃挂单即使长时间没有回调也继续订阅。

启动及每60秒用所选产品的REST校准挂单：普通Spot `/api/v3/openOrders`、PM Margin
`/papi/v1/margin/openOrders`、PM UM `/papi/v1/um/openOrders`、普通USDⓈ-M
`/fapi/v1/openOrders`。按返回结果发现并维护交易对；
成交回调仍会即时创建交易对订阅，因此不依赖历史 JSONL 扫描。首次部署且从未收到回调的既有挂单也能通过该接口发现。

现货使用一条 combined `bookTicker` WebSocket，期货使用另一条 combined WebSocket。
交易对变化时通过连接内的 `SUBSCRIBE` / `UNSUBSCRIBE` 消息动态更新，不重建连接；只有真正断线时才自动重连。
行情写入队列有上限，磁盘跟不上时淘汰最旧行情；
连接、重连、退订和存储异常记录到运行日志。成交窗口、连接状态和退订等关键事件使用独立的无淘汰队列，优先落盘。

这是一个只读监控程序，不涉及下单操作。一个常驻进程同时监控数个子账户，持续保存成交 JSONL，并按固定间隔自动完成配对、权益/收益计算、Excel/HTML 生成和邮件发送。

## 启动步骤

推荐使用uv环境

克隆项目，安装uv

windows:
```powershell
git clone https://github.com/mengxin000/py_account_monitor.git
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

linux
```powershell
git clone https://github.com/mengxin000/py_account_monitor.git
curl -LsSf https://astral.sh/uv/install.sh | sh
```
## 配置

每个账户一个配置文件，凭证直接放在账户文件中：

以三个账户为例

windows:
```powershell
cd python_binance
Copy-Item config/account_zdl.local.example.json config/account_zdl.local.json
Copy-Item config/account_mfx.local.example.json config/account_mfx.local.json
Copy-Item config/account_dh.local.example.json config/account_dh.local.json
```
linux:
```powershell
cd python_binance
cp config/account_zdl.local.example.json config/account_zdl.local.json
cp config/account_mfx.local.example.json config/account_mfx.local.json
cp config/account_dh.local.example.json config/account_dh.local.json
```
分别编辑三个文件，填写 `api_key`、`secret_key`、`subaccount_email(可以跳过)、spot.mode和futures.mode`。
API Key 只需读取和用户数据权限。

表示现货用普通现货，期货用普通合约
```json
  "spot": {
    "mode": "spot"
  },
  "futures": {
    "mode": "usdm"
  },
```
表示现货用PM现货，期货用PM合约
```json
  "spot": {
    "mode": "pm_margin"
  },
  "futures": {
    "mode": "pm_um"
  },
```

也可用 `key_type` 明确指定。例：
```json
"spot": {
  "mode": "spot",
  "key_type": "auto"
},
"futures": { "mode": "usdm" },
```
`key_type`支持 `auto`、`hmac`、`ed25519`。

账户文件用 `spot.mode` 和 `futures.mode` 分别选择产品。顶层
`api_key` / `secret_key` 是 spot 凭证；下层usdm `api_key` / `secret_key` 是futures凭证
默认自动识别 HMAC 或 Ed25519；

支持的模式：`spot.mode` 为 `spot`（普通现货）、`pm_margin`（PM Margin 现货）或
`none`；`futures.mode` 为 `pm_um`（PM U 本位期货）、`usdm`（普通 USDⓈ-M 期货）或
`none`。

**事实上无需修改key_type，只需填入密钥和模式，再配置邮箱发送，就可以使用uv启动**

## 创建多账户总配置和邮件配置：

windows:
```powershell
Copy-Item config/accounts.local.example.json config/accounts.local.json
Copy-Item config/email.local.example.json config/email.local.json
```

linux:
```powershell
cp config/accounts.local.example.json config/accounts.local.json
cp config/email.local.example.json config/email.local.json
```

在accounts.local.json配置要监控的账户，将其填入`accounts`，
其中`first_report_delay_seconds`: 60,`report_interval_seconds`: 1800   用于调整邮件发送的时间，
first_report_delay_seconds是程序启动后第一封邮件发送时间，report_interval_seconds是下一封邮件发送间隔时间，单位为秒

在email.local.json中配置邮箱发送邮件
需要将`address`，`user`，`pass`改为你自己的，用于smtp邮件验证，
`sender` 为邮件发送方，`recipients`为邮件接收方，`cc`为抄送

以163邮箱smtp发送为例
```json
{
  "address": "smtp://smtp.163.com:587",
  "user": "monitor@163.com",
  "pass": "your-smpt-pass",
  "sender": "monitor@163.com",
  "recipients": ["recipients@example.com"],
  "cc": [],
  "use_starttls": true,
  "subject_prefix": "Binance账户监控"
}
```

## 启动

在项目目录执行一次即可常驻运行：

```powershell
uv sync
uv run binance-monitor
```

程序启动后会同时执行：

1. 每个账户每 5 秒 REST 获取账户权益和持仓，但只保留当日 09:30 基准与最新权益状态，不再逐次写 `account_info.jsonl`；
2. WebSocket 接收成交和订单回调，并按交易对写入 JSONL；
3. 在 `00:05、08:05、16:05` 查询一次已结算资金费率；
4. 每 30 分钟读取当天所有 JSONL，按照规则配对；
5. 生成每个账户的 Excel 和 HTML；
6. 一封邮件发送三个账户的汇总 HTML 和三个 Excel 附件；
7. 网络断开自动重连，程序重启后仍从 JSONL 和 `equity.json` 重新计算。

停止程序使用 `Ctrl+C`。不再需要单独运行采集、重放、报告或发邮件命令。

连接成功、断线、自动重连、账户采集、成交写入、报告生成和邮件发送都会追加到
`log/YYYYMMDDrun.txt`；终端只显示每秒刷新的状态面板和错误摘要。

## 核心逻辑
配对规则位于`core/legacy_matching.py`，撮合回放位于`replay/batch_replay.py`

每个账户每天只有一个原始成交回调文件。报告回放时先按基础币分组；例如 `AAVEUSDT` 与 `AAVEUSDC` 都进入 AAVE 匹配器，普通配对结果写入 `matches/AAVE.jsonl`，Exposure也只在AAVE内部处理。

报告和excel位于 `output/<account_id>/YYYYMMDD/`。

