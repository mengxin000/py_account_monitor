# OKX 只读连接与主服务接入

主服务已支持在现有账户文件的 `legs` 中选择 OKX，与 Binance 或 Hyperliquid 组合，沿用一个启动命令、09:30 基准和现有报表布局。
未自动更换任何生产账户的交易所或凭证，也不添加新账户配置文件。

## 主服务用法

在现有 `config/account_dh.local.json`（或其他账户文件）中，将需要更换的那条腿设为：

```json
{
  "exchange": "okx",
  "market": "perp",
  "credentials": {
    "api_key": "填写OKX_API_KEY",
    "secret_key": "填写OKX_SECRET_KEY",
    "passphrase": "填写创建API时设置的口令"
  },
  "usd_to_usdt": 1.0,
  "usdc_to_usdt": 1.0,
  "token_rates": {},
  "symbol_map": {},
  "reconcile_interval_seconds": 60,
  "simulated": false
}
```

`market` 可为 `spot`、`margin`、`perp`、`futures`，对应 SPOT/MARGIN/SWAP/FUTURES。API 凭证必须属于实际被监控的账户。
两条腿都是 OKX 时目前必须是同一实际账户、不同市场，凭证及估值设置一致；只创建一个采集器，总权益只计一次。
其他腿保留既有 Binance/Hyperliquid 配置。主启动命令仍为 `uv run python -m service.daemon --config config/accounts.local.json`。
默认配对规则维护在 `core/order_linking.py::RULE_TABLE`；不需要另建映射配置文件。

主服务新增 `collectors/okx/integrated.py`：

- 原始 WS 推送写入公共 `runtime/<账户>/<交易日>/all_callbacks.jsonl`，不把累计订单状态当作逐笔成交。
- REST `fills-history` 按 billId 去重，查产品规格后标准化，写公共 `trade_callbacks.jsonl`，原始数据和 normalizedOrder 同存。
- 启动、重连、定时及成交推送触发核对。循环核对至少间隔 5 秒；默认空闲等待 60 秒；发报告前也会核对。
- REST 缺少 clOrdId 字段时按 instId/ordId 查询原订单。未下单的程序不会凭空生成关联 ID。
- 资金费以账单 pnl 的原符号换算为 U，写公共 funding.jsonl，按交易日累计并显示分市场及总资费。
- totalEq 是 USD 估值，乘 usd_to_usdt 后加入总权益，按主服务现有规则记录基准；查询失败不当作零权益。
- USD/USDC 默认按配置值 1 估值，并非实时汇率；其他手续费/资金费币种须提供 token_rates，缺少时明确报错。
- 同一个实际 OKX 账户的现货和合约权益共用，不重复计入。权益不含资金账户 `/asset/balances`。
- 标准化失败会留下原始审计记录并显式报错；首次核对未完成或核对失败的账户不生成伪完整报告。原有其他账户报告不受此过滤影响。

公共交易日沿用主程序本地时区，请将运行环境设为 Asia/Shanghai。下方独立审计目录仍固定北京时间。
OKX 网页挂单/盘口及公共 raw 行情采集本次不扩展。成交价差沿用现有公式：现货/期货以期货为分母；Binance/Hyperliquid 双期货保持原约定，其他同类型组合仍按 SELL-BUY/SELL，不新增腿方向口径。

支持 USDT/USDC 报价的现货及 ctValCcy 为标的币、ctMult=1 的线性合约；反向合约、期权、非单位乘数不支持。
下单数量按张数乘 ctVal 转换，成本手续费取反后换算；收益仍为卖出金额减买入金额减手续费，不使用 OKX realizedPnl 替代套利配对收益。

## 独立原始采集工具（可选排查，不同时运行同账户）

新建 `config/account_okx.local.json`（实际密钥不要提交到版本库）：

```json
{
  "account_id": "dh",
  "output_dir": "runtime/dh",
  "okx": {
    "enabled": true,
    "api_key": "填OKX API Key",
    "secret_key": "填OKX Secret Key",
    "passphrase": "创建API时设置的Passphrase",
    "simulated": false,
    "inst_types": ["SPOT", "SWAP", "FUTURES"],
    "rest_interval_seconds": 5,
    "reconcile_interval_seconds": 60
  }
}
```

在 `python_binance` 目录运行，一个命令持续采集：

```powershell
uv run python -m cli.collect_okx --config config/account_okx.local.json
```

`Ctrl+C` 正常停止。多个账户可重复传入 `--config`，每个账户有独立连接和数据目录。
API 只需读取权限，不需要交易、提现权限；Passphrase 不是登录密码。
也支持显式的 `api_key_env`、`secret_key_env`、`passphrase_env`，分别引用环境变量；同一字段不要同时填明文和 `_env`。
独立采集入口还可从 `legs` 中读取唯一的 `exchange: "okx"` 项；其原始数据不直接参与配对。

可在 `okx` 内加 `proxy: "http://127.0.0.1:7897"`。地区站点不同可显式设置 `rest_base_url` 和 `websocket_url`，按该账户所属 OKX 站点官方文档填写。
`simulated: true` 时 REST 带模拟盘标识，并默认连接模拟盘私有 WebSocket；不要混用实盘密钥。

## 已实现的接口

| 类型 | 路径 / 频道 | 用途 |
|---|---|---|
| 公共 REST | `/api/v5/public/time` | 中点时间同步；签名使用 UTC |
| 公共 REST | `/api/v5/public/instruments` | 保存合约面值/计价币/精度等原始元数据，每交易日更新 |
| 私有 REST | `/api/v5/account/balance` | 账户权益和币种余额，默认每 5 秒 |
| 私有 REST | `/api/v5/account/positions` | 原始持仓，默认每 5 秒 |
| 私有 REST | `/api/v5/trade/orders-pending` | 分页查询已有挂单，启动、重连和默认每 60 秒 |
| 私有 REST | `/api/v5/trade/fills-history` | 分页补查逐笔成交，同上 |
| 私有 REST | `/api/v5/account/bills?type=8` | 资金费账单，同上，保留 173/174 子类型 |
| 私有 WS | `orders / instType=ANY` | 新建、部分成交、成交、撤单等订单变更 |
| 私有 WS | `account` | 账户变更 |
| 私有 WS | `positions / instType=ANY` | 持仓变更 |

默认 REST `https://www.okx.com`；私有 WS `wss://ws.okx.com:8443/ws/v5/private`。
使用 HMAC-SHA256 + Base64 签名，等待登录和全部订阅确认才报告 CONNECTED。
文本 ping/pong、重连退避、请求节流（单实例最多约 4 GET/s）、限频重试均在连接模块中。
接收时记录微秒本地时间；有界队列满时阻塞、不主动丢弃订单事件；消费者写盘异常会报错停止，不能声称网络故障时不漏任何事件。
REST 补查可找回成交，**不能还原掉线期间全部 NEW/CANCELED 推送**。多个进程使用同一 OKX UID 时共享交易所限额，不要重复运行同一账户采集。

## 独立存储

```text
runtime/dh/okx/YYYYMMDD/
├─ all_callbacks.jsonl    # orders/account/positions 原始推送；按接收时间分桶
├─ trade_callbacks.jsonl  # 有成交量的订单状态；按成交时间分桶（含部分成交撤单）
├─ fills.jsonl            # REST 逐笔成交，billId 去重，不与累计订单状态相加
├─ funding.jsonl          # 原币种、原符号的资金费账单，billId 去重
├─ account.json           # 最新完整 REST 余额和持仓；原子替换
├─ open_orders.json       # 最新完整挂单快照；原子替换
└─ instruments.json       # 原始产品元数据
log/okx/YYYYMMDDrun.txt   # 连接、重连、错误等日志
```

按**北京时间 09:30**划分交易日，不依赖服务器系统时区。路径根据配置 `output_dir` 自动追加 `okx`。
REST 首次补查从当前交易日开始前 60 秒起；持续运行按成功水位回查并重叠 60 秒，跨日最多追溯前一交易日。重启不补查更早历史。
原始推送可包含重复状态；`trade_callbacks` 按订单状态身份去重，`fills` 按 billId 去重，重启从相应日志恢复去重信息。
程序会原样保存 `ordId/clOrdId/tradeId/fillSz/accFillSz/fillPx/avgPx/fee/feeCcy/fillFee` 等字段，不截断 ID，不翻转手续费符号，不假定每次推送是一笔增量成交。
OKX 衍生品数量可能是“张”；`totalEq` 是 USD 估值，不直接当作 USDT；普通现货和衍生品属于同一交易账户时也不重复加权益。
独立采集器不生成 `normalizedOrder`；主服务的 integrated 桥接器会额外生成公共逐笔输入并参与配对/邮件。范围与约束见 [ORDER_LINKING.md](ORDER_LINKING.md)。
当前交易账户余额不等同于“资金账户”余额；不包含 `/asset/balances`。

官方依据：[OKX API V5 文档](https://www.okx.com/docs-v5/en/)，REST Authentication、WebSocket Login、Orders channel、Balance、Fills history、Bills 章节。
本地可运行离线测试：`python -B -m unittest discover -s tests -p test_okx.py -v`。尚需用真实只读密钥验收网络与账户权限。
