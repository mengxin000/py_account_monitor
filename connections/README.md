# 连接层

- `binance/base.py`：凭证、端点配置、公共异常和回调类型。
- `binance/rest.py`：HMAC REST 签名、时间同步、请求重试。
- `binance/pm_stream.py`：PM listenKey 创建、续期、失效重连与关闭。
- `binance/spot_stream.py`：普通现货 WebSocket API 签名订阅，独立重连。
- `binance/market_stream.py`：原有公共行情连接类。
- `core/connection.py` 保留兼容导出；旧导入方式不需要调整。

账户采集编排仍在 collectors/account_monitor.py，来源转换在
collectors/binance/normalize.py。本阶段不拆报表调度和权益存储，避免同时改动业务。

## 配置和启动

继续使用原来的单命令启动服务。每个账户默认启动 PM 和 Spot 两条私有流，
复用账户顶层 api_key/secret_key（HMAC 密钥）。现有账户配置无需增加文件。
若现货使用另一套密钥，在该账户 JSON 对象中增加：

```json
"spot": {
  "enabled": true,
  "api_key": "现货API Key",
  "secret_key": "现货Secret Key"
}
```

不要把此片段作为第二个 JSON 对象追加在文件末尾。无需现货时使用
`"spot": {"enabled": false}`。本适配器目前支持 HMAC，不支持 RSA/Ed25519。

如需指定本地 HTTP 代理，在同一账户配置增加
`"proxy": "http://127.0.0.1:7897"`，作用于该账户 REST 和两条私有流。
不配置时使用环境代理/系统网络路由。该字段不改变共享公共行情连接的代理设置。

## 数据

all_callbacks.jsonl / trade_callbacks.jsonl 路径和筛选规则不变。
新增外层 exchange、source、accountScope，原始 data 不改：

```json
{"exchange":"binance","source":"spot_stream","accountScope":"spot"}
```

PM 流按 fs 区分 um/cm，PM executionReport 标识为 pm_margin。
旧文件不改写，未提供来源的历史记录保持 unknown。
部分成交累积用账户/产品/交易对/系统订单ID组合隔离；ID包含关系配对和收益公式不变。
启动后的 Spot 推送不会自动补齐此前断线期间的成交。

连接失败、订阅成功、重连写入现有运行日志；终端分别显示 PM/Spot 连接状态。
仅拿到订阅成功确认才标记 Spot CONNECTED，不能用 REST 200 代替订阅成功。

接口依据：https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-api/user-data-stream

## 合计权益与终端（第二阶段）

每轮采集 PM actualEquity、普通 Spot balances（free + locked）及估值行情。
现货用 USDT 计价，USDC 也按市场价格转换；无价格的非零资产使本轮权益无效，不按零处理。
PM USD 通过账户配置 `pm_usd_to_usdt` 折算，默认 1.0，是明确的平价估值假设而非实时外汇。
实际损益 = 当前合计权益 - 同口径基准权益，未扣外部充值/提现。
PM 和 Spot 之间的划转无需另扣，但两接口非原子快照，划转瞬间可能短暂偏差。

09:30 换日后首个成功的完整采集建立基准，记录真实采集时间，不伪装成精确09:30。
重启恢复同口径基准；旧 PM 单独基准保留在 previousScopeBaseline，升级后首次完整采集建立新基准。
因此升级当天的合计实际损益从新基准时间计算，不代表整个交易日。
权益/基准缺失时 Excel 和邮件显示未就绪，不再用理论损益替代。

多账户配置支持一个顶层 `proxy`，例如 `http://127.0.0.1:7897`，供账户连接与共享行情复用。
单账户 proxy 可覆盖默认；多个账户使用不同代理时必须明确 market_data.proxy。
不配置时使用环境代理或现有网络路由，不自动猜测端口。
终端分资产表与连接表，MMR只显示整数，各连接状态独立；详细日志仅写文件，按09:30换桶。
