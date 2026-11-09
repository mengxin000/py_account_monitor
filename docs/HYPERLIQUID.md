# Hyperliquid永续与Binance组合监控

启动命令不变：`uv run binance-monitor`。
多账户文件仍然是`config/accounts.local.json`，其`accounts`继续引用各单账户配置。
旧版单账户配置保持兼容；本功能不会自动替换任何正在使用的凭证。

不同监控账户可以同时使用不同组合（例如 zdl 为 Binance Spot + Hyperliquid，mfx 为 Binance Spot + USDM，dh 为 PM Margin + PM UM），各自写入独立运行目录，邮件依旧汇总各自 Excel。凭证、目录冲突检查和显式环境变量配置见 README 的“多账户独立组合”。公共 `BINANCE_*` 环境变量不再自动覆盖每份账户配置。

## 单账户配置

在原来的单账户文件中，使用`legs`选择组合。例如Binance普通现货 + Hyperliquid统一账户永续：

```json
{
  "account_id": "dh",
  "output_dir": "runtime/dh",
  "rest_interval_seconds": 5,
  "proxy": "http://127.0.0.1:7897",
  "legs": {
    "binance": {
      "exchange": "binance",
      "market": "spot",
      "credentials": {
        "api_key": "填写币安只读API_KEY",
        "secret_key": "填写对应凭证",
        "key_type": "auto"
      }
    },
    "hedge": {
      "exchange": "hyperliquid",
      "market": "perp",
      "address": "0x填写实际主账户或子账户的40位地址",
      "usdc_to_usdt": 1.0,
      "symbol_map": { 
        "xyz:QQQ": "QQQUSDC"
      }
    }
  }
}
```

地址必须是实际账户地址，不是API agent钱包地址。Hyperliquid只读监控不需要私钥。
目前Hyperliquid账户模式支持`unifiedAccount`；如果实际模式不同，权益明确报错而非按零统计。
所有`legs`配置优先于旧的`spot`、`futures`、`hyperliquid`产品选择。
`legs`支持最多两条腿，同一个监控账户最多一个Hyperliquid地址。
代理字段可省略；也可继续由多账户配置提供共享代理。

币安侧`market`可选：

| market | 连接 |
| --- | --- |
| spot | 普通现货 |
| pm_margin | PM Margin现货 |
| usdm | 普通USDⓈ-M永续 |
| pm_um | PM UM永续 |

Binance永续 + Hyperliquid永续：将上例币安腿`market`改为`usdm`或`pm_um`，使用对应账户的凭证。

换回Binance现货 + Binance永续：保留币安腿，将`hedge`改为：

```json
{
  "exchange": "binance",
  "market": "usdm",
  "credentials": {
    "api_key": "填写币安合约只读API_KEY",
    "secret_key": "填写对应凭证",
    "key_type": "auto"
  }
}
```

也可以恢复原来的旧版账户配置。修改连接配置后重启生效。
组合、Hyperliquid账户地址或折算汇率改变时，同一交易日会重新建立权益基准，并保留旧基准供核对。
跨09:30按原有规则建立新交易日目录和权益基准。

## 订单关联、数量与收益

- Binance内部配对保留原有ID包含关系。
- 跨平台配对：动态取得币安数字orderId长度N，与Hyperliquid cloid去掉`0x`后的前N个字符比较。
- cloid应是`0x`加32个十六进制字符，币安orderId的十进制数字原样嵌入前缀，不能将整个ID转为十六进制再比较。
- 匹配仍按同一账户、同一标的、相反方向进行；发现多个币安ID命中同一个cloid前缀时停止本轮计算并报错。
- Hyperliquid每笔fill独立作为成交片段参与配对，按`coin+oid+time+tid`去重。一个订单的多笔fill可能生成多条配对记录，数量和手续费分别分摊。
- cloid优先从fill/订单推送缓存取得，缺失时查询`orderStatus`；没有cloid的成交保留进入Exposure。
- `closedPnl`不是套利收益。套利收益继续用卖出金额－买入金额－双方分摊手续费。
- `builderFee`已包含在Hyperliquid的`fee`中，不重复扣除；负手续费按返佣处理。
- 现货/永续价差沿用`(永续价－现货价)/永续价`；跨平台永续/永续使用`(Binance价－Hyperliquid价)/Binance价`。
- 普通币名如`BTC`自动规范成`BTCUSDC`；`xyz:QQQ`默认去掉`xyz:`前缀，规范成`QQQUSDC`，无需单独配置。显式`symbol_map`优先，其他DEX前缀仍需配置。原生名称保留用于行情订阅；底层标的别名仍从`config/underlying_alias.json`热加载。

## 持久化与恢复

文件目录和发送邮件流程不变：

```text
runtime/dh/YYYYMMDD/
  trade_callbacks.jsonl
  all_callbacks.jsonl
  matches/<标的>.jsonl
  unmatched.jsonl
  exposure_matches.jsonl
  exposure_remain.jsonl
  funding.jsonl
  equity.json
```

Hyperliquid记录增加`exchange=hyperliquid`、`accountScope=hyperliquid`、`eventId`和`normalizedOrder`。
`data`保留原始fill或订单更新，`normalizedOrder`供回放/实时计算使用。
Hyperliquid数据按交易所事件时间归属09:30交易日，接收时间另存为微秒`receivedTimeUs`。
重连后重新订阅，并查询当前交易日的历史成交与资金费用，和落盘记录去重。
历史接口本身有最多可查成交条数等限制，不能承诺恢复任意长时间停机前的数据；分页无法继续时明确报错。
未开启Hyperliquid的账户不会连接其REST或WebSocket。

Hyperliquid资金费用通过`userFundings`推送保存，同时在启动/重连和每小时05秒进行REST核对。
Binance原有00:05、08:05、16:05任务不变。两者资金费用写入同一账户的`funding.jsonl`，通过交易所及事件身份去重。

## 权益与计价

Hyperliquid统一权益采用`spotClearinghouseState`统一资产余额估值，加上各永续DEX持仓未实现损益；不重复加入各DEX的`marginSummary.accountValue`。
持仓状态优先使用`allDexsClearinghouseState`推送，状态过期30秒后由REST核对。
MMR按抵押币汇总跨DEX维持保证金，除以统一余额减逐仓占用，显示为整数百分比。
任一必需账户组件查询失败，总权益和实际损益显示未就绪，保留原有基准。
总权益=选中的Binance账户权益+Hyperliquid统一权益；实际损益=当前总权益－当日组合基准。
充值、提现和跨平台转账仍未从实际损益中剔除，沿用原程序口径。

默认USDC按1 USDT折算。`usdc_to_usdt`是固定配置值，不是实时汇率；会记录到新成交用于回放。
其他手续费币种必须提供`token_rates`的USDT折算率；缺失时明确报错。
本次未修改原有Binance BNB手续费折算逻辑，该已知问题仍需独立修复。

## 网页与行情

网页的两条交易腿下拉菜单可选择“Hyperliquid 永续”。
Hyperliquid盘口输入原生coin，例如`BTC`或`xyz:QQQ`，不要输入币安式`BTCUSDT`。
支持15档快照、挂单标记、成交、普通配对与Exposure查看。
Hyperliquid BBO采集沿用共享市场进程、30秒缓存、成交前后窗口、按小时压缩和保留期限；落盘位于`runtime/raw/YYYYMMDD/hyperliquid/`。
HIP-3盘口的冒号在Windows目录中编码为`%3A`。

## 官方接口

- [Info接口](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
- [永续账户和资金费用](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)
- [统一账户余额](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot)
- [WebSocket订阅与数据结构](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
- [统一账户模式与风险比例](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/account-abstraction-modes)

连接默认使用主网`https://api.hyperliquid.xyz/info`和`wss://api.hyperliquid.xyz/ws`。
未使用真实Hyperliquid账户进行在线验证；上线前需用实际账户检查回调、权益和映射。
