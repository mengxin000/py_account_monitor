# 标准订单与关联规则表

现有 `MatchOrder` 和 `MatchRecord` 不替换，`id` 仍为 CID，`system_id` 仍为 SID。
新增可选 `reference_sid`（默认空字符串），用于显式指向另一笔订单，不能覆盖订单自身 ID。
Hyperliquid 适配器从 `0xb<SID>b<后缀>` 生成此字段；历史 JSONL 缺少字段时在内存中解析，原文件不改写。

## 规则表

`core/order_linking.py` 的只读 `RULE_TABLE` 按无序平台组合索引，双向共用一个条目：

| 组合 | 规则 |
|---|---|
| Binance / Binance | 原有四种 CID/SID、CID/CID 包含关系 |
| Binance / OKX | 同上 |
| OKX / OKX | 同上；交易程序必须遵守关联 ID 约定 |
| Binance / Hyperliquid | Hyper 引用 SID 完整匹配另一边 SID |
| OKX / Hyperliquid | 同上，Hyper 必须编码 OKX ordId |
| 未配置组合（包括 Hyper / Hyper） | 不做普通关联匹配 |

规则函数只返回 bool，不更改订单、不扣数量、不算收益。原有 account_id、方向和 exposure 标记限制继续保留；同标的分组仍由回放层和实时引擎完成。
此表选择的是关联规则，不是队列优先级。默认映射继续在代码中维护，不新增账户配置文件。
内部支持可选的账户级 `matching_rules` 嵌套映射（交易所→另一交易所→规则名称），主服务会独立传入回放和实时投影，避免全局串用。
不配置时使用上表；显式配置时替换默认表，缺省单元不做普通配对，空对象禁用全部普通关联匹配（不禁用 Exposure）。
可用规则为 `contains_ids`、`references_sid`、`disabled`，双向只写一次；冲突或拼写错误启动时拒绝。配置不热加载。

## 歧义处理

分配数量前检查候选。订单身份保留 exchange/account_id/account_scope/market_type/symbol/SID；不把 execution_id 当成不同订单。
同一订单多个成交片段正常配对；多个对冲订单显式引用同一原始 SID 也允许逐次消耗。
如果一个包含表达式意外命中多个不同原始订单（如 `hedge1234` 同时命中 SID 123 和 1234），抛出 `AmbiguousOrderLink`，该 matcher 停止后续普通配对及 Exposure。
选择失败即停止本轮计算，而不是新增隔离报表格式：回放在写输出文件之前失败，因此旧的衍生文件保留。主服务会记录该账户报告失败，不会把它伪装成完整的新报告。
已经过期移出普通队列的历史订单不在本次候选检查范围内；该检查不声称能判断交易程序未编码的真实关联。

## OKX 标准化边界

新增 `collectors/okx/normalize.py::normalize_fill`，输入 REST fills-history **逐笔**记录、同合约产品规格、显式币种估值，返回 MatchOrder 字段。
支持 USDT/USDC 计价的 SPOT/MARGIN，以及面值按标的币计量、ctMult 为 1 的线性 SWAP/FUTURES。
现货数量保留；合约张数乘 ctVal；费用按原币种估值后取反（成本为正，返佣为负）。第三币种必须提供汇率，不能乘当前交易对价格。原生币手续费可按该笔成交价换算。
反向合约、非单位 ctMult、缺失规格/汇率明确报错。标准化成交带 billId execution_id，匹配器据此对逐笔成交去重；Binance 原有累计成交处理保持不变。

**OKX 主服务接入使用 REST 逐笔 fills 作为唯一配对源，不把 WS 累计 orders 相加。** 详见 [OKX.md](OKX.md)。利润公式及 Excel/邮件布局不变。

测试：`python -B -m unittest discover -s tests -p test_order_linking.py -v`。
