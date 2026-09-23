# 只读实时监控台

原有采集、JSONL、Excel 和邮件不变。网页后端是独立进程，盘口接收再按 Spot / UM 分进程；网页进程异常不停止分析服务。

```text
原采集 ── trade_callbacks.jsonl ── 原回放/Excel/邮件
                    └── 只读游标 ── 内存配对/Exposure
原采集 ── all_callbacks.jsonl ── 内存挂单（REST校验）
Binance depth20 ── 独立接收 ── 最新15档内存快照
                                  │
                           web/app.py 只读接口
                                  │ HTTPS / WSS
                       本地网页 / Vercel 静态前端
```

## 启用：仍然一个命令

在现有 `config/accounts.local.json` 顶层增加以下 `web`，保留其他配置。不增加账户配置文件，不改 API 密钥：

```json
"web": {
  "enabled": true,
  "host": "127.0.0.1",
  "port": 8080,
  "allowed_origins": [],
  "users": [
    {"username": "viewer", "password_env": "MONITOR_VIEWER_PASSWORD", "accounts": ["zdl", "mfx", "dh"]}
  ]
}
```

用户的 `accounts` 必须是实际已启用账户 ID 的子集，可以配置多个网页用户。缺少web或enabled=false时不启动网页服务。

Windows PowerShell，在 `python_binance` 目录：

```powershell
$env:MONITOR_VIEWER_PASSWORD = ([System.Net.NetworkCredential]::new("", (Read-Host "设置网页登录密码（至少16字符）" -AsSecureString))).Password
uv run binance-monitor
```

打开 <http://127.0.0.1:8080>，后端地址填 `http://127.0.0.1:8080`，使用viewer和刚设置的密码登录。此密码不是 Binance API Secret。

会话有效8小时，仅存浏览器内存，刷新需重新登录。后台重启后旧会话失效。密码只从环境变量读取。
公网远程必须使用 HTTPS，禁止把无TLS的8080直接公开。可信局域网可按下节使用HTTP。最多100会话、20条浏览器连接；登录尝试全服务每分钟限20次。

## 可信局域网访问

设置 `web.host` 为 `0.0.0.0` 并重启原监控命令。其他设备直接打开 `http://运行程序电脑的局域网IP:8080`，后端地址自动使用当前网址，保留原网页登录验证。
HTTP仅允许私有IPv4（10/8、172.16/12、192.168/16）及本机地址的同源页面；不要从Vercel的HTTPS页面连接局域网HTTP。无需修改allowed_origins为通配符。

Windows防火墙需允许TCP 8080，建议限定实际网卡、服务器局域网地址和本地网段，不关闭整个防火墙，不配置路由器端口映射。若网卡被标记为公用，可建立仅针对该网卡/地址/网段的规则，不必将整个网络改为专用。
局域网HTTP明文传输网页登录凭证和账户数据，只用于可信网络。IP或网段变化后需同步更新访问地址和防火墙规则。

## 页面和计算口径

- 两侧独立选择 Spot / Margin 或 UM、交易对，支持现货—现货、现货—期货、期货—期货。
- 每市场最多100个订阅品种，共享同市场同交易对盘口；订阅20档、展示买卖各15档。只保留内存，不增加raw文件。
- 叠加当前账户挂单，来源区分Spot、PM Margin、UM；档外订单仍在挂单列表中。
- 最多250ms推送一次最新快照，慢客户端超时断开；行情超过3秒无更新标记过期。
- 每500ms检查JSONL尾部，新成交增量处理，乱序重建。无变化复用结果，不重建Excel。分页列表每页50条，仅在账户、页码或计算版本变化后请求（每2秒兜底检查）。
- 网页使用协议v2：同一条WebSocket内切换账户/交易对；只传变化的数据分区，不随盘口重复发送成交原文。最多一个未确认快照，客户端处理并确认后才发送最新状态，慢链路不排队补发历史盘口。上限4次/秒，实际频率受往返耗时影响；30秒未确认断线重连。旧客户端仍兼容原快照协议。
- 顶部显示“往返含渲染”耗时，包含网络及浏览器处理，不代表交易所到本机延迟。盘口底部显示采集时间。
- 普通配对显示双方订单ID、系统ID、方向、成交价格、分摊手续费、配对数量、收益及挂单价差/成交价差。价差为原始比值（非百分数），缺失显示破折号；不修改配对和收益计算。
- 更新后重启监控服务并强制刷新网页；若使用Vercel独立前端，还需重新部署前端。
- 权益沿用原PM+Spot状态和09:30基准；采集状态15秒未更新警告。MMR只显示整数。
- 交易损益＝普通配对收益＋Exposure收益；普通配对数不含Exposure，未匹配条数取Exposure消耗前的条数。
- 页面收益是实时暂算。ID匹配、超时、手续费均沿用LegacyMatcher，没有修改原算法。
- 每小时从同一JSONL字节边界独立重建并比较；有差异切换到重建结果，不覆盖正式matches/unmatched等文件。
- 重启恢复当前09:30交易日；不补交易所漏推。未写完的最后一行等待补完，坏行报错而不静默跳过。
- 当前挂单由回调更新、每60秒REST校验。日报下载只读已生成xlsx，不触发重算；尚未生成时404。
- 首版盘口支持Binance Spot/PM Margin和UM，不含CM、Hyperliquid。

内存配对随当日成交量增长，09:30释放上一日状态。大量成交下恢复/核验需实测，不承诺固定毫秒延迟。
盘口、挂单、权益不是交易所同一时刻的原子快照。正式报表与暂算可能因统计时点不同暂时不一致。

## Vercel部署

**只部署 `web/frontend`，不要上传config、runtime、output、log或整个项目。**

1. Root Directory选 `python_binance/web/frontend`（单独上传前端时选该目录本身），Framework选Other，无需npm构建，已提供vercel.json。
2. Python仍在常驻电脑/VPS运行；通过有效证书的反向代理提供HTTPS域名，转发到127.0.0.1:8080。代理须支持WebSocket Upgrade，空闲超时大于60秒。
3. web.allowed_origins增加实际前端域名，如 `https://your-monitor.vercel.app`，精确匹配，不使用通配符。
4. 网页后端地址填HTTPS域名，浏览器直连WSS。Vercel只托管页面，不运行采集进程、不接收Binance密钥。
5. 后端系统时区设Asia/Shanghai，确保09:30与原程序一致，并保持系统校时。

本次未创建公网域名、开放防火墙或执行Vercel发布。正式外网启用前需验证HTTPS、账户授权、反向代理访问控制与带宽。

## 目录

| 文件 | 职责 |
| --- | --- |
| realtime/engine.py | 字节游标、增量配对、恢复和核验 |
| realtime/orders.py | 挂单来源隔离和REST/回调竞争处理 |
| realtime/depth.py | 公共盘口接收和快照 |
| web/app.py | 登录、权限、推送、分页、下载 |
| web/frontend/ | 无构建静态前端 |
| service/web_service.py | 可选独立进程监督 |

网页仅提供真实账户登录和监控，需要连接运行中的后端。

测试：`python -m unittest discover -s tests -p test_realtime_dashboard.py -v`。
