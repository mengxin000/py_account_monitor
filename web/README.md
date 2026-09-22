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
远程必须使用 HTTPS，禁止把无TLS的8080直接公开。最多100会话、20条浏览器连接；登录尝试全服务每分钟限20次。

## 页面和计算口径

- 两侧独立选择 Spot / Margin 或 UM、交易对，支持现货—现货、现货—期货、期货—期货。
- 每市场最多100个订阅品种，共享同市场同交易对盘口；订阅20档、展示买卖各15档。只保留内存，不增加raw文件。
- 叠加当前账户挂单，来源区分Spot、PM Margin、UM；档外订单仍在挂单列表中。
- 最多250ms推送一次最新快照，慢客户端超时断开；行情超过3秒无更新标记过期。
- 每500ms检查JSONL尾部，新成交增量处理，乱序重建。无变化复用结果，不重建Excel。分页列表每2秒刷新、每页50条。
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

离线预览（全部模拟数据，无账户读取）：

```powershell
python -m http.server 8765 --bind 127.0.0.1 --directory web/frontend
```

打开 <http://127.0.0.1:8765/?demo=1>。仅用于开发预览，生产不需额外运行此命令。

测试：`python -m unittest discover -s tests -p test_realtime_dashboard.py -v`。
