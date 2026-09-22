# 公共行情采集时序

服务仍然使用原来的单命令启动。Spot/Futures 各有一个按需启动的接收子进程，
同市场所有交易对共享一个动态订阅连接。凭证不传入公共行情子进程。

每次 WebSocket receive 返回后立即记录：
- receivedTimeUs：本机 Unix 微秒时间。
- receivedMonoNs：本机单调时钟，用于内部排队耗时。
- connectionId / receiveSequence：重连隔离和消息序号（包括订阅回复，不能把序号间隔直接解释为行情丢失）。
- rawPayload：未重新序列化的原始 TEXT 消息字符串；这是应用消息，不是包含 TLS/WebSocket 帧头的网络字节。
- clockSyncStatus=unverified：不声称本机已与交易所精确对时。

入队后主进程解析、筛选成交窗口并批量写盘，每批最多256条。
processedMonoNs - receivedMonoNs 可用于观察接收至处理的等待时间。
receivedTimeMs 保留兼容旧分析；交易所 E/T 保持原始值，不人为提高精度。
原始消息仅随选中的行情窗口保存，不新增全天全量行情存储。

目录和保留规则不变：runtime/raw/YYYYMMDD/spot|futures/SYMBOL/HH.jsonl。
按接收时间归桶；过小时后台压缩到 HH.jsonl.gz.tmp，再原子重命名为 HH.jsonl.gz。
临时 gzip 文件不是分析输入，不需拼接小时文件。晚到记录允许追加 gzip member。

IPC 队列默认每市场50000条，可在 market_data.ingress_queue_max 修改。
满时丢弃新消息而不阻塞接收，ingressDroppedTotal 和 windows.jsonl 的 ingress_overflow 标记缺口。
现有写盘队列仍丢弃旧行情，archive_overflow 记录缺口。
这不是无损采集承诺：分析延迟时应排除溢出、重连和未确认时钟的区间。
Ctrl+C 尽力排空；超过3秒仍无法退出会终止接收进程并记录可能丢失的告警。

报表阻塞不再直接阻塞接收子进程，但可能延迟主进程的解析和成交窗口触发。
本地时间仍是应用层消费时刻，而非网卡收包时刻；代理、操作系统缓冲及本机时钟误差仍存在。
