# OUTBOX-DB-1 fillfactor A/B（排水路径 WAL）

> 生成时间：2026-09-03 22:50:05 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息、排水 2000 条。

| 配置 | claim WAL/消息 | complete WAL/消息 | 排水合计 WAL/消息 |
|---|---|---|---|
| fillfactor=90（A） | 1,105 | 957 | 2,062 |
| fillfactor=75（B） | 916 | 849 | 1,765 |

> 以 pg_stat_statements 语句级 wal_bytes / rows 归因，A/B 同容器顺序运行，仅 fillfactor 不同。
