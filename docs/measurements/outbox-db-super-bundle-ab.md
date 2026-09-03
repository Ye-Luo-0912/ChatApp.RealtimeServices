# OUTBOX-DB-1 合并同事务写入 A/B（super-bundle 单条 CTE）

> 生成时间：2026-09-02 16:05:58 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息。

| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |
|---|---|---|---|---|---|
| A：合并（admission CTE + bundle，2 条数据语句） | 2 | 13.4 | 1.19 | 3,472 | 27.3 |
| B：super-bundle（合并为单条 CTE） | 1 | 9.5 | 0.96 | 4,755 | 37.0 |

> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅写入合并程度不同；
> 确定性收益是每次消息的数据路径 SQL −1（热路径 2 条 → 1 条）：B 把生产合并路径的 admission CTE
> （生命周期/授权/幂等 + 会话与 member 写入 + 序号分配）与 bundle CTE（message + outbox + ledger）
> 合并为单条语句，热路径数据往返 2 → 1；两路径写入行集完全相同（conversations / members / messages /
> outbox / command_idempotency_ledger 各 1 行），故 WAL 列预期接近，收益集中于往返削减；
> SQL 往返/消息含连接会话管理语句，短窗口下随连接池复用有 ±0.1 级波动，以热路径语句数为权威归因。

## A：合并热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.13 | 19.1 | 2,779 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_4da8d3180c…` |
| 2,000 | 1.0 | 0.13 | 4.0 | 353 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,093 | 1.0 | 0.00 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,512 | 1.3 | 0.00 | 0.0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 2,093 | 1.0 | 0.00 | 0.0 | 0 | `RESET ALL` |
| 2,094 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,507 | 1.3 | 0.00 | 0.0 | 0 | `COMMIT` |
| 2,093 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,095 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,095 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,095 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## A：合并（admission CTE + bundle）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 6,944,758 | 3,472 |
| WAL 记录 | 54,616 | 27.3 |
| WAL FPI | 333 | 0.17 |
| WAL write | 1,818 | 0.909 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 6A5B03D0C4125F4D | 2,000 | 263.0 | 2,000 | 0 | 603 | 38,233 | 0 | 5,559,500 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_4da8d3180cf9"."messages" (  …` |
| 7F8CBE236CAB3E90 | 2,000 | 256.3 | 2,000 | 0 | 0 | 8,097 | 0 | 707,384 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 1F1A5B390EB81DF3 | 1 | 209.6 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 64AB052FA5FE730A | 1 | 187.3 | 0 | 0 | 0 | 36 | 1 | 4,103 | `CREATE INDEX CONCURRENTLY "ix_conversation_members_user_pinned_list"     ON "rt_outbox_aud…` |
| 13FE8940C25F27BC | 1 | 36.4 | 3,615 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| C651AFCF55162FC2 | 6 | 27.1 | 1,032 | 0 | 0 | 7 | 0 | 418 | `SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid FROM (     -- A…` |
| 31EC5DFCEFD4401 | 34 | 26.2 | 34 | 0 | 19 | 646 | 0 | 114,670 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "realtime"."messages" (         me…` |
| 86D18C8B24474D63 | 1 | 13.4 | 0 | 0 | 0 | 5 | 0 | 336 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| E276F3FC63A3E3E2 | 1 | 12.4 | 0 | 0 | 0 | 6 | 0 | 394 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 3AC8235C2FFE7F1E | 1 | 12.1 | 0 | 0 | 0 | 7 | 0 | 546 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 4B40B1F5983D7496 | 1 | 11.2 | 0 | 0 | 0 | 6 | 0 | 392 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| E2FCD983F2E71AC4 | 1 | 10.1 | 0 | 0 | 0 | 5 | 0 | 340 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| EC37CE6C65CB7824 | 1 | 9.9 | 0 | 0 | 0 | 6 | 0 | 484 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 261466F1BD346818 | 1 | 9.8 | 0 | 0 | 0 | 6 | 0 | 488 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 168EFDB0902C7F21 | 1 | 9.0 | 0 | 0 | 0 | 4 | 0 | 278 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 9DFE0023C220BBE7 | 2,093 | 8.9 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| D6C944F452669EEE | 2,512 | 7.8 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| CA500FB662CCE79B | 1 | 7.6 | 0 | 0 | 0 | 70 | 1 | 7,098 | `CREATE TABLE IF NOT EXISTS "rt_group_amplification_send_38208ab0"."schema_migrations" (   …` |
| 10D79BC4EB4AA48 | 1 | 7.3 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_outbox_compact_publish_b6694435"."schema_migrations" (     …` |
| 7D7ED15418175A3B | 3 | 6.7 | 0 | 0 | 0 | 140 | 2 | 15,245 | `CREATE TABLE IF NOT EXISTS "rt_outbox_compact_publish_b6694435"."schema_migration_checkpoi…` |
| EC23FA6C96F40416 | 1 | 6.2 | 0 | 0 | 1 | 69 | 1 | 10,594 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_d8dfbe3b"."schema_migrations" (     "ve…` |
| C5A6730AE8C2011E | 1 | 6.1 | 0 | 0 | 2 | 70 | 1 | 11,000 | `CREATE TABLE IF NOT EXISTS "rt_outbox_reclaim_token_02e36e4e"."schema_migrations" (     "v…` |
| 55CD82040F8F95D3 | 1 | 5.8 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_ledger_txn_rollback_974a04a3"."schema_migrations" (     "ve…` |
| B9A3FC5813DDA531 | 2,093 | 5.7 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| D3F4539C46147179 | 34 | 5.3 | 34 | 0 | 0 | 139 | 0 | 21,814 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 20928A0A690DF580 | 2,094 | 5.1 | 2,094 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 642279687B67DC61 | 1 | 4.9 | 0 | 0 | 3 | 186 | 2 | 24,480 | `CREATE TABLE IF NOT EXISTS "rt_ledger_txn_rollback_974a04a3"."attachments" (     "attachme…` |
| 4E4C0FE91E523DFC | 1 | 4.9 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Expired_cbf8a6d5"."schema_migrations" (     "ver…` |
| D8C5F0CC2C23A63 | 3 | 4.8 | 0 | 0 | 1 | 144 | 2 | 15,645 | `CREATE TABLE IF NOT EXISTS "rt_group_amplification_send_38208ab0"."schema_migration_checkp…` |
| 4BBD11988980EF14 | 1 | 4.7 | 0 | 0 | 0 | 135 | 2 | 14,475 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Expired_cbf8a6d5"."relationship_change_log" (   …` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 6A5B03D0C4125F4D | 2,000 | 5,559,500 | 38,233 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_4da8d3180cf9"."messages" (  …` |
| 7F8CBE236CAB3E90 | 2,000 | 707,384 | 8,097 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 31EC5DFCEFD4401 | 34 | 114,670 | 646 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "realtime"."messages" (         me…` |
| C48B05E5B509A7DB | 1 | 110,148 | 1,006 | 0 | `INSERT INTO "rt_group_amplification_recall_3247ac18"."conversation_members" (     conversa…` |
| 96BF4DAEE4DE052F | 1 | 110,148 | 1,006 | 0 | `INSERT INTO "rt_group_amplification_send_38208ab0"."conversation_members" (     conversati…` |
| BAAC44745E436BA6 | 1 | 30,189 | 209 | 1 | `CREATE TABLE IF NOT EXISTS "rt_group_amplification_send_38208ab0"."relationship_projection…` |
| 4C410D9AFE93AD55 | 1 | 29,373 | 203 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_d8dfbe3b"."relationship_projection_rebu…` |
| 6B8B776097959158 | 1 | 27,385 | 171 | 2 | `CREATE TABLE IF NOT EXISTS "rt_group_amplification_send_38208ab0"."relationship_projection…` |
| E1B6DA6093623D4A | 1 | 26,963 | 170 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_d8dfbe3b"."relationship_projection_item…` |
| DDD8E68B7D471605 | 1 | 26,528 | 204 | 1 | `CREATE TABLE IF NOT EXISTS "rt_group_amplification_recall_3247ac18"."relationship_projecti…` |
| B326E3AE4A7C5566 | 1 | 26,039 | 206 | 1 | `CREATE TABLE IF NOT EXISTS "rt_ledger_txn_rollback_974a04a3"."relationship_projection_rebu…` |
| 19FC97319D6A28C4 | 1 | 25,737 | 201 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_crash_claim_0391e3fb"."relationship_projection…` |
| E6C59AF38678D09F | 1 | 25,497 | 201 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_compact_publish_b6694435"."relationship_projection_r…` |
| 26A432E0F37ABF72 | 1 | 25,481 | 201 | 1 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Expired_cbf8a6d5"."relationship_projection_rebui…` |
| 642279687B67DC61 | 1 | 24,480 | 186 | 2 | `CREATE TABLE IF NOT EXISTS "rt_ledger_txn_rollback_974a04a3"."attachments" (     "attachme…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| friendships | 0 | 0 | 0 | 0 | 0 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 2,000 | 0 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| outbox_replay_audit | 0 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 2,000 | 0 | 0 | 0 | 0 |
| message_reactions | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_snapshots | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_history | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_inbox | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |
| message_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| conversation_members | 0 | 2,000 | 0 | 2,000 | 13 |
| conversation_membership_periods | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| message_state | 0 | 0 | 0 | 0 | 0 |
| relationship_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| schema_migrations | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：conversations 2,000/2,000（100%）；conversation_members 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：super-bundle 热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.28 | 24.2 | 3,300 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,028 | 1.0 | 0.00 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,028 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,028 | 1.0 | 0.00 | 0.0 | 0 | `RESET ALL` |
| 2,028 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,028 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,028 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,028 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## B：super-bundle（合并同事务写入为单条 CTE）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 9,511,708 | 4,756 |
| WAL 记录 | 73,931 | 37.0 |
| WAL FPI | 381 | 0.19 |
| WAL write | 2,403 | 1.202 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| F34F8BE5D519327F | 2,000 | 550.2 | 2,000 | 0 | 632 | 48,427 | 0 | 6,601,417 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 1F1A5B390EB81DF3 | 1 | 209.3 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 3D477F44F9E6752B | 1 | 56.7 | 0 | 0 | 0 | 36 | 1 | 4,103 | `CREATE INDEX CONCURRENTLY "ix_conversation_members_user_pinned_list"     ON "rt_lifecycle_…` |
| C651AFCF55162FC2 | 6 | 40.5 | 1,032 | 0 | 0 | 27 | 0 | 1,530 | `SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid FROM (     -- A…` |
| 13FE8940C25F27BC | 1 | 29.6 | 3,694 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 8D745B6662C17598 | 1 | 13.9 | 0 | 0 | 0 | 10 | 0 | 814 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 4AD2DE504CCD2597 | 1 | 13.1 | 0 | 0 | 0 | 5 | 0 | 426 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 53F8AC5100B11537 | 1 | 12.9 | 0 | 0 | 0 | 5 | 0 | 336 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 183A58B7E580281B | 1 | 12.4 | 0 | 0 | 0 | 20 | 1 | 2,148 | `CREATE INDEX IF NOT EXISTS "ix_messages_forwarded_from" ON "rt_outbox_pending_index_clean…` |
| A0C75BF13E3F2974 | 1 | 11.4 | 0 | 0 | 0 | 6 | 0 | 492 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| F9531E415772CEFA | 3 | 11.2 | 0 | 0 | 0 | 152 | 2 | 16,387 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Ticketed_3f8223df"."schema_migration_checkpoints…` |
| 31B355BFE1AD9187 | 1 | 10.9 | 0 | 0 | 0 | 5 | 0 | 426 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| A21E5D1FCD87441A | 3 | 9.1 | 0 | 0 | 1 | 145 | 2 | 19,609 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_fbaf1b7c"."schema_migration_ch…` |
| 9DFE0023C220BBE7 | 2,028 | 8.3 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 46D1DC8BA3B33123 | 1 | 7.9 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_fbaf1b7c"."schema_migrations" …` |
| 1C97027C18F8E3AE | 1 | 7.7 | 0 | 0 | 0 | 69 | 1 | 7,040 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_missing_f9bf7984"."schema_migrations" (     "v…` |
| FCDB34D84707E19C | 1 | 7.1 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Ticketed_3f8223df"."schema_migrations" (     "ve…` |
| 3C73A4C804597180 | 6 | 6.5 | 0 | 0 | 0 | 2 | 0 | 112 | `-- Load field definitions for (free-standing) composite types SELECT typ.oid, att.attname,…` |
| 2C6C296E1832DDD5 | 1 | 6.4 | 0 | 0 | 1 | 145 | 2 | 15,699 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_deleted_rejects_writes_a6a051f8"."message_mutatio…` |
| 4E4A1368A2ADEC69 | 1 | 6.3 | 1 | 5 | 11 | 12 | 0 | 1,090 | `INSERT INTO "rt_voice_bind_Uploaded_c37fda5a"."attachments" (     attachment_id, uploader_…` |
| 1E5A15324DD82797 | 1 | 6.3 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_shared_concurrent_dd49e375"."schema_migrations" (…` |
| A45E185DB4D1A6B8 | 1 | 5.7 | 0 | 0 | 4 | 141 | 2 | 20,068 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_shared_concurrent_dd49e375"."messages" (     "me…` |
| 4F98BFD6E61A82E4 | 1 | 5.7 | 0 | 0 | 1 | 68 | 1 | 6,984 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_c37fda5a"."schema_migrations" (     "ve…` |
| AA316D208DE9A1EB | 3 | 5.6 | 0 | 0 | 2 | 143 | 2 | 19,087 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_c37fda5a"."schema_migration_checkpoints…` |
| 36C71B8AA31A14A2 | 1 | 5.1 | 0 | 0 | 2 | 169 | 2 | 18,933 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_c37fda5a"."relationship_projection_item…` |
| E8BF85A8E03F125D | 1 | 5.1 | 0 | 0 | 1 | 146 | 2 | 16,070 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_fbaf1b7c"."outbox" (     "eve…` |
| 20928A0A690DF580 | 2,028 | 5.0 | 2,028 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| A0A76E17B6D0F16B | 1 | 5.0 | 1 | 6 | 13 | 14 | 0 | 1,226 | `INSERT INTO "rt_voice_bind_Ticketed_3f8223df"."attachments" (     attachment_id, uploader_…` |
| 25612453ACEED702 | 1 | 4.8 | 0 | 0 | 3 | 183 | 2 | 24,450 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_c37fda5a"."attachments" (     "attachme…` |
| E046DF963A6C2210 | 1 | 4.8 | 0 | 0 | 2 | 153 | 2 | 19,156 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_fbaf1b7c"."friend_requests" ( …` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| F34F8BE5D519327F | 2,000 | 6,601,417 | 48,427 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 1580C8F0ACADCF40 | 1 | 30,125 | 204 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_missing_f9bf7984"."relationship_projection_reb…` |
| 2EA67751069560F6 | 1 | 27,147 | 219 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_fbaf1b7c"."relationship_projec…` |
| A5907600E9458089 | 1 | 26,243 | 218 | 1 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_c37fda5a"."relationship_projection_rebu…` |
| A7B84C3177EECF28 | 1 | 25,637 | 201 | 1 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_deleted_rejects_writes_a6a051f8"."relationship_pr…` |
| 9D665F6388A4D53A | 1 | 24,764 | 201 | 1 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Ticketed_3f8223df"."relationship_projection_rebu…` |
| E7CD50B02E1A6415 | 1 | 24,747 | 188 | 2 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_shared_concurrent_dd49e375"."attachments" (     "…` |
| 25612453ACEED702 | 1 | 24,450 | 183 | 2 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_c37fda5a"."attachments" (     "attachme…` |
| 2F3B97419516F5FC | 1 | 24,142 | 180 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_missing_f9bf7984"."attachments" (     "attachm…` |
| FE760D8365EF6933 | 1 | 23,183 | 169 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_missing_f9bf7984"."relationship_projection_ite…` |
| 88B112170A78B1ED | 1 | 22,664 | 146 | 2 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_shared_concurrent_dd49e375"."outbox" (     "even…` |
| 3DEB9E703BFB256A | 1 | 22,497 | 169 | 2 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Ticketed_3f8223df"."relationship_projection_item…` |
| 95365CA51C5334DC | 1 | 22,413 | 171 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_fbaf1b7c"."relationship_projec…` |
| 5916446FF8EB54E2 | 1 | 21,478 | 190 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_fbaf1b7c"."relationship_projec…` |
| 58E0B34B71F43C26 | 1 | 21,455 | 193 | 2 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_c37fda5a"."relationship_projection_hist…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 1,998 | -8 |
| conversation_members | 0 | 2,000 | 0 | 1,999 | -24 |
| device_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| attachments | 0 | 0 | 0 | 0 | 0 |
| message_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| message_reactions | 0 | 0 | 0 | 0 | 0 |
| group_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| user_deletion_tombstones | 0 | 0 | 0 | 0 | 0 |
| command_idempotency_ledger | 2,000 | 0 | 0 | 0 | 0 |
| group_operation_audit | 0 | 0 | 0 | 0 | 0 |
| conversation_membership_periods | 0 | 0 | 0 | 0 | 0 |
| account_cleanup_jobs | 0 | 0 | 0 | 0 | 0 |
| message_state | 0 | 0 | 0 | 0 | 0 |
| friend_requests | 0 | 0 | 0 | 0 | 0 |
| friendships | 0 | 0 | 0 | 0 | 0 |
| relationship_mutation_requests | 0 | 0 | 0 | 0 | 0 |
| relationship_sync_cursors | 0 | 0 | 0 | 0 | 0 |
| relationship_change_log | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_versions | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_items | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_inbox | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_snapshots | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_rebuild_state | 0 | 0 | 0 | 0 | 0 |
| relationship_projection_history | 0 | 0 | 0 | 0 | 0 |
| outbox_replay_audit | 0 | 0 | 0 | 0 | 0 |

> HOT 命中率（hot_updates / updates）：conversations 1,998/2,000（100%）；conversation_members 1,999/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

