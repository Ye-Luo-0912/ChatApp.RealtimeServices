# OUTBOX-DB-1 合并同事务写入 A/B（super-bundle 单条 CTE）

> 生成时间：2026-09-03 22:49:55 UTC；语料固定、随机种子 20260813；每配置 inbound 2000 条消息。

| 配置 | 热路径 SQL/消息 | SQL 往返/消息 | 总执行时间/消息(ms) | WAL 字节/消息 | WAL 记录/消息 |
|---|---|---|---|---|---|
| A：合并（admission CTE + bundle，2 条数据语句） | 2 | 12.9 | 0.94 | 3,834 | 29.6 |
| B：super-bundle（合并为单条 CTE） | 1 | 9.3 | 0.75 | 3,310 | 25.4 |

> 以 pg_stat_statements 语句级 calls/wal_bytes 归因，A/B 同容器顺序运行、仅写入合并程度不同；
> 确定性收益是每次消息的数据路径 SQL −1（热路径 2 条 → 1 条）：B 把生产合并路径的 admission CTE
> （生命周期/授权/幂等 + 会话与 member 写入 + 序号分配）与 bundle CTE（message + outbox + ledger）
> 合并为单条语句，热路径数据往返 2 → 1；两路径写入行集完全相同（conversations / members / messages /
> outbox / command_idempotency_ledger 各 1 行），故 WAL 列预期接近，收益集中于往返削减；
> SQL 往返/消息含连接会话管理语句，短窗口下随连接池复用有 ±0.1 级波动，以热路径语句数为权威归因。

## A：合并热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.11 | 19.1 | 2,779 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_32c13fb0c6…` |
| 2,000 | 1.0 | 0.10 | 4.0 | 353 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,026 | 1.0 | 0.00 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,426 | 1.2 | 0.00 | 0.0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 2,026 | 1.0 | 0.00 | 0.0 | 0 | `RESET ALL` |
| 2,026 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,423 | 1.2 | 0.00 | 0.0 | 0 | `COMMIT` |
| 2,026 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,026 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,026 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,026 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## A：合并（admission CTE + bundle）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 7,668,681 | 3,834 |
| WAL 记录 | 59,149 | 29.6 |
| WAL FPI | 100 | 0.05 |
| WAL write | 2,321 | 1.161 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| E5AFA989139E6561 | 2,000 | 215.7 | 2,000 | 0 | 602 | 38,237 | 0 | 5,559,264 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_32c13fb0c603"."messages" (  …` |
| 1F1A5B390EB81DF3 | 1 | 209.0 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 573DCDB8A2FCC27A | 2,000 | 197.1 | 2,000 | 0 | 0 | 8,097 | 0 | 707,424 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 56E20AFCD7439C9D | 1 | 145.3 | 0 | 0 | 0 | 36 | 1 | 4,103 | `CREATE INDEX CONCURRENTLY "ix_conversation_members_user_pinned_list"     ON "rt_voice_bind…` |
| C49C71344BD7CF03 | 1 | 38.8 | 2,047 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| C651AFCF55162FC2 | 6 | 20.3 | 1,032 | 0 | 0 | 5 | 0 | 310 | `SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid FROM (     -- A…` |
| 5AE1ADAF6A86BFB0 | 1 | 10.8 | 0 | 0 | 0 | 6 | 0 | 484 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 4791C451E8498C72 | 1 | 10.7 | 0 | 0 | 0 | 6 | 0 | 398 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 6295D2061B54B3EB | 1 | 10.5 | 0 | 0 | 0 | 6 | 0 | 488 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| E7A2DE41789F1387 | 1 | 10.0 | 0 | 0 | 0 | 6 | 0 | 400 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 67DFCFC8592AA202 | 1 | 9.4 | 0 | 0 | 0 | 6 | 0 | 490 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| E49D0622BE025D32 | 1 | 9.0 | 0 | 0 | 0 | 4 | 0 | 278 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 985402309A5DC9F | 1 | 8.7 | 0 | 0 | 0 | 44 | 0 | 4,023 | `ALTER TABLE "rt_outbox_reclaim_token_47b4ad33"."message_state" ADD CONSTRAINT fk_message_s…` |
| 6690771C9A2BDCAA | 1 | 8.4 | 0 | 0 | 0 | 4 | 0 | 278 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 9DFE0023C220BBE7 | 2,026 | 6.8 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| D6C944F452669EEE | 2,426 | 5.8 | 0 | 0 | 0 | 0 | 0 | 0 | `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| 7954E31A88255C7D | 1 | 5.7 | 0 | 0 | 0 | 14 | 0 | 1,458 | `DROP INDEX CONCURRENTLY IF EXISTS "rt_outbox_audit_list_2c86d521"."ix_outbox_pending_attem…` |
| 4AF876927F98B980 | 1 | 5.4 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_outbox_reclaim_token_47b4ad33"."schema_migrations" (     "v…` |
| B9A3FC5813DDA531 | 2,026 | 5.2 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 585290785E79A0DA | 1 | 5.0 | 0 | 0 | 0 | 25 | 1 | 2,897 | `CREATE INDEX CONCURRENTLY "ix_outbox_pending_created" ON "rt_ledger_txn_rollback_84080684"…` |
| A7093EFE9C300A14 | 1 | 4.9 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_ledger_txn_rollback_84080684"."schema_migrations" (     "ve…` |
| DE8D27C9D8E5DD27 | 1 | 4.9 | 0 | 0 | 0 | 68 | 1 | 6,986 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_list_2c86d521"."schema_migrations" (     "vers…` |
| 9BA3EBEDB5073110 | 1 | 4.8 | 1 | 5 | 11 | 11 | 0 | 1,026 | `INSERT INTO "rt_voice_bind_Scanning_4a0c9032"."attachments" (     attachment_id, uploader_…` |
| 26AA63265F16757C | 1 | 4.7 | 0 | 0 | 1 | 69 | 1 | 7,746 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_crash_claim_a6fd832c"."schema_migrations" (   …` |
| 7302A5964BA4A13B | 1 | 4.7 | 0 | 0 | 1 | 70 | 1 | 7,206 | `CREATE TABLE IF NOT EXISTS "rt_group_amplification_edit_9d29498d"."schema_migrations" (   …` |
| 5E974A6F7C84F404 | 1 | 4.4 | 0 | 0 | 4 | 186 | 2 | 24,474 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Expired_b9f4bd18"."attachments" (     "attachmen…` |
| 587FECE708B64BA5 | 3 | 4.4 | 0 | 0 | 1 | 144 | 2 | 15,477 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_crash_claim_a6fd832c"."schema_migration_checkp…` |
| 875D456D6BC08F61 | 3 | 4.3 | 0 | 0 | 1 | 142 | 2 | 19,497 | `CREATE TABLE IF NOT EXISTS "rt_outbox_reclaim_token_47b4ad33"."schema_migration_checkpoint…` |
| 587890D2EEB3970D | 1 | 4.1 | 0 | 0 | 2 | 94 | 1 | 14,683 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_list_2c86d521"."conversations" (     "convers…` |
| ED5C045B04C0D91A | 1 | 4.1 | 1 | 4 | 9 | 11 | 0 | 980 | `INSERT INTO "rt_voice_bind_Expired_b9f4bd18"."attachments" (     attachment_id, uploader_u…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| E5AFA989139E6561 | 2,000 | 5,559,264 | 38,237 | 0 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "perf_32c13fb0c603"."messages" (  …` |
| 573DCDB8A2FCC27A | 2,000 | 707,424 | 8,097 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 247859BDA132156C | 1 | 110,148 | 1,006 | 0 | `INSERT INTO "rt_group_amplification_edit_9d29498d"."conversation_members" (     conversati…` |
| 3221D9D8394BC808 | 1 | 29,429 | 204 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_reclaim_token_47b4ad33"."relationship_projection_reb…` |
| D2FF291FD6F27AAC | 1 | 29,337 | 207 | 1 | `CREATE TABLE IF NOT EXISTS "rt_group_amplification_edit_9d29498d"."relationship_projection…` |
| 3B7252228BAB20C4 | 1 | 29,117 | 202 | 1 | `CREATE TABLE IF NOT EXISTS "rt_ledger_txn_rollback_84080684"."relationship_projection_rebu…` |
| E9F1A8B4EB6F908 | 1 | 27,577 | 172 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_list_2c86d521"."relationship_projection_items"…` |
| 5DD1516F574F9BA | 1 | 26,687 | 213 | 1 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Expired_b9f4bd18"."relationship_projection_rebui…` |
| C1AFF28E609CDB21 | 1 | 26,083 | 207 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_list_2c86d521"."relationship_projection_rebuil…` |
| 53C50C97941B7973 | 1 | 25,497 | 201 | 1 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Scanning_4a0c9032"."relationship_projection_rebu…` |
| BF0EEB71979B1BCF | 1 | 24,743 | 185 | 2 | `CREATE TABLE IF NOT EXISTS "rt_group_amplification_edit_9d29498d"."relationship_projection…` |
| 5E974A6F7C84F404 | 1 | 24,474 | 186 | 2 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Expired_b9f4bd18"."attachments" (     "attachmen…` |
| 6B33BB861E53D106 | 1 | 24,171 | 148 | 2 | `CREATE TABLE IF NOT EXISTS "rt_ledger_txn_rollback_84080684"."relationship_projection_snap…` |
| 385A0FA77A7298F1 | 1 | 23,537 | 172 | 2 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Expired_b9f4bd18"."relationship_projection_items…` |
| E94818A73340D6EF | 1 | 23,219 | 169 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_reclaim_token_47b4ad33"."relationship_projection_ite…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 2,000 | -8 |
| conversation_members | 0 | 2,000 | 0 | 2,000 | 14 |
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

> HOT 命中率（hot_updates / updates）：conversations 2,000/2,000（100%）；conversation_members 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。


## B：super-bundle 热路径语句（近热路径 = 调用数 ≥ 消息数一半）

| 调用 | 调用/消息 | exec/消息(ms) | wal_records/消息 | wal_bytes/调用 | sql 片段 |
|---|---|---|---|---|---|
| 2,000 | 1.0 | 0.20 | 24.2 | 3,301 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     F…` |
| 2,024 | 1.0 | 0.00 | 0.0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| 2,024 | 1.0 | 0.00 | 0.0 | 0 | `RESET ALL` |
| 2,024 | 1.0 | 0.00 | 0.0 | 0 | `SELECT pg_advisory_unlock_all()` |
| 2,024 | 1.0 | 0.00 | 0.0 | 0 | `CLOSE ALL` |
| 2,024 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD SEQUENCES` |
| 2,024 | 1.0 | 0.00 | 0.0 | 0 | `UNLISTEN *` |
| 2,024 | 1.0 | 0.00 | 0.0 | 0 | `DISCARD TEMP` |

## B：super-bundle（合并同事务写入为单条 CTE）（2000 条近热路径消息）

### 每消息成本汇总（WAL）

| 指标 | 窗口总量 | 每消息 |
|---|---|---|
| WAL 字节 | 6,621,387 | 3,311 |
| WAL 记录 | 50,885 | 25.4 |
| WAL FPI | 250 | 0.12 |
| WAL write | 1,681 | 0.841 |
| WAL sync | 0 | 0.000 |

### Top SQL（按执行耗时降序，窗口增量）

| queryid | calls | exec(ms) | rows | blks_read | dirtied | wal_records | wal_fpi | wal_bytes | sql |
|---|---|---|---|---|---|---|---|---|---|
| 287418D9446B3AF5 | 2,000 | 402.1 | 2,000 | 0 | 621 | 48,429 | 0 | 6,602,306 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 1F1A5B390EB81DF3 | 1 | 207.7 | 1 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_stat_force_next_flush(), pg_sleep($1)` |
| 2AD6BD6EC9665A0D | 1 | 102.4 | 0 | 0 | 0 | 37 | 1 | 4,153 | `CREATE INDEX CONCURRENTLY "ix_conversation_members_user_pinned_list"     ON "rt_lifecycle_…` |
| C651AFCF55162FC2 | 5 | 22.2 | 860 | 0 | 0 | 3 | 0 | 196 | `SELECT ns.nspname, t.oid, t.typname, t.typtype, t.typnotnull, t.elemtypoid FROM (     -- A…` |
| C49C71344BD7CF03 | 1 | 20.8 | 2,723 | 0 | 0 | 0 | 0 | 0 | `SELECT queryid, query, calls, total_exec_time, rows,        shared_blks_read, shared_blks_…` |
| 648961FB01E444E0 | 1 | 13.1 | 0 | 0 | 0 | 8 | 0 | 626 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 63D823AC0B9DF08A | 1 | 11.5 | 0 | 0 | 0 | 6 | 0 | 488 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 98AC1853651047FF | 1 | 10.8 | 0 | 0 | 0 | 6 | 0 | 494 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 319921C387CD9D2E | 1 | 10.7 | 0 | 0 | 0 | 7 | 0 | 542 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 1FF541EAC640F5A | 1 | 10.3 | 0 | 0 | 0 | 6 | 0 | 390 | `DO $$ BEGIN     IF EXISTS (         SELECT 1 FROM information_schema.columns         WHERE…` |
| 1D41E8AD84E0B9D1 | 1 | 8.4 | 1 | 1 | 23 | 26 | 0 | 3,798 | `WITH inserted_message AS MATERIALIZED (     INSERT INTO "rt_lifecycle_active_after_rollbac…` |
| 9DFE0023C220BBE7 | 2,024 | 6.4 | 0 | 0 | 0 | 0 | 0 | 0 | `SET SESSION AUTHORIZATION DEFAULT` |
| C431907809E26F56 | 1 | 6.2 | 0 | 0 | 0 | 70 | 1 | 7,215 | `CREATE TABLE IF NOT EXISTS "realtime"."schema_migrations" (     "version" integer NOT NULL…` |
| 6FB3118884DDDBD2 | 1 | 5.7 | 0 | 0 | 4 | 139 | 2 | 14,815 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."relationship_change_log" (  …` |
| B8D3DC37523ED3E1 | 1 | 5.7 | 0 | 0 | 1 | 69 | 1 | 7,026 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_f4166fbf"."schema_migrations" (     "ve…` |
| F353A2690F9249F | 1 | 5.5 | 0 | 0 | 1 | 69 | 1 | 7,866 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_d1e6c813"."schema_migrations" …` |
| 10D00B0DC19F1074 | 1 | 4.9 | 0 | 0 | 1 | 69 | 1 | 7,038 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."schema_migrations" (     "ve…` |
| 238584DBF4534C5F | 1 | 4.7 | 0 | 0 | 0 | 37 | 1 | 4,165 | `CREATE INDEX CONCURRENTLY "ix_conversation_members_user_pinned_list"     ON "rt_outbox_aud…` |
| 46D6128C6FE46D8B | 1 | 4.5 | 0 | 0 | 1 | 139 | 2 | 18,329 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."friend_requests" (     "requ…` |
| D9507526C04D2C69 | 1 | 4.5 | 0 | 0 | 3 | 103 | 2 | 11,078 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."friendships" (     "friendsh…` |
| C049A50FA16E1BEE | 3 | 4.5 | 0 | 0 | 1 | 143 | 2 | 15,419 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_d1e6c813"."schema_migration_ch…` |
| B9A3FC5813DDA531 | 2,024 | 4.3 | 0 | 0 | 0 | 0 | 0 | 0 | `RESET ALL` |
| 7EB68A6188984E4A | 1 | 4.3 | 0 | 0 | 3 | 185 | 2 | 24,408 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_d1e6c813"."attachments" (     …` |
| 75D57A4834A0BB83 | 3 | 4.2 | 0 | 0 | 3 | 143 | 2 | 15,411 | `CREATE TABLE IF NOT EXISTS "realtime"."schema_migration_checkpoints" (     "migration_vers…` |
| 6AA35CE9C8E92ACB | 3 | 4.1 | 0 | 0 | 1 | 143 | 2 | 15,577 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."schema_migration_checkpoints…` |
| 66CBF308DA60102 | 1 | 4.1 | 1 | 5 | 11 | 11 | 0 | 1,026 | `INSERT INTO "rt_voice_bind_Uploaded_f4166fbf"."attachments" (     attachment_id, uploader_…` |
| 20928A0A690DF580 | 2,024 | 4.0 | 2,024 | 0 | 0 | 0 | 0 | 0 | `SELECT pg_advisory_unlock_all()` |
| C7D9F8D0FE6C011D | 1 | 3.9 | 0 | 0 | 1 | 184 | 2 | 20,626 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_active_after_rollback_d3c8f0e4"."attachments" (  …` |
| 756E532241F1D2BA | 1 | 3.8 | 0 | 0 | 2 | 138 | 1 | 14,589 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."group_operation_audit" (    …` |
| 7A2975F9E4D89B70 | 1 | 3.8 | 0 | 0 | 3 | 135 | 1 | 17,970 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_d1e6c813"."group_operation_aud…` |

### Top SQL（按 WAL 字节降序，窗口增量）

| queryid | calls | wal_bytes | wal_records | wal_fpi | sql |
|---|---|---|---|---|---|
| 287418D9446B3AF5 | 2,000 | 6,602,306 | 48,429 | 0 | `WITH ordered_users AS MATERIALIZED (     SELECT DISTINCT t.user_id     FROM (VALUES ($2), …` |
| 9DEBFDCA885F1BDA | 1 | 29,708 | 211 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."relationship_projection_rebu…` |
| F8F10996AD1DB2FD | 1 | 24,913 | 200 | 1 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_active_after_rollback_d3c8f0e4"."relationship_pro…` |
| D1D45B53123618E8 | 1 | 24,784 | 201 | 1 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_f4166fbf"."relationship_projection_rebu…` |
| 8F34AE3A93A81112 | 1 | 24,737 | 200 | 1 | `CREATE TABLE IF NOT EXISTS "realtime"."relationship_projection_rebuild_state" (     "id" s…` |
| 7EB68A6188984E4A | 1 | 24,408 | 185 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_pending_index_cleanup_d1e6c813"."attachments" (     …` |
| 8D635AAC52B4DAE9 | 1 | 23,248 | 170 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."relationship_projection_item…` |
| F7470B0497D29F65 | 1 | 22,397 | 167 | 2 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_active_after_rollback_d3c8f0e4"."relationship_pro…` |
| C12C67489523BAF7 | 1 | 21,619 | 182 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."relationship_projection_hist…` |
| E4B10393007D3119 | 1 | 21,351 | 187 | 2 | `CREATE TABLE IF NOT EXISTS "rt_voice_bind_Uploaded_f4166fbf"."relationship_projection_hist…` |
| 9DB17B83421AECE0 | 1 | 20,919 | 183 | 2 | `CREATE TABLE IF NOT EXISTS "realtime"."relationship_projection_history" (     "owner_user_…` |
| 62A69CB444BB4206 | 1 | 20,859 | 182 | 2 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_active_after_rollback_d3c8f0e4"."relationship_pro…` |
| C7D9F8D0FE6C011D | 1 | 20,626 | 184 | 2 | `CREATE TABLE IF NOT EXISTS "rt_lifecycle_active_after_rollback_d3c8f0e4"."attachments" (  …` |
| A4A6D31079C94B29 | 1 | 20,598 | 122 | 1 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."conversation_members" (    …` |
| 1A30E06F1F48B8B7 | 1 | 20,520 | 182 | 2 | `CREATE TABLE IF NOT EXISTS "rt_outbox_audit_atomic_3e5ee10e"."attachments" (     "attachme…` |

### 表级统计（窗口增量）

| table | inserts | updates | deletes | hot_updates | dead_tuples |
|---|---|---|---|---|---|
| schema_migrations | 0 | 0 | 0 | 0 | 0 |
| schema_migration_checkpoints | 0 | 0 | 0 | 0 | 0 |
| messages | 2,000 | 0 | 0 | 0 | 0 |
| outbox | 2,000 | 0 | 0 | 0 | 0 |
| conversations | 0 | 2,000 | 0 | 2,000 | -11 |
| conversation_members | 0 | 2,000 | 0 | 2,000 | -6 |
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

> HOT 命中率（hot_updates / updates）：conversations 2,000/2,000（100%）；conversation_members 2,000/2,000（100%）。non-HOT 更新会对已修改索引列维护索引并产生额外 WAL。

