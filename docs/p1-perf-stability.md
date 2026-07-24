# Realtime P1 performance notes

## Outbox metrics
- Hot-path gauges (`pending` / `dead`) update on publish success, dead-letter, and dead replay.
- `OutboxMetricsCollector` reconciles Pending/Dead aggregates on a long interval (default 5m), not every 5s.
- `GetStatsAsync` only aggregates `status IN (Pending, Dead)` via index-friendly subqueries (ops `/ops/outbox/summary` and rare reconcile).
- Human/ops curl: also `/ops/migrations/progress` and `/ops/backlogs/` (see [ops.md](ops.md)).

## SaveAsync (Npgsql production path)
Created path round-trips (same transaction):
1. `INSERT messages … ON CONFLICT DO NOTHING`
2. Optional attachment bind (`UPDATE … RETURNING`)
3. `NpgsqlBatch`: conversation tip CTE + receiver unread `UPDATE`（两语句一往返；未读仍须独立语句，因 PG modifying CTE 快照限制）
4. Multi-row `INSERT outbox … ON CONFLICT DO NOTHING`

Duplicate path: no Outbox re-insert (message + outbox were committed atomically).

## EfCore gaps (non-production / fallback)
- Extra `SaveChanges` round-trips vs Npgsql CTE merge (conversation advance now shares the merged SQL helper).
- `ApplyReceiptAsync` still lacks conversation read-cursor / unread outbox advance that Npgsql has.
- `DeleteByUserAsync` still deletes messages + outbox only; no Direct wipe / peer tip-unread repair (use Npgsql store in production).
- Concurrent unique-violation duplicate path returns Duplicate without repair tooling.

## Account deletion conversation semantics (Npgsql)
- **Direct**: delete the conversation for both members (messages already removed by sender/receiver filter).
- **Non-direct**: tombstone deleted member; clear tip when `last_sender_user_id` was the deleted user; zero remaining peers' unread / clear peer projection.

## Deferred
- Multi-device sync beyond device cursors already present.
- See [attachments.md](attachments.md) for formal attachment model (Migration012 landed; Server blob GC / export preference still follow-up).
