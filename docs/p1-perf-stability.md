# Realtime P1 performance notes

## Outbox metrics
- Hot-path gauges (`pending` / `dead`) update on publish success, dead-letter, and dead replay.
- `OutboxMetricsCollector` reconciles Pending/Dead aggregates on a long interval (default 5m), not every 5s.
- `GetStatsAsync` only aggregates `status IN (Pending, Dead)` via index-friendly subqueries (ops `/ops/outbox/summary` and rare reconcile).
- Human/ops curl: also `/ops/migrations/progress` and `/ops/backlogs/` (see [ops.md](ops.md)).

## 2026-08-08 shared hot-path layer

- `CoalescingAsyncSignal` uses one atomic pending bit plus a single-slot semaphore. Concurrent
  notifications no longer race on `CurrentCount` or use `SemaphoreFullException` as control flow.
- `SharedLeaseScheduler<TState>` gives each worker runtime one timer and one indexed min-heap.
  Fast ACK/Outbox operations do not create a task, linked CTS, or delay timer; completed nodes are
  removed immediately and reused through a bounded 4,096-node pool. Version tokens prevent stale
  handles from completing a reused node, and in-flight renewals delay reuse until the callback exits.
- UTF-8 temporary writers use two thread-affine cache slots, a 64 KiB retention ceiling, ArrayPool
  buffers, and versioned leases. Mutable payload/event instances are never shared between requests.
- The wire microbenchmark (`ShortRun`, Windows/.NET 10, 512-byte content) reduced allocation from
  `3.88 KB` to `2.09 KB` per event (`-46%`). Serialization time was `2.331 us` versus `2.699 us`
  (`+16%`), so this is deliberately a GC/allocation trade; end-to-end CPU must be decided by the
  Linux workload rerun, not this isolated benchmark.

## SaveAsync (Npgsql production path)

The no-attachment direct-message path uses one transaction and two fixed commands:

1. One positional-parameter admission/sequence CTE acquires ordered lifecycle locks, checks
   tombstones and direct-message authorization, reads the idempotency canonical, and allocates the
   conversation/member sequence only for an active, authorized, new command.
2. One data-modifying bundle CTE inserts the message, one multi-target Outbox event, and the optional
   idempotency ledger row atomically, then the transaction commits.

The production Npgsql path no longer runs a separate pre-transaction authorization query. Attachment
messages keep the bind-aware staged path because the final event must contain the bound metadata.
Duplicate/canonical paths do not advance the sequence or reinsert the Outbox row. Each call still owns
its connection, transaction, commands, parameters, and result; only immutable schema SQL is cached.

## Outbox publication and disk pressure

- The common case (`records.Count == 1`) avoids `Parallel.ForAsync`, result arrays, and batch arrays;
  its completion update still checks `claim_token` and returns the exact affected-row count.
- Multi-record batches allocate result/group collections only when needed. A rejected lease stops
  completion writes and lets the next owner retry under JetStream message-id deduplication.
- The publisher shares one lease scheduler for its lifetime. The previous per-batch linked CTS,
  `Task.Run`, `Task.Delay`, and cancellation-exception loop are gone.
- Published retention is configurable. The default `0` completes a successfully published claim with
  one claim-token-guarded `DELETE`, avoiding a Published tuple plus a later cleanup tuple. Set a positive
  hour value when operators explicitly require queryable Published history. Cleanup then deletes up to
  `2,000 × 30 = 60,000` rows per
  minute with 100 ms spacing: enough for 640 rows/s plus 56% headroom while reducing delete-command
  count versus 500-row batches. Pending and dead rows keep their reliability semantics.

These changes target the formal soak's `2,034,057 TaskCanceledException`, 68
`SemaphoreFullException`, `97,988.93 B/msg`, and `10.1964 DB ops/msg`. Unit/integration tests validate
behavior; short same-profile Smoke/Change/Capacity runs decide SQL, WAL, DB-op, allocation, CPU, GC,
and latency regressions. A 30-minute Candidate is reserved for a frozen release candidate, while the
8-hour Formal run is only for release gating, long-lived memory, and checkpoint/WAL steady state.

## 2026-08-09 commit-hint and database-session layer

- A committed Outbox event id is offered to a bounded 65,536-entry in-process queue only after the
  business transaction commits. The queue is a performance hint, never a reliability authority:
  overflow, restart, another instance's writes, or a throwing custom signal are recovered by a
  periodic database scan. Signal failures therefore cannot turn an already committed command into a
  false write failure.
- The publisher claims hinted ids with the same Pending/retry/lease/claim-token predicates as the
  recovery scan. A worker owns one serial Npgsql connection and two prepared commands (hint and
  recovery) for its lifetime; it is not shared across workers or concurrent operations. A storage
  failure disposes the session and recreates it through the existing bounded retry loop.
- Published completion is accumulated up to 100 rows or 100 ms. NATS publication is not delayed;
  only the claim-token-guarded database completion is combined. The flush window is validated below
  one third of the lease. A crash or stale token leaves a Pending row for normal lease recovery and
  JetStream message-id deduplication.
- While hints are available, the full Pending recovery query runs at most every five seconds. When
  idle, the worker sleeps directly until that recovery deadline instead of issuing empty scans at the
  old 200 ms poll cadence. Implementations without id hints retain the configured legacy poll cadence.
- Migration 058 removes the unused global conversation-tip index and sets conversation fillfactor to
  80 so hot tip updates can use HOT. Migration 059 removes only the duplicate Pending created-time
  index; `ix_outbox_pending(next_attempt_at_ms, created_at_ms)` remains for retry/recovery, and Dead,
  Published-cleanup, target-user, and ownership indexes are unchanged.

Validation: Release solution build has zero warnings/errors; unit tests `296/296` and PostgreSQL
container integration tests `42/42` pass. These tests establish ownership, retry, index, and migration
semantics. Same-profile short runs decide SQL/WAL/operation/allocation regressions; Candidate and
Formal runs are reserved for frozen-candidate trends and final long-lived release evidence.

## 2026-08-09 bounded hint coalescing decision

The Outbox publisher can coalesce an underfilled hint batch for `0..50 ms`; `0` disables the wait and
is the default. The optional wait never crosses recovery-scan or completion-flush deadlines and does
not change Pending/lease/claim-token recovery semantics. It shares only the worker-owned queue and
immutable configuration; connections, commands, transactions, payloads, and mutable sessions remain
single-owner. With the default `0`, the hot path creates no coalescing delay timer.

Each configuration (`0 ms` and `2 ms`) ran three clean 20-second cross-Gateway trials at 320 msg/s. Both
configurations delivered and acknowledged every message with no duplicate, missing, pending, or dead
record. `2 ms` reduced median DB operations/message by `20.36%` and preclaim calls by `74.58%`, with
unchanged throughput, but median delivery P95 rose from about `24.6 ms` to `38.9–47.1 ms`, delivery
P99 from `69.6–73.7 ms` to `98.3 ms`, and one of three runs reached about `393 ms` P99. The clean
`0 ms` control had no comparable spike. Therefore `2 ms` remains an explicit resource-priority option;
it is not a safe low-tail-latency default, and `3 ms` is also rejected as a default.

This repeated short A/B is sufficient for the configuration decision, not for `MemoryStable` or a
release-soak verdict. The capacity manifest and report now record the effective window so future runs
cannot silently mix configurations.

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
