# Message retention GC



Age-based hard-delete of old realtime messages so `SyncBootstrap:RetentionHorizonMs` / `BeyondRetention` is backed by real purge (not just sync invalidation).



## Relationship: horizon ↔ GC



| Knob | Role |

|------|------|

| `SyncBootstrap:RetentionHorizonMs` | Sync: cursor older than `tip − horizon` → `BeyondRetention` (0 = off) |

| `MessageRetention:Enabled` | Master switch for the GC worker (default `false`) |

| `MessageRetention:RetentionHorizonMs` | GC cutoff: delete `received_at_ms < now − horizon`. When 0, falls back to SyncBootstrap horizon, then `RetentionDays` |

| `MessageRetention:RetentionDays` | Convenience when both ms horizons are 0 |



**Contract:** keep the same window for sync and GC. GC removes rows older than the horizon; sync tells clients whose watermark is past `tip − horizon` to reset. Purged ids still appear as `MessageNotFound` and are reclassified to `BeyondRetention` when the client is clearly pre-horizon.



## Worker behavior



- Hosted service `MessageRetentionWorker`: batched keyset deletes with sleep + `MaxBatchesPerCycle`

- Multi-instance: Postgres session advisory lock (`MSGRETN`)

- Hard-delete message rows; cascade `message_reactions` + `message_mutation_requests`

- **Attachments:** Bound rows on purged messages are unbound (`message_id`/`conversation_id` cleared) and marked `Abandoned`, then chunked `AttachmentBlobsPurge` outbox events are enqueued (same contract as account cleanup; Server `AccountCleanupSagaWorker` deletes blobs). Confirmed-but-unbound uploads are never touched.

- Repair conversation tip (+ member `last_message_at_ms`) so empty / tip-purged conversations are not left dangling

- **Unread:** recount `conversation_members.unread_count` from messages still present after each member's last-read watermark (clamped ≥ 0 / max tracked), in the same tip-repair pass

- **Silent GC** — no `ConversationChanged` / `UnreadCountChanged` / gateway fanout in v1 (blob purge outbox is the exception)



Idle when `Enabled=false` or effective horizon is 0.



## Metrics & ops



- Counters: `realtime.messages.retention.deleted`, `realtime.messages.retention.errors`

- Gauge: `realtime.messages.retention.lag` (age of oldest still-purgeable row, seconds)

- `GET /ops/backlogs/`: `MessagesBeyondRetentionCount` + `OldestPurgeableReceivedAtMs` when retention is effectively enabled



## Config example



```json

"SyncBootstrap": { "RetentionHorizonMs": 7776000000 },

"MessageRetention": {

  "Enabled": true,

  "RetentionHorizonMs": 0,

  "BatchSize": 500,

  "IntervalMs": 60000,

  "BatchSleepMs": 100,

  "MaxBatchesPerCycle": 100

}

```



(With `RetentionHorizonMs: 0` on MessageRetention, GC uses the SyncBootstrap value above.)

