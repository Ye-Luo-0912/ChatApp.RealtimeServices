# Realtime ops endpoints

Auth: header `X-Ops-Api-Key` = `Ops:ApiKey`. Empty key is open in non-Production; Production returns 503 if unset.

## Existing outbox

| Method | Path |
|--------|------|
| GET | `/ops/outbox/summary` |
| GET | `/ops/outbox/?status=&targetUserId=&offset=&limit=` |
| GET | `/ops/outbox/{eventId}` |
| POST | `/ops/outbox/{eventId}/replay` |
| POST | `/ops/outbox/replay` |

## Migrations & backlogs

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/ops/migrations/progress` | Catalog versions, applied rows, open checkpoints, `NotFullyAppliedVersions`, `HasDeferredInProgress` |
| GET | `/ops/backlogs/` | Outbox pending/dead ages + mig009 `messages.conversation_id IS NULL` count (if 009 not applied) + attachment status counts + optional `MessagesBeyondRetentionCount` / `OldestPurgeableReceivedAtMs` when `MessageRetention` is enabled |

Account cleanup saga / inbox DLQ / `T_AttachmentBlobDeleteJob` live on **ChatApp.Server** (`/api/admin/account-cleanup-saga`, `/api/admin/ops/*`).

## Relationship projection

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/ops/relationship-projection/status` | Rebuilder cursor, stable passes and coverage |
| GET | `/ops/relationship-projection/streams` | Privacy-minimized local stream metadata |
| GET | `/ops/relationship-projection/reconcile` | Server digest versus local projection; 200/409/503 |

For the release gate, load the Ops key into `CHATAPP_OPS_API_KEY` from the secret store and run:

```powershell
pwsh scripts/Invoke-RelationshipProjectionReconcile.ps1 -BaseUri https://realtime.example
```

The command performs two bounded full passes, requires stable Rebuilder state before and after each pass,
compares full-pass SHA-256 fingerprints, writes a JSON report under `.artifacts/`, and never writes the key.
Redirects and keyed HTTP are rejected. `-AllowUnauthenticated` and `-AllowInsecureHttp` are only for an
explicitly isolated non-Production instance.

Related metrics: outbox pending/dead gauges + `OutboxMetricsCollector` reconcile (see [p1-perf-stability.md](p1-perf-stability.md)); message retention GC metrics in [message-retention.md](message-retention.md).
