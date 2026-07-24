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
| GET | `/ops/backlogs/` | Outbox pending/dead ages + mig009 `messages.conversation_id IS NULL` count (if 009 not applied) + attachment status counts |

Account cleanup saga / inbox DLQ / `T_AttachmentBlobDeleteJob` live on **ChatApp.Server** (`/api/admin/account-cleanup-saga`, `/api/admin/ops/*`).

Related metrics: outbox pending/dead gauges + `OutboxMetricsCollector` reconcile (see [p1-perf-stability.md](p1-perf-stability.md)).
