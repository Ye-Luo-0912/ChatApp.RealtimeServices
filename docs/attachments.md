# Attachments (Migration012)

## Wire format

### AttachmentRef (v1)
History, Sync catch-up, and Realtime downlink payload v2 carry attachment refs (not inline blobs):

| Field | Notes |
|-------|--------|
| `refVersion` | Current: `1` |
| `attachmentId` | Stable id |
| `fileName` | Optional display name |
| `contentType` | MIME |
| `sizeBytes` | Size in bytes |
| `status` | `0=Scanning`, `1=Available` (Bound → Available) |
| `downloadApiHint` | Usually attachmentId; client calls `GET /api/attachments/{id}/download` |
| `downloadToken` | Optional short-lived token (Server-issued) |
| `thumbnailApiHint` | Optional thumbnail path hint |

TCP Gateway `ChatMessage` / `MessageHistoryItem` mirror the same shape; uplink sends only `attachmentIds[]`.

### RealtimeChatMessagePayload v2
`payloadVersion: 2` with `attachments: AttachmentRef[]`. v1 events omit attachments.

### Content fingerprint v2
`content_fingerprint` prefix `2:`; hash input: `receiver + content + sorted unique attachmentIds`.
Same content + same attachment set (any order) → Duplicate; same content + different set → ContentConflict.
Nullable/legacy (v1) stored fingerprints still compare by recomputing v2 from DB-bound attachment ids.

### Download hint
No permanent public URL. Clients use `downloadApiHint` (usually attachmentId) against ChatApp.Server:

1. `POST /api/attachments/{id}/ticket` (session auth) → short-lived single-use `ticket`
2. `GET /api/attachments/{id}/download?ticket=...` (or session-auth download without ticket)

`downloadToken` on the wire remains optional / null unless Server fills it; Realtime does not mint tickets.

### Sync device cursors
Bootstrap resolves client watermarks to a real in-conversation message for incremental catch-up.
Invalid / unusable cursors emit additive `ResetsRequired` (with tip hints + `SyncCursorResetReason`) instead of silent tip-clamp success:
- `MessageNotFound` — missing/deleted/random message id (or empty tip)
- `AheadOfTip` — watermark message is ahead of conversation tip
- `MembershipLost` — client watermark for a conversation the user is not a member of
- `GapTooLarge` — optional when `SyncBootstrap:MaxCatchUpGapMs` is set and tip lag exceeds it
- `BeyondRetention` — optional when `SyncBootstrap:RetentionHorizonMs` is set and client watermark is older than `tip − RetentionHorizonMs` (also reclassifies MessageNotFound when the missing id is clearly pre-horizon). Backed by age-based GC when `MessageRetention:Enabled` (see [message-retention.md](message-retention.md)).

Device cursor upsert persists **only** the last message actually returned in catch-up — never raw client watermarks and never ResetRequired tips (avoids skipping undelivered history).

## Status (DB)
`realtime.attachments.status` smallint: `0=Ticketed`, `1=Confirmed`, `2=Bound`, `3=Abandoned`.

## Owned by Realtime
- Schema + `NpgsqlRealtimeAttachmentStore`
- `SaveAsync` bind (Confirmed → Bound) when `IncomingMessageCommand.AttachmentIds` present
- History/Sync enrich via `RealtimeHistoryAttachmentEnricher` (batch `ListByMessageIdsAsync`)
- Sync bootstrap resolves watermarks via `ResolveSyncWatermarksAsync` (ResetRequired for invalid/future cursors; no silent tip-clamp)
- Account delete: delete rows → enqueue chunked `AttachmentBlobsPurge` → `AccountCleanupCompleted`

## Online migrations 009–011
- `IRealtimeSchemaMigration.RequiresTransaction` (default true); runner skips outer txn when false
- Migration009: per-batch commit + `schema_migration_checkpoints`; keyset + `FOR UPDATE SKIP LOCKED`; interrupt/resume
- Migration010/011: `CREATE INDEX CONCURRENTLY` outside txn
- Dual-write tip already on write path (`conversations.last_message_*` + `members.last_message_at_ms`); list reads `COALESCE`; backfill then contract

## Server responsibilities (not in this repo)
- Blob ticket/upload/confirm; write Confirmed rows via same Postgres
- Confirm enqueues content scan (Scanning until worker Confirmed/Rejected); bind/download blocked while Scanning
- Consume `AttachmentBlobsPurge` on account-cleanup subject and delete object keys
- Export prefers formal attachment rows; URL-scan remains legacy fallback
- Implement `GET /api/attachments/{id}/download` (authz + optional short-lived ticket) and `POST .../ticket`

## Residuals / deferred
- Age-based message purge/tombstone GC → **landed**: `MessageRetentionWorker` + [message-retention.md](message-retention.md) (hard-delete + tip repair; Bound attachment blobs left for orphan/account GC)
- EfCore message store does **not** bind attachments (production path is Npgsql); conflict compare treats existing attachments as empty
- No Ticketed insert API in Realtime yet (Server may insert Confirmed directly after upload confirm)
- No Abandoned sweeper worker for unbound Ticketed/Confirmed age index → **landed**: Server `AttachmentAbandonedAgeSweeper` (via `AttachmentCleanupWorker`) marks aged unbound Ticketed/Confirmed → Abandoned and enqueues blob delete tombs.
- Orphan `message_id` when peer messages deleted but uploader differs (uploader rows purged only on uploader account delete) — also applies after retention GC leaves Bound attachments pointing at purged messages
- Retention GC does not recompute denormalized `unread_count` (unlikely issue for long horizons)
- Soft-tombstone then hard-delete / ConversationChanged fanout not implemented (v1 is silent hard-delete)
- `scripts/realtime-schema.sql` still omits migrations 8–11 DDL history; appends attachments + version 12 for Job bootstrap
- Multi-device attachment sync beyond message fanout
- Thumbnail generation pipeline not implemented
