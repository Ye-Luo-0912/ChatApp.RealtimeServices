# Message receipts & read state

## Models

### Per-member conversation watermark (DM + group)

Stored on `realtime.conversation_members`:

- `last_read_message_id`
- `last_read_at_ms`
- `unread_count`

Authoritative advance path: `ConversationMarkRead` → `AdvanceReadCursorAsync`.

- Membership required (`grp:…` and `dm:…`)
- Cursor clamped to conversation tip
- Non-members → `not_found`
- Multi-device: max-merge only (never moves backward)

Sync / conversation list expose the **current user’s** watermark via `LastReadMessageId` / `LastReadAtMs`.

### Per-message delivery / read (DM only)

`MessageReceipt` updates `messages.delivered_at_ms` / `read_at_ms` when `receiver_user_id` matches the acking user, then emits `MessageReceiptUpdated` to the **sender**.

Group messages use `receiver_user_id = 0`, so per-message receipts are rejected (`receipt_not_allowed`). **Deferred** for groups — use conversation watermarks instead.

## Events on MarkRead

When the watermark advances:

| Event | Target | Purpose |
|-------|--------|---------|
| `UnreadCountChanged` | reader | Own unread / watermark sync (incl. other devices) |
| `ConversationRead` | other active members | Peer read UI: `(conversationId, readerUserId, lastReadMessageId, lastReadAtMs)` |

One Outbox row per target user (not N² fan-out of pairwise receipts). Gateway downlink: `PacketCommand.ConversationRead` (152).

## Residuals

- Per-message / per-device delivery matrix for groups
- Aggregated “read by N of M” summaries beyond client-side watermark tracking
