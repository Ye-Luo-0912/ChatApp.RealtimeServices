# Message edit & recall

## Windows

| Setting | Default | Meaning |
|---------|---------|---------|
| `MessageEdit:MaxAgeMinutes` | `15` | Sender may edit within this age after `received_at_ms` |
| `MessageRecall:MaxAgeMinutes` | `2` | Sender may recall within this age after `received_at_ms` |

Only the original sender may edit or recall. Admin override is out of scope for v1.

## Persistence

- `edit_version` — starts at `1`; each successful content edit increments by 1
- `edited_at_ms` — last successful edit time (null if never edited)
- `recalled_at_ms` — soft-delete / recall time; content cleared to `""` for all readers
- `changed_at_ms` — max of insert / last edit / recall time; used by sync catch-up

History and sync return latest content, `edit_version`, `edited_at_ms`, `recalled_at_ms`, and `changed_at_ms`. Recalled rows are stubs (empty body + `recalled_at_ms`).

## Realtime events

- `MessageEdited` — conversation participants (receiver + sender-device echo); payload includes `messageId`, `editVersion`, `content`, `editedAtMs`
- `MessageRecalled` — same fan-out; payload includes `recalledAtMs` (content already redacted server-side)

Edit EventIds include version so successive edits are not deduped by Outbox.

## Sync catch-up (mutation-aware)

Catch-up after a watermark uses `(changed_at_ms, message_id)`, not insert-only `(received_at_ms, message_id)`.

Wire field `AfterReceivedAtMs` / cursor `ReceivedAtMs` remains the name for compatibility; for catch-up it means **after changed_at**. Clients should advance the watermark from catch-up `NextCursor` (or each item’s `ChangedAtMs`), not from display `ReceivedAtMs` alone. Fresh history pages (no after-cursor) still order by `received_at_ms` for chronology.

## Idempotency

Edit/recall commands carry `requestId`. Duplicates for the same actor return the stored result via `message_mutation_requests`. Reusing a request id for a different message or edit content yields `request_id_conflict`.

## Attachments

- **Recall:** message is marked recalled; Bound attachments are left in place until orphan/GC policy runs (no immediate unbind).
- **Edit (v1):** content-only; attachment set is unchanged.

## Fingerprint / send path

Edits are updates to the same `message_id`. They do **not** create a new send or change `(sender_user_id, client_message_id)` idempotency. Recall is a status change, not a new message.
