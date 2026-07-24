# Message reactions (emoji)

## Rules (v1)

- Conversation members (or DM sender/receiver) may add/remove a reaction on a visible message.
- Recalled messages reject reactions (`message_recalled`).
- Reaction key: unicode emoji or short code, max length `MessageReaction:MaxEmojiLength` (default 32).
- Unique per `(message_id, user_id, emoji)`.
- Idempotent: add twice → success (no second event); remove missing → success.

## Limits

| Setting | Default |
|---------|---------|
| `MessageReaction:MaxDistinctEmojisPerMessage` | 20 |
| `MessageReaction:MaxReactionsPerUserPerMessage` | 20 |
| `MessageReaction:MaxEmojiLength` | 32 |

## Persistence

Table `message_reactions (message_id, user_id, emoji, created_at_ms)` PK unique.

Successful add/remove bumps `messages.changed_at_ms` so sync catch-up re-delivers the message stub; history/sync enrichers attach reaction summaries (`emoji → count`, `reactedByMe`).

## Wire / events

| Direction | Command / event |
|-----------|-----------------|
| Uplink | TCP `AddReactionRequest` (130) / `RemoveReactionRequest` (133) → NATS `chat.message-reaction` |
| Ack | `AddReactionAck` (131) / `RemoveReactionAck` (134) |
| Downlink | `ReactionAdded` (132) / `ReactionRemoved` (135) to message sender + receiver (skip originating reactor session) |

EventIds include reactor + emoji + `occurredAtMs` so remove→re-add is not Outbox-deduped.

## Sync

Catch-up continues to use `(changed_at_ms, message_id)`. Reaction changes advance the watermark the same way as edit/recall.
