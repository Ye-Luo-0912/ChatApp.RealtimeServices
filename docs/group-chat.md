# Group chat (v1)

## Model

- `ConversationType.Group = 2`
- Conversation Id: `grp:{32 hex}` (`ConversationId.CreateGroup`)
- Members in `realtime.conversation_members` with `role` smallint:
  - `1 Owner` · `2 Admin` · `3 Member`
- Group title / creator on `realtime.conversations` (`title`, `created_by_user_id`)
- Migration: `Migration019_GroupConversationRoles` (online-friendly `ADD COLUMN IF NOT EXISTS`)

## Membership API (Realtime-owned)

NATS Core request/reply subject: `chat.group-conversation`

`GroupConversationOperation`:

| Op | Who | Notes |
|----|-----|-------|
| Create | any authenticated user | creator = Owner; optional initial members |
| AddMembers | Owner / Admin | |
| RemoveMember | Owner / Admin | cannot remove Owner; Admin cannot remove Admin |
| Leave | any member | Owner must transfer ownership first |
| ChangeRole | Owner only | `NewRole=Owner` transfers ownership (former Owner → Admin) |
| ListMembers | members only | |

Gateway TCP: `CreateGroupRequest`…`ListGroupMembersResponse` (136–147), downlink `MemberJoined` / `MemberLeft` / `MemberRemoved` / `RoleChanged` (148–151).

## Events (Outbox → Gateway)

One Outbox row per `TargetUserId` (same pattern as reactions):

- `ConversationListChanged` — group created / tip updates (`Title` on payload)
- `MemberJoined` / `MemberLeft` / `MemberRemoved` / `RoleChanged`
- Message lifecycle for groups fans out to **all active members**:
  - `MessageReceived` / `MessageEdited` / `MessageRecalled` / reactions

## Messaging

- Uplink: set `ChatMessage.ConversationId` to `grp:…` (TargetUserId ignored / 0)
- Only active members may send; removed members fail `forbidden`
- History by conversation already membership-gated; by message-id allows group members
- Conversation mark-read / prefs continue to use membership rows

## Residuals (out of scope for v1)

- Invite links / join codes
- @mentions
- Announcement channels
- Mute-at-group level beyond per-member prefs already present
- Fine-grained permission matrix beyond Owner / Admin / Member
- Per-message delivered/read receipts for groups (still DM-oriented; use conversation mark-read)
- User profile enrichment on create (Realtime does not call ChatApp.Server for display names)
