using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Messaging;

/// <summary>
/// 消息写入 admission 的共享数据库读取层：在同一条 SQL 中按固定顺序取得生命周期共享锁、
/// 检查 tombstone，并读取幂等账本 canonical，避免多个短命令和往返。
/// </summary>
internal static class MessageWriteAdmissionReader
{
    /// <summary>
    /// 单聊热路径把 admission 与会话序号分配合并为一个数据库命令。只有生命周期正常、
    /// 事务内授权通过且幂等账本未命中时，才推进 Conversation/member 序号；因此重放、
    /// 授权拒绝和已注销用户不会产生序号副作用。
    /// </summary>
    public static async Task<MessageWriteAdmission> AcquireDirectAndAllocateSequenceAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        long senderUserId,
        long receiverUserId,
        string clientMessageId,
        string conversationId,
        string messageId,
        string preview,
        long receivedAtMs,
        CancellationToken ct)
    {
        var commandText = schema.GetOrAddCommandText(
            "message-write-admission-direct-sequence",
            static schema => $"""
             WITH ordered_users AS MATERIALIZED (
                 SELECT DISTINCT t.user_id
                 FROM (VALUES ($2), ($3)) AS t(user_id)
                 WHERE t.user_id > 0
                 ORDER BY t.user_id
             ),
             locked_users AS MATERIALIZED (
                 SELECT u.user_id
                 FROM ordered_users AS u
                 WHERE pg_advisory_xact_lock_shared(
                     ($1::bigint # u.user_id)) IS NULL
             ),
             lifecycle AS MATERIALIZED (
                 SELECT COALESCE(MAX(tombstone.state), 0)::smallint AS state
                 FROM locked_users AS locked
                 LEFT JOIN {schema.UserDeletionTombstonesTableSql} AS tombstone
                   ON tombstone.user_id = locked.user_id
             ),
             canonical AS MATERIALIZED (
                 SELECT command_id, content_fingerprint, result_kind, message_id, received_at_ms
                 FROM {schema.CommandIdempotencyLedgerTableSql}
                 WHERE sender_user_id = $2
                   AND client_message_id = $4
                 LIMIT 1
             ),
             direct_user_state AS MATERIALIZED (
                 SELECT
                     COUNT(*) FILTER (WHERE "Id" = $2) > 0 AS sender_exists,
                     COUNT(*) FILTER (WHERE "Id" = $3) > 0 AS receiver_exists,
                     COALESCE(
                         MAX("FriendRequestPolicy"::int) FILTER (WHERE "Id" = $3),
                         -1) AS privacy_policy
                 FROM public."AspNetUsers"
                 WHERE "Id" IN ($2, $3)
             ),
             direct_authorization AS MATERIALIZED (
                 SELECT CASE
                     WHEN NOT direct_user_state.sender_exists THEN 1
                     WHEN NOT direct_user_state.receiver_exists THEN 2
                     WHEN EXISTS (
                         SELECT 1
                         FROM public."T_BlockRecords"
                         WHERE "BlockerId" = $3
                           AND "BlockedUserId" = $2
                     ) THEN 3
                     WHEN direct_user_state.privacy_policy = 2 THEN 4
                     WHEN NOT (
                         EXISTS (
                             SELECT 1
                             FROM public."T_UserFriendEntry"
                             WHERE "UserId" = $2
                               AND "FriendId" = $3
                               AND NOT "IsDeleted"
                         )
                         AND EXISTS (
                             SELECT 1
                             FROM public."T_UserFriendEntry"
                             WHERE "UserId" = $3
                               AND "FriendId" = $2
                               AND NOT "IsDeleted"
                         )
                     ) THEN 5
                     ELSE 0
                 END::smallint AS decision
                 FROM direct_user_state
             ),
             write_gate AS MATERIALIZED (
                 SELECT 1
                 FROM lifecycle
                 CROSS JOIN direct_authorization
                 WHERE lifecycle.state = 0
                   AND direct_authorization.decision = 0
                   AND NOT EXISTS (SELECT 1 FROM canonical)
             ),
             upsert_conversation AS (
                 INSERT INTO {schema.ConversationsTableSql} (
                     conversation_id, type, created_at_ms, updated_at_ms,
                     last_message_id, last_message_preview, last_message_at_ms,
                     last_sender_user_id, last_sequence
                 )
                 SELECT
                     $5, $9, $6, $6,
                     $7, $8, $6,
                     $2, 1
                 FROM write_gate
                 ON CONFLICT (conversation_id) DO UPDATE SET
                     last_sequence = {schema.ConversationsTableSql}.last_sequence + 1,
                     last_message_id = CASE
                         WHEN {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                              OR ({schema.ConversationsTableSql}.last_message_at_ms,
                                  {schema.ConversationsTableSql}.last_message_id)
                                 < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                         THEN EXCLUDED.last_message_id
                         ELSE {schema.ConversationsTableSql}.last_message_id
                     END,
                     last_message_preview = CASE
                         WHEN {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                              OR ({schema.ConversationsTableSql}.last_message_at_ms,
                                  {schema.ConversationsTableSql}.last_message_id)
                                 < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                         THEN EXCLUDED.last_message_preview
                         ELSE {schema.ConversationsTableSql}.last_message_preview
                     END,
                     last_message_at_ms = CASE
                         WHEN {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                              OR ({schema.ConversationsTableSql}.last_message_at_ms,
                                  {schema.ConversationsTableSql}.last_message_id)
                                 < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                         THEN EXCLUDED.last_message_at_ms
                         ELSE {schema.ConversationsTableSql}.last_message_at_ms
                     END,
                     last_sender_user_id = CASE
                         WHEN {schema.ConversationsTableSql}.last_message_at_ms IS NULL
                              OR ({schema.ConversationsTableSql}.last_message_at_ms,
                                  {schema.ConversationsTableSql}.last_message_id)
                                 < (EXCLUDED.last_message_at_ms, EXCLUDED.last_message_id)
                         THEN EXCLUDED.last_sender_user_id
                         ELSE {schema.ConversationsTableSql}.last_sender_user_id
                     END,
                     updated_at_ms = EXCLUDED.updated_at_ms
                 RETURNING last_sequence
             ),
             ensure_receiver AS (
                 INSERT INTO {schema.ConversationMembersTableSql} (
                     conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms
                 )
                 SELECT $5, $3, $2, $6, $6
                 FROM upsert_conversation
                 ON CONFLICT (conversation_id, user_id) DO NOTHING
             ),
             sender_upsert AS (
                 INSERT INTO {schema.ConversationMembersTableSql} (
                     conversation_id, user_id, peer_user_id, joined_at_ms, last_message_at_ms, sent_count
                 )
                 SELECT $5, $2, $3, $6, $6, 1
                 FROM upsert_conversation
                 ON CONFLICT (conversation_id, user_id) DO UPDATE SET
                     sent_count = {schema.ConversationMembersTableSql}.sent_count + 1,
                     last_message_at_ms = $6
                 RETURNING sent_count
             )
             SELECT
                 lifecycle.state,
                 canonical.command_id,
                 canonical.content_fingerprint,
                 canonical.result_kind,
                 canonical.message_id,
                 canonical.received_at_ms,
                 direct_authorization.decision,
                 (SELECT last_sequence FROM upsert_conversation),
                 (SELECT sent_count FROM sender_upsert)
             FROM lifecycle
             LEFT JOIN canonical ON TRUE
             CROSS JOIN direct_authorization;
             """);
        await using var command = new NpgsqlCommand(commandText, connection, transaction);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, UserLifecycleAdvisoryLock.NamespaceKey);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, senderUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, receiverUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, clientMessageId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, conversationId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, receivedAtMs);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, messageId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, preview);
        command.Parameters.AddWithValue(NpgsqlDbType.Smallint, (short)ConversationType.Direct);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return await ReadAdmissionAsync(reader, senderUserId, clientMessageId, includesSequence: true, ct)
            .ConfigureAwait(false);
    }

    public static async Task<MessageWriteAdmission> AcquireAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        long senderUserId,
        long receiverUserId,
        string clientMessageId,
        bool requireDirectAuthorization,
        CancellationToken ct)
    {
        var commandText = schema.GetOrAddCommandText(
            "message-write-admission",
            static schema => $"""
             WITH ordered_users AS MATERIALIZED (
                 SELECT DISTINCT t.user_id
                 FROM (VALUES ($2), ($3)) AS t(user_id)
                 WHERE t.user_id > 0
                 ORDER BY t.user_id
             ),
             locked_users AS MATERIALIZED (
                 SELECT u.user_id
                 FROM ordered_users AS u
                 WHERE pg_advisory_xact_lock_shared(
                     ($1::bigint # u.user_id)) IS NULL
             ),
             lifecycle AS MATERIALIZED (
                 SELECT COALESCE(MAX(tombstone.state), 0)::smallint AS state
                 FROM locked_users AS locked
                 LEFT JOIN {schema.UserDeletionTombstonesTableSql} AS tombstone
                   ON tombstone.user_id = locked.user_id
             ),
             canonical AS MATERIALIZED (
                 SELECT command_id, content_fingerprint, result_kind, message_id, received_at_ms
                 FROM {schema.CommandIdempotencyLedgerTableSql}
                 WHERE sender_user_id = $2
                   AND client_message_id = $4
                 LIMIT 1
             ),
             direct_user_state AS MATERIALIZED (
                 SELECT
                     COUNT(*) FILTER (WHERE "Id" = $2) > 0 AS sender_exists,
                     COUNT(*) FILTER (WHERE "Id" = $3) > 0 AS receiver_exists,
                     COALESCE(
                         MAX("FriendRequestPolicy"::int) FILTER (WHERE "Id" = $3),
                         -1) AS privacy_policy
                 FROM public."AspNetUsers"
                 WHERE $5
                   AND "Id" IN ($2, $3)
             ),
             direct_authorization AS MATERIALIZED (
                 SELECT CASE
                     WHEN NOT $5 THEN NULL
                     WHEN NOT direct_user_state.sender_exists THEN 1
                     WHEN NOT direct_user_state.receiver_exists THEN 2
                     WHEN EXISTS (
                         SELECT 1
                         FROM public."T_BlockRecords"
                         WHERE "BlockerId" = $3
                           AND "BlockedUserId" = $2
                     ) THEN 3
                     WHEN direct_user_state.privacy_policy = 2 THEN 4
                     WHEN NOT (
                         EXISTS (
                             SELECT 1
                             FROM public."T_UserFriendEntry"
                             WHERE "UserId" = $2
                               AND "FriendId" = $3
                               AND NOT "IsDeleted"
                         )
                         AND EXISTS (
                             SELECT 1
                             FROM public."T_UserFriendEntry"
                             WHERE "UserId" = $3
                               AND "FriendId" = $2
                               AND NOT "IsDeleted"
                         )
                     ) THEN 5
                     ELSE 0
                 END::smallint AS decision
                 FROM direct_user_state
             )
             SELECT
                 lifecycle.state,
                 canonical.command_id,
                 canonical.content_fingerprint,
                 canonical.result_kind,
                 canonical.message_id,
                 canonical.received_at_ms,
                 direct_authorization.decision
             FROM lifecycle
             LEFT JOIN canonical ON TRUE
             CROSS JOIN direct_authorization;
             """);
        await using var command = new NpgsqlCommand(
            commandText,
            connection,
            transaction);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, UserLifecycleAdvisoryLock.NamespaceKey);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, senderUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, receiverUserId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, clientMessageId);
        command.Parameters.AddWithValue(NpgsqlDbType.Boolean, requireDirectAuthorization);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return await ReadAdmissionAsync(reader, senderUserId, clientMessageId, includesSequence: false, ct)
            .ConfigureAwait(false);
    }

    private static async Task<MessageWriteAdmission> ReadAdmissionAsync(
        NpgsqlDataReader reader,
        long senderUserId,
        string clientMessageId,
        bool includesSequence,
        CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            throw new InvalidOperationException("消息写入 admission 查询未返回结果。");

        var lifecycleState = reader.GetInt16(0);
        var lifecycle = lifecycleState switch
        {
            (short)UserLifecycleState.Frozen => LifecycleGateResult.Frozen,
            (short)UserLifecycleState.Active => LifecycleGateResult.Active,
            _ => LifecycleGateResult.Deleted
        };

        IdempotencyLedgerEntry? ledgerEntry = null;
        if (!reader.IsDBNull(1))
        {
            ledgerEntry = new IdempotencyLedgerEntry(
                SenderUserId: senderUserId,
                ClientMessageId: clientMessageId,
                CommandId: reader.GetString(1),
                ContentFingerprint: reader.GetString(2),
                ResultKind: (IdempotencyLedgerResultKind)reader.GetByte(3),
                MessageId: reader.IsDBNull(4) ? null : reader.GetString(4),
                ReceivedAtMs: reader.GetInt64(5));
        }

        DirectMessageAuthorizationDecision? authorizationDecision = reader.IsDBNull(6)
            ? null
            : (DirectMessageAuthorizationDecision)reader.GetInt16(6);

        long? conversationSequence = includesSequence && !reader.IsDBNull(7)
            ? reader.GetInt64(7)
            : null;
        long? senderSequence = includesSequence && !reader.IsDBNull(8)
            ? reader.GetInt64(8)
            : null;

        return new MessageWriteAdmission(
            lifecycle,
            ledgerEntry,
            authorizationDecision,
            conversationSequence,
            senderSequence);
    }
}

internal readonly record struct MessageWriteAdmission(
    LifecycleGateResult Lifecycle,
    IdempotencyLedgerEntry? LedgerEntry,
    DirectMessageAuthorizationDecision? DirectAuthorizationDecision,
    long? ConversationSequence = null,
    long? SenderSequence = null);
