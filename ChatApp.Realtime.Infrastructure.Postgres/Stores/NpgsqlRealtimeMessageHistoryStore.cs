using System.Data;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeMessageHistoryStore : IRealtimeMessageHistoryStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;

    public NpgsqlRealtimeMessageHistoryStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
    }

    public async Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
        long userId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        if (take is <= 0 or > 101)
            throw new ArgumentOutOfRangeException(nameof(take));

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 history.message_id,
                 history.client_message_id,
                 history.sender_user_id,
                 history.receiver_user_id,
                 history.conversation_id,
                 history.content,
                 history.received_at_ms,
                 history.delivered_at_ms,
                 history.read_at_ms,
                 history.reply_to_message_id,
                 history.reply_to_sender_user_id,
                 history.reply_to_preview,
                 history.recalled_at_ms,
                 history.forwarded_from_message_id,
                 history.forwarded_from_sender_user_id,
                 history.forwarded_from_preview,
                 history.edit_version,
                 history.edited_at_ms,
                 history.changed_at_ms,
             mentioned_user_ids,
             mentioned_roles
             FROM (
                 (
                     SELECT
                         message_id,
                         client_message_id,
                         sender_user_id,
                         receiver_user_id,
                         conversation_id,
                         content,
                         received_at_ms,
                         delivered_at_ms,
                         read_at_ms,
                         reply_to_message_id,
                         reply_to_sender_user_id,
                         reply_to_preview,
                         recalled_at_ms,
                         forwarded_from_message_id,
                         forwarded_from_sender_user_id,
                         forwarded_from_preview,
                         edit_version,
                         edited_at_ms,
                         changed_at_ms,
                     mentioned_user_ids,
                     mentioned_roles
                     FROM {_databaseSchema.MessagesTableSql}
                     WHERE receiver_user_id = @user_id
                       AND (
                           @before_received_at_ms IS NULL
                           OR received_at_ms < @before_received_at_ms
                           OR (
                               received_at_ms = @before_received_at_ms
                               AND message_id < @before_message_id
                           )
                       )
                     ORDER BY received_at_ms DESC, message_id DESC
                     LIMIT @take
                 )
                 UNION ALL
                 (
                     SELECT
                         message_id,
                         client_message_id,
                         sender_user_id,
                         receiver_user_id,
                         conversation_id,
                         content,
                         received_at_ms,
                         delivered_at_ms,
                         read_at_ms,
                         reply_to_message_id,
                         reply_to_sender_user_id,
                         reply_to_preview,
                         recalled_at_ms,
                         forwarded_from_message_id,
                         forwarded_from_sender_user_id,
                         forwarded_from_preview,
                         edit_version,
                         edited_at_ms,
                         changed_at_ms,
                     mentioned_user_ids,
                     mentioned_roles
                     FROM {_databaseSchema.MessagesTableSql}
                     WHERE sender_user_id = @user_id
                       AND receiver_user_id <> @user_id
                       AND (
                           @before_received_at_ms IS NULL
                           OR received_at_ms < @before_received_at_ms
                           OR (
                               received_at_ms = @before_received_at_ms
                               AND message_id < @before_message_id
                           )
                       )
                     ORDER BY received_at_ms DESC, message_id DESC
                     LIMIT @take
                 )
             ) AS history
             ORDER BY history.received_at_ms DESC, history.message_id DESC
             LIMIT @take;
             """,
            connection);

        BindCursorParameters(command, beforeReceivedAtMs, beforeMessageId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("take", take);

        return await ReadMessagesAsync(command, take, ct).ConfigureAwait(false);
    }

    public async Task<ConversationMessageHistoryResult> QueryByConversationAsync(
        long userId,
        string conversationId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);
        if (take is <= 0 or > 101)
            throw new ArgumentOutOfRangeException(nameof(take));

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             WITH membership AS (
                 SELECT EXISTS (
                     SELECT 1
                     FROM {_databaseSchema.ConversationMembersTableSql}
                     WHERE conversation_id = @conversation_id
                       AND user_id = @user_id
                 ) AS is_member
             )
             SELECT
                 membership.is_member,
                 msg.message_id,
                 msg.client_message_id,
                 msg.sender_user_id,
                 msg.receiver_user_id,
                 msg.conversation_id,
                 msg.content,
                 msg.received_at_ms,
                 msg.delivered_at_ms,
                 msg.read_at_ms,
                 msg.reply_to_message_id,
                 msg.reply_to_sender_user_id,
                 msg.reply_to_preview,
                 msg.recalled_at_ms,
                 msg.forwarded_from_message_id,
                 msg.forwarded_from_sender_user_id,
                 msg.forwarded_from_preview,
                 msg.edit_version,
                 msg.edited_at_ms,
                 msg.changed_at_ms,
             mentioned_user_ids,
             mentioned_roles
             FROM membership
             LEFT JOIN LATERAL (
                 SELECT
                     message_id,
                     client_message_id,
                     sender_user_id,
                     receiver_user_id,
                     conversation_id,
                     content,
                     received_at_ms,
                     delivered_at_ms,
                     read_at_ms,
                     reply_to_message_id,
                     reply_to_sender_user_id,
                     reply_to_preview,
                     recalled_at_ms,
                     forwarded_from_message_id,
                     forwarded_from_sender_user_id,
                     forwarded_from_preview,
                     edit_version,
                     edited_at_ms,
                     changed_at_ms,
                 mentioned_user_ids,
                 mentioned_roles
                 FROM {_databaseSchema.MessagesTableSql}
                 WHERE conversation_id = @conversation_id
                   AND membership.is_member
                   AND (
                        @before_received_at_ms IS NULL
                        OR received_at_ms < @before_received_at_ms
                        OR (
                            received_at_ms = @before_received_at_ms
                            AND message_id < @before_message_id
                        )
                   )
                 ORDER BY received_at_ms DESC, message_id DESC
                 LIMIT @take
             ) AS msg ON TRUE;
             """,
            connection);

        BindCursorParameters(command, beforeReceivedAtMs, beforeMessageId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("conversation_id", conversationId.Trim());
        command.Parameters.AddWithValue("take", take);

        return await ReadConversationMessagesAsync(command, take, ct).ConfigureAwait(false);
    }

    public async Task<ConversationMessageHistoryResult> QueryByConversationAfterAsync(
        long userId,
        string conversationId,
        long afterChangedAtMs,
        string afterMessageId,
        int take,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(afterMessageId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(afterChangedAtMs);
        if (take is <= 0 or > 101)
            throw new ArgumentOutOfRangeException(nameof(take));

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             WITH membership AS (
                 SELECT EXISTS (
                     SELECT 1
                     FROM {_databaseSchema.ConversationMembersTableSql}
                     WHERE conversation_id = @conversation_id
                       AND user_id = @user_id
                 ) AS is_member
             )
             SELECT
                 membership.is_member,
                 msg.message_id,
                 msg.client_message_id,
                 msg.sender_user_id,
                 msg.receiver_user_id,
                 msg.conversation_id,
                 msg.content,
                 msg.received_at_ms,
                 msg.delivered_at_ms,
                 msg.read_at_ms,
                 msg.reply_to_message_id,
                 msg.reply_to_sender_user_id,
                 msg.reply_to_preview,
                 msg.recalled_at_ms,
                 msg.forwarded_from_message_id,
                 msg.forwarded_from_sender_user_id,
                 msg.forwarded_from_preview,
                 msg.edit_version,
                 msg.edited_at_ms,
                 msg.changed_at_ms,
             mentioned_user_ids,
             mentioned_roles
             FROM membership
             LEFT JOIN LATERAL (
                 SELECT
                     message_id,
                     client_message_id,
                     sender_user_id,
                     receiver_user_id,
                     conversation_id,
                     content,
                     received_at_ms,
                     delivered_at_ms,
                     read_at_ms,
                     reply_to_message_id,
                     reply_to_sender_user_id,
                     reply_to_preview,
                     recalled_at_ms,
                     forwarded_from_message_id,
                     forwarded_from_sender_user_id,
                     forwarded_from_preview,
                     edit_version,
                     edited_at_ms,
                     changed_at_ms,
                 mentioned_user_ids,
                 mentioned_roles
                 FROM {_databaseSchema.MessagesTableSql}
                 WHERE conversation_id = @conversation_id
                   AND membership.is_member
                   AND (
                        changed_at_ms > @after_changed_at_ms
                        OR (
                            changed_at_ms = @after_changed_at_ms
                            AND message_id > @after_message_id
                        )
                   )
                 ORDER BY changed_at_ms ASC, message_id ASC
                 LIMIT @take
             ) AS msg ON TRUE;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("conversation_id", conversationId.Trim());
        command.Parameters.AddWithValue("after_changed_at_ms", afterChangedAtMs);
        command.Parameters.AddWithValue("after_message_id", afterMessageId.Trim());
        command.Parameters.AddWithValue("take", take);

        return await ReadConversationMessagesAsync(command, take, ct).ConfigureAwait(false);
    }

    public async Task<bool> IsConversationMemberAsync(
        long userId,
        string conversationId,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT 1
             FROM {_databaseSchema.ConversationMembersTableSql}
             WHERE conversation_id = @conversation_id
               AND user_id = @user_id
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("conversation_id", conversationId.Trim());
        command.Parameters.AddWithValue("user_id", userId);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is not null;
    }

    public async Task<IReadOnlySet<string>> FilterMemberConversationIdsAsync(
        long userId,
        IReadOnlyCollection<string> conversationIds,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        if (conversationIds.Count == 0)
            return new HashSet<string>(StringComparer.Ordinal);

        var normalized = conversationIds
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Select(static id => id.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (normalized.Length == 0)
            return new HashSet<string>(StringComparer.Ordinal);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT conversation_id
             FROM {_databaseSchema.ConversationMembersTableSql}
             WHERE user_id = @user_id
               AND conversation_id = ANY(@conversation_ids);
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        var ids = command.Parameters.Add("conversation_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
        ids.Value = normalized;

        var members = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            members.Add(reader.GetString(0));

        return members;
    }

    public async Task<IReadOnlyDictionary<string, IReadOnlyList<RealtimeHistoryMessage>>> QueryCatchUpsAsync(
        long userId,
        IReadOnlyList<HistoryCatchUpQuery> queries,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        var map = new Dictionary<string, IReadOnlyList<RealtimeHistoryMessage>>(
            queries.Count,
            StringComparer.Ordinal);
        if (queries.Count == 0)
            return map;

        // Normalize + de-dupe by conversation; keep first valid take/watermark.
        var normalized = new List<(string ConversationId, long? AfterAt, string? AfterId, int Take)>(queries.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var query in queries)
        {
            if (string.IsNullOrWhiteSpace(query.ConversationId) || query.Take is <= 0 or > 101)
            {
                map[query.ConversationId ?? string.Empty] = Array.Empty<RealtimeHistoryMessage>();
                continue;
            }

            var conversationId = query.ConversationId.Trim();
            if (!seen.Add(conversationId))
            {
                map.TryAdd(conversationId, Array.Empty<RealtimeHistoryMessage>());
                continue;
            }

            long? afterAt = query.AfterReceivedAtMs;
            string? afterId = string.IsNullOrWhiteSpace(query.AfterMessageId)
                ? null
                : query.AfterMessageId.Trim();
            if (afterAt is null || afterId is null)
            {
                afterAt = null;
                afterId = null;
            }

            normalized.Add((conversationId, afterAt, afterId, query.Take));
            map[conversationId] = Array.Empty<RealtimeHistoryMessage>();
        }

        if (normalized.Count == 0)
            return map;

        try
        {
            await QueryCatchUpsBatchedAsync(userId, normalized, map, ct).ConfigureAwait(false);
        }
        catch
        {
            // ????????????????? SQL / ??????
            foreach (var item in normalized)
            {
                ct.ThrowIfCancellationRequested();
                ConversationMessageHistoryResult result;
                if (item.AfterAt is long afterAt && item.AfterId is not null)
                {
                    result = await QueryByConversationAfterAsync(
                            userId,
                            item.ConversationId,
                            afterAt,
                            item.AfterId,
                            item.Take,
                            ct)
                        .ConfigureAwait(false);
                }
                else
                {
                    result = await QueryByConversationAsync(
                            userId,
                            item.ConversationId,
                            beforeReceivedAtMs: null,
                            beforeMessageId: null,
                            take: item.Take,
                            ct)
                        .ConfigureAwait(false);
                }

                map[item.ConversationId] = result.IsMember
                    ? result.Messages
                    : Array.Empty<RealtimeHistoryMessage>();
            }
        }

        return map;
    }

    private async Task QueryCatchUpsBatchedAsync(
        long userId,
        IReadOnlyList<(string ConversationId, long? AfterAt, string? AfterId, int Take)> requests,
        Dictionary<string, IReadOnlyList<RealtimeHistoryMessage>> map,
        CancellationToken ct)
    {
        var conversationIds = new string[requests.Count];
        var afterAts = new long[requests.Count];
        var afterIds = new string[requests.Count];
        var hasAfter = new bool[requests.Count];
        var takes = new int[requests.Count];
        for (var i = 0; i < requests.Count; i++)
        {
            var item = requests[i];
            conversationIds[i] = item.ConversationId;
            takes[i] = item.Take;
            if (item.AfterAt is long at && item.AfterId is not null)
            {
                hasAfter[i] = true;
                afterAts[i] = at;
                afterIds[i] = item.AfterId;
            }
            else
            {
                hasAfter[i] = false;
                afterAts[i] = 0;
                afterIds[i] = string.Empty;
            }
        }

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             WITH requests AS (
                 SELECT
                     t.ord,
                     t.conversation_id,
                     t.has_after,
                     t.after_at,
                     t.after_id,
                     t.take
                 FROM unnest(
                     @conversation_ids::text[],
                     @has_after::boolean[],
                     @after_ats::bigint[],
                     @after_ids::text[],
                     @takes::int[]
                 ) WITH ORDINALITY AS t(
                     conversation_id, has_after, after_at, after_id, take, ord
                 )
             ),
             membership AS (
                 SELECT
                     r.ord,
                     r.conversation_id,
                     r.has_after,
                     r.after_at,
                     r.after_id,
                     r.take,
                     EXISTS (
                         SELECT 1
                         FROM {_databaseSchema.ConversationMembersTableSql} AS m
                         WHERE m.conversation_id = r.conversation_id
                           AND m.user_id = @user_id
                     ) AS is_member
                 FROM requests r
             )
             SELECT
                 membership.conversation_id,
                 membership.is_member,
                 msg.message_id,
                 msg.client_message_id,
                 msg.sender_user_id,
                 msg.receiver_user_id,
                 msg.conversation_id,
                 msg.content,
                 msg.received_at_ms,
                 msg.delivered_at_ms,
                 msg.read_at_ms,
                 msg.reply_to_message_id,
                 msg.reply_to_sender_user_id,
                 msg.reply_to_preview,
                 msg.recalled_at_ms,
                 msg.forwarded_from_message_id,
                 msg.forwarded_from_sender_user_id,
                 msg.forwarded_from_preview,
                 msg.edit_version,
                 msg.edited_at_ms,
                 msg.changed_at_ms,
             mentioned_user_ids,
             mentioned_roles
             FROM membership
             LEFT JOIN LATERAL (
                 SELECT
                     message_id,
                     client_message_id,
                     sender_user_id,
                     receiver_user_id,
                     conversation_id,
                     content,
                     received_at_ms,
                     delivered_at_ms,
                     read_at_ms,
                     reply_to_message_id,
                     reply_to_sender_user_id,
                     reply_to_preview,
                     recalled_at_ms,
                     forwarded_from_message_id,
                     forwarded_from_sender_user_id,
                     forwarded_from_preview,
                     edit_version,
                     edited_at_ms,
                     changed_at_ms,
                 mentioned_user_ids,
                 mentioned_roles
                 FROM {_databaseSchema.MessagesTableSql}
                 WHERE conversation_id = membership.conversation_id
                   AND membership.is_member
                   AND (
                        CASE
                            WHEN membership.has_after THEN
                                changed_at_ms > membership.after_at
                                OR (
                                    changed_at_ms = membership.after_at
                                    AND message_id > membership.after_id
                                )
                            ELSE TRUE
                        END
                   )
                 ORDER BY
                     CASE WHEN membership.has_after THEN changed_at_ms END ASC,
                     CASE WHEN membership.has_after THEN message_id END ASC,
                     CASE WHEN NOT membership.has_after THEN received_at_ms END DESC,
                     CASE WHEN NOT membership.has_after THEN message_id END DESC
                 LIMIT membership.take
             ) AS msg ON TRUE
             ORDER BY membership.ord,
                      CASE WHEN membership.has_after THEN msg.changed_at_ms END ASC,
                      CASE WHEN membership.has_after THEN msg.message_id END ASC,
                      CASE WHEN NOT membership.has_after THEN msg.received_at_ms END DESC,
                      CASE WHEN NOT membership.has_after THEN msg.message_id END DESC;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("conversation_ids", conversationIds);
        command.Parameters.AddWithValue("has_after", hasAfter);
        command.Parameters.AddWithValue("after_ats", afterAts);
        command.Parameters.AddWithValue("after_ids", afterIds);
        command.Parameters.AddWithValue("takes", takes);

        var buckets = new Dictionary<string, List<RealtimeHistoryMessage>>(
            requests.Count,
            StringComparer.Ordinal);
        await using var reader = await command
            .ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var conversationId = reader.GetString(0);
            var isMember = reader.GetBoolean(1);
            if (!isMember || reader.IsDBNull(2))
            {
                buckets.TryAdd(conversationId, new List<RealtimeHistoryMessage>());
                continue;
            }

            if (!buckets.TryGetValue(conversationId, out var list))
            {
                list = new List<RealtimeHistoryMessage>(8);
                buckets[conversationId] = list;
            }

            list.Add(ReadMessage(reader, offset: 2));
        }

        foreach (var item in requests)
        {
            map[item.ConversationId] = buckets.TryGetValue(item.ConversationId, out var list)
                ? list
                : Array.Empty<RealtimeHistoryMessage>();
        }
    }

    public async Task<RealtimeHistoryMessage?> TryGetByIdAsync(
        string messageId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 message_id,
                 client_message_id,
                 sender_user_id,
                 receiver_user_id,
                 conversation_id,
                 content,
                 received_at_ms,
                 delivered_at_ms,
                 read_at_ms,
                 reply_to_message_id,
                 reply_to_sender_user_id,
                 reply_to_preview,
                 recalled_at_ms,
                 forwarded_from_message_id,
                 forwarded_from_sender_user_id,
                 forwarded_from_preview,
                 edit_version,
                 edited_at_ms,
                 changed_at_ms,
             mentioned_user_ids,
             mentioned_roles
             FROM {_databaseSchema.MessagesTableSql}
             WHERE message_id = @message_id
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId.Trim());

        await using var reader = await command
            .ExecuteReaderAsync(CommandBehavior.SingleRow, ct)
            .ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return ReadMessage(reader, offset: 0);
    }

    public async Task<IReadOnlyDictionary<string, ResolvedSyncWatermark>> ResolveSyncWatermarksAsync(
        IReadOnlyList<ConversationSyncWatermarkInput> watermarks,
        CancellationToken ct = default)
    {
        var result = new Dictionary<string, ResolvedSyncWatermark>(StringComparer.Ordinal);
        if (watermarks.Count == 0)
            return result;

        var normalized = new List<ConversationSyncWatermarkInput>(watermarks.Count);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in watermarks)
        {
            if (string.IsNullOrWhiteSpace(item.ConversationId)
                || string.IsNullOrWhiteSpace(item.AfterMessageId)
                || item.AfterReceivedAtMs <= 0)
            {
                continue;
            }

            var conversationId = item.ConversationId.Trim();
            if (!seen.Add(conversationId))
                continue;

            normalized.Add(new ConversationSyncWatermarkInput
            {
                ConversationId = conversationId,
                AfterReceivedAtMs = item.AfterReceivedAtMs,
                AfterMessageId = item.AfterMessageId.Trim(),
                TipReceivedAtMs = item.TipReceivedAtMs,
                TipMessageId = string.IsNullOrWhiteSpace(item.TipMessageId)
                    ? null
                    : item.TipMessageId.Trim()
            });
        }

        if (normalized.Count == 0)
            return result;

        var conversationIds = new string[normalized.Count];
        var afterAts = new long[normalized.Count];
        var afterIds = new string[normalized.Count];
        var tipAts = new long[normalized.Count];
        var tipIds = new string[normalized.Count];
        var hasTipHints = new bool[normalized.Count];
        for (var i = 0; i < normalized.Count; i++)
        {
            var item = normalized[i];
            conversationIds[i] = item.ConversationId;
            afterAts[i] = item.AfterReceivedAtMs;
            afterIds[i] = item.AfterMessageId;
            if (item.TipReceivedAtMs is > 0
                && !string.IsNullOrWhiteSpace(item.TipMessageId))
            {
                hasTipHints[i] = true;
                tipAts[i] = item.TipReceivedAtMs.Value;
                tipIds[i] = item.TipMessageId!;
            }
        }

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             WITH inputs AS (
                 SELECT
                     t.conversation_id,
                     t.after_at,
                     t.after_id,
                     t.has_tip_hint,
                     t.tip_at,
                     t.tip_id
                 FROM unnest(
                     @conversation_ids::text[],
                     @after_ats::bigint[],
                     @after_ids::text[],
                     @has_tip_hints::boolean[],
                     @tip_ats::bigint[],
                     @tip_ids::text[]
                 ) AS t(
                     conversation_id, after_at, after_id, has_tip_hint, tip_at, tip_id
                 )
             ),
             tips AS (
                 SELECT
                     i.conversation_id,
                     i.after_at,
                     i.after_id,
                     CASE
                         WHEN i.has_tip_hint THEN i.tip_at
                         ELSE c.last_message_at_ms
                     END AS tip_at,
                     CASE
                         WHEN i.has_tip_hint THEN i.tip_id
                         ELSE c.last_message_id
                     END AS tip_id
                 FROM inputs i
                 LEFT JOIN {_databaseSchema.ConversationsTableSql} c
                     ON c.conversation_id = i.conversation_id
             ),
             resolved AS (
                 SELECT
                     t.conversation_id,
                     t.after_at,
                     t.after_id,
                     t.tip_at,
                     t.tip_id,
                     m.received_at_ms AS msg_at,
                     m.message_id AS msg_id
                 FROM tips t
                 LEFT JOIN {_databaseSchema.MessagesTableSql} m
                     ON m.conversation_id = t.conversation_id
                    AND m.message_id = t.after_id
             )
             SELECT
                 conversation_id,
                 after_at,
                 after_id,
                 tip_at,
                 tip_id,
                 msg_at,
                 msg_id
             FROM resolved;
             """,
            connection);
        command.Parameters.AddWithValue("conversation_ids", conversationIds);
        command.Parameters.AddWithValue("after_ats", afterAts);
        command.Parameters.AddWithValue("after_ids", afterIds);
        command.Parameters.AddWithValue("has_tip_hints", hasTipHints);
        command.Parameters.AddWithValue("tip_ats", tipAts);
        command.Parameters.AddWithValue("tip_ids", tipIds);

        await using var reader = await command
            .ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var conversationId = reader.GetString(0);
            var clientAfterAt = reader.IsDBNull(1) ? 0L : reader.GetInt64(1);
            var clientAfterId = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
            long? tipAt = reader.IsDBNull(3) ? null : reader.GetInt64(3);
            string? tipId = reader.IsDBNull(4) ? null : reader.GetString(4);
            if (tipAt is not > 0 || string.IsNullOrWhiteSpace(tipId))
            {
                result[conversationId] = new ResolvedSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = 0,
                    AfterMessageId = string.Empty,
                    IsValid = false,
                    InvalidationKind = SyncWatermarkInvalidationKind.MessageNotFound,
                    TipReceivedAtMs = tipAt,
                    TipMessageId = tipId,
                    ClientAfterReceivedAtMs = clientAfterAt,
                    ClientAfterMessageId = clientAfterId
                };
                continue;
            }

            if (!reader.IsDBNull(5) && !reader.IsDBNull(6))
            {
                var msgAt = reader.GetInt64(5);
                var msgId = reader.GetString(6);
                if (IsAfter(msgAt, msgId, tipAt.Value, tipId))
                {
                    result[conversationId] = new ResolvedSyncWatermark
                    {
                        ConversationId = conversationId,
                        AfterReceivedAtMs = tipAt.Value,
                        AfterMessageId = tipId,
                        IsValid = false,
                        InvalidationKind = SyncWatermarkInvalidationKind.AheadOfTip,
                        TipReceivedAtMs = tipAt,
                        TipMessageId = tipId,
                        ClientAfterReceivedAtMs = clientAfterAt,
                        ClientAfterMessageId = clientAfterId
                    };
                    continue;
                }

                result[conversationId] = new ResolvedSyncWatermark
                {
                    ConversationId = conversationId,
                    AfterReceivedAtMs = msgAt,
                    AfterMessageId = msgId,
                    IsValid = true,
                    TipReceivedAtMs = tipAt,
                    TipMessageId = tipId,
                    ClientAfterReceivedAtMs = clientAfterAt,
                    ClientAfterMessageId = clientAfterId
                };
                continue;
            }

            result[conversationId] = new ResolvedSyncWatermark
            {
                ConversationId = conversationId,
                AfterReceivedAtMs = tipAt.Value,
                AfterMessageId = tipId,
                IsValid = false,
                InvalidationKind = SyncWatermarkInvalidationKind.MessageNotFound,
                TipReceivedAtMs = tipAt,
                TipMessageId = tipId,
                ClientAfterReceivedAtMs = clientAfterAt,
                ClientAfterMessageId = clientAfterId
            };
        }

        return result;
    }

    private static bool IsAfter(long at, string id, long tipAt, string tipId) =>
        at > tipAt || (at == tipAt && string.CompareOrdinal(id, tipId) > 0);

    private static void BindCursorParameters(
        NpgsqlCommand command,
        long? beforeReceivedAtMs,
        string? beforeMessageId)
    {
        command.Parameters
            .Add("before_received_at_ms", NpgsqlDbType.Bigint)
            .Value = beforeReceivedAtMs.HasValue
                ? beforeReceivedAtMs.Value
                : DBNull.Value;
        command.Parameters
            .Add("before_message_id", NpgsqlDbType.Varchar)
            .Value = beforeMessageId is not null
                ? beforeMessageId
                : DBNull.Value;
    }

    private static async Task<IReadOnlyList<RealtimeHistoryMessage>> ReadMessagesAsync(
        NpgsqlCommand command,
        int take,
        CancellationToken ct)
    {
        var messages = new List<RealtimeHistoryMessage>(take);
        await using var reader = await command
            .ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            messages.Add(ReadMessage(reader, offset: 0));

        return messages;
    }

    private static async Task<ConversationMessageHistoryResult> ReadConversationMessagesAsync(
        NpgsqlCommand command,
        int take,
        CancellationToken ct)
    {
        await using var reader = await command
            .ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct)
            .ConfigureAwait(false);

        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return ConversationMessageHistoryResult.Forbidden;

        var isMember = reader.GetBoolean(0);
        if (!isMember)
            return ConversationMessageHistoryResult.Forbidden;

        var messages = new List<RealtimeHistoryMessage>(Math.Min(take, 16));
        if (!reader.IsDBNull(1))
            messages.Add(ReadMessage(reader, offset: 1));

        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (reader.IsDBNull(1))
                continue;
            messages.Add(ReadMessage(reader, offset: 1));
        }

        return ConversationMessageHistoryResult.Ok(messages);
    }

    private static RealtimeHistoryMessage ReadMessage(NpgsqlDataReader reader, int offset) =>
        new()
        {
            MessageId = reader.GetString(offset),
            ClientMessageId = reader.GetString(offset + 1),
            SenderUserId = reader.GetInt64(offset + 2),
            ReceiverUserId = reader.GetInt64(offset + 3),
            ConversationId = reader.IsDBNull(offset + 4) ? null : reader.GetString(offset + 4),
            Content = reader.GetString(offset + 5),
            ReceivedAtMs = reader.GetInt64(offset + 6),
            DeliveredAtMs = reader.IsDBNull(offset + 7) ? null : reader.GetInt64(offset + 7),
            ReadAtMs = reader.IsDBNull(offset + 8) ? null : reader.GetInt64(offset + 8),
            ReplyToMessageId = reader.IsDBNull(offset + 9) ? null : reader.GetString(offset + 9),
            ReplyToSenderUserId = reader.IsDBNull(offset + 10) ? null : reader.GetInt64(offset + 10),
            ReplyToPreview = reader.IsDBNull(offset + 11) ? null : reader.GetString(offset + 11),
            RecalledAtMs = reader.IsDBNull(offset + 12) ? null : reader.GetInt64(offset + 12),
            ForwardedFromMessageId = reader.IsDBNull(offset + 13) ? null : reader.GetString(offset + 13),
            ForwardedFromSenderUserId = reader.IsDBNull(offset + 14) ? null : reader.GetInt64(offset + 14),
            ForwardedFromPreview = reader.IsDBNull(offset + 15) ? null : reader.GetString(offset + 15),
            EditVersion = reader.IsDBNull(offset + 16) ? 1 : reader.GetInt32(offset + 16),
            EditedAtMs = reader.IsDBNull(offset + 17) ? null : reader.GetInt64(offset + 17),
            ChangedAtMs = reader.IsDBNull(offset + 18)
                ? reader.GetInt64(offset + 6)
                : reader.GetInt64(offset + 18),
            MentionedUserIds = reader.IsDBNull(offset + 19)
                ? null
                : reader.GetFieldValue<long[]>(offset + 19),
            MentionedRoles = reader.IsDBNull(offset + 20)
                ? null
                : reader.GetFieldValue<string[]>(offset + 20)
        };
}
