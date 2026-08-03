using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 三-1/2/3/4：基于 PostgreSQL 的消息已读回执查询实现。
/// <para>
/// 利用 conversation_members.last_read_sequence 推导已读者，不需要新表。
/// </para>
/// </summary>
public sealed class NpgsqlRealtimeReadReceiptStore : IRealtimeReadReceiptStore
{
    /// <summary>
    /// 三-1/2：小群阈值。成员数 ≤ 此值时返回完整 reader list，超过时返回 aggregate count。
    /// </summary>
    public const int SmallGroupThreshold = 200;

    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _schema;

    public NpgsqlRealtimeReadReceiptStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema schema)
    {
        _databaseClient = databaseClient;
        _schema = schema;
    }

    public async Task<MessageReaderPage> GetReadersAsync(
        string conversationId,
        long conversationSequence,
        long viewerUserId,
        long? cursor,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);
        pageSize = Math.Clamp(pageSize, 1, 100);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        // 四-1：JOIN messages 表验证 viewerUserId 是消息发送者，且消息属于该会话。
        // 同时检查 membership 在消息产生时已入群且未离群。
        var sql = $"""
            SELECT cm.user_id, cm.last_read_at_ms
            FROM {_schema.ConversationMembersTableSql} cm
            INNER JOIN {_schema.MessagesTableSql} m
                ON m.conversation_id = cm.conversation_id
                AND m.conversation_sequence = @conversation_sequence
            LEFT JOIN {_schema.MembershipPeriodsTableSql} mp
                ON mp.conversation_id = cm.conversation_id
                AND mp.user_id = cm.user_id
                AND mp.joined_at_ms <= m.created_at_ms
                AND (mp.left_at_ms IS NULL OR mp.left_at_ms > m.created_at_ms)
            WHERE cm.conversation_id = @conversation_id
              AND cm.user_id <> @viewer_user_id
              AND m.sender_user_id = @viewer_user_id
              AND cm.last_read_sequence IS NOT NULL
              AND cm.last_read_sequence >= @conversation_sequence
              AND cm.left_at_ms IS NULL
              AND (@cursor IS NULL OR cm.user_id > @cursor)
            ORDER BY cm.user_id
            LIMIT @page_size;
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("conversation_sequence", conversationSequence);
        command.Parameters.AddWithValue("viewer_user_id", viewerUserId);
        command.Parameters.AddWithValue("cursor", (object?)cursor ?? DBNull.Value);
        command.Parameters.AddWithValue("page_size", pageSize + 1); // 多取一条判断 HasMore

        var readers = new List<MessageReader>(pageSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            readers.Add(new MessageReader
            {
                UserId = reader.GetInt64(0),
                ReadAtMs = reader.IsDBNull(1) ? 0 : reader.GetInt64(1)
            });
        }

        var hasMore = readers.Count > pageSize;
        if (hasMore)
            readers.RemoveAt(readers.Count - 1);

        var nextCursor = hasMore && readers.Count > 0
            ? (long?)readers[^1].UserId
            : null;

        return new MessageReaderPage
        {
            Readers = readers,
            NextCursor = nextCursor,
            HasMore = hasMore
        };
    }

    public async Task<MessageReadSummary> GetReadSummaryAsync(
        string conversationId,
        long conversationSequence,
        long viewerUserId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        var sql = $"""
            SELECT
                COUNT(*) FILTER (WHERE cm.last_read_sequence IS NOT NULL
                                  AND cm.last_read_sequence >= @conversation_sequence) AS read_count,
                COUNT(*) AS total_count
            FROM {_schema.ConversationMembersTableSql} cm
            INNER JOIN {_schema.MessagesTableSql} m
                ON m.conversation_id = cm.conversation_id
                AND m.conversation_sequence = @conversation_sequence
            LEFT JOIN {_schema.MembershipPeriodsTableSql} mp
                ON mp.conversation_id = cm.conversation_id
                AND mp.user_id = cm.user_id
                AND mp.joined_at_ms <= m.created_at_ms
                AND (mp.left_at_ms IS NULL OR mp.left_at_ms > m.created_at_ms)
            WHERE cm.conversation_id = @conversation_id
              AND cm.user_id <> @viewer_user_id
              AND m.sender_user_id = @viewer_user_id
              AND cm.left_at_ms IS NULL;
            """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("conversation_sequence", conversationSequence);
        command.Parameters.AddWithValue("viewer_user_id", viewerUserId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return new MessageReadSummary { ReadCount = 0, TotalMemberCount = 0, IsSmallGroup = true };

        // 四-2：COUNT(*) 返回 bigint，使用 GetInt64 后做 checked/clamp 转换。
        var readCount64 = reader.GetInt64(0);
        var totalCount64 = reader.GetInt64(1);
        var readCount = checked((int)readCount64);
        var totalCount = checked((int)totalCount64);

        return new MessageReadSummary
        {
            ReadCount = readCount,
            TotalMemberCount = totalCount,
            IsSmallGroup = totalCount <= SmallGroupThreshold
        };
    }
}
