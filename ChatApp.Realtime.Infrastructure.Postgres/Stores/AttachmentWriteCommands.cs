using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 附件绑定 SQL（消息 SaveAsync 同事务复用）。
/// </summary>
internal static class AttachmentWriteCommands
{
    public const int MaxAttachmentsPerMessage = 32;

    /// <summary>
    /// 绑定 Confirmed → Bound，并通过 RETURNING 一次取回线协议所需字段。
    /// </summary>
    public static async Task<IReadOnlyList<RealtimeAttachmentRecord>> BindConfirmedToMessageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        string messageId,
        string? conversationId,
        long uploaderUserId,
        IReadOnlyList<string> attachmentIds,
        CancellationToken ct)
    {
        if (attachmentIds.Count == 0)
            return [];

        var distinctIds = attachmentIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (distinctIds.Length == 0)
            return [];
        if (distinctIds.Length > MaxAttachmentsPerMessage)
            throw new InvalidOperationException(
                $"单条消息附件数不能超过 {MaxAttachmentsPerMessage}。");

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {schema.AttachmentsTableSql}
             SET message_id = @message_id,
                 conversation_id = @conversation_id,
                 status = @bound_status,
                 bound_at_ms = @bound_at_ms
             WHERE attachment_id = ANY(@attachment_ids)
               AND uploader_user_id = @uploader_user_id
               AND status = @confirmed_status
             RETURNING attachment_id, uploader_user_id, object_key, public_url, content_type,
                       size_bytes, original_name, status, message_id, conversation_id,
                       client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                       content_hash;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue(
            "conversation_id",
            (object?)conversationId ?? DBNull.Value);
        command.Parameters.AddWithValue("bound_status", (short)AttachmentStatus.Bound);
        command.Parameters.AddWithValue("bound_at_ms", now);
        command.Parameters.AddWithValue("uploader_user_id", uploaderUserId);
        command.Parameters.AddWithValue("confirmed_status", (short)AttachmentStatus.Confirmed);
        var idsParam = command.Parameters.Add("attachment_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
        idsParam.Value = distinctIds;

        var records = new List<RealtimeAttachmentRecord>(distinctIds.Length);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            records.Add(new RealtimeAttachmentRecord
            {
                AttachmentId = reader.GetString(0),
                UploaderUserId = reader.GetInt64(1),
                ObjectKey = reader.GetString(2),
                PublicUrl = reader.IsDBNull(3) ? null : reader.GetString(3),
                ContentType = reader.GetString(4),
                SizeBytes = reader.GetInt64(5),
                OriginalName = reader.IsDBNull(6) ? null : reader.GetString(6),
                Status = (AttachmentStatus)reader.GetInt16(7),
                MessageId = reader.IsDBNull(8) ? null : reader.GetString(8),
                ConversationId = reader.IsDBNull(9) ? null : reader.GetString(9),
                ClientAttachmentId = reader.IsDBNull(10) ? null : reader.GetString(10),
                CreatedAtMs = reader.GetInt64(11),
                ConfirmedAtMs = reader.IsDBNull(12) ? null : reader.GetInt64(12),
                BoundAtMs = reader.IsDBNull(13) ? null : reader.GetInt64(13),
                ContentHash = reader.FieldCount > 14 && !reader.IsDBNull(14)
                    ? reader.GetString(14)
                    : null
            });
        }

        return records;
    }
}
