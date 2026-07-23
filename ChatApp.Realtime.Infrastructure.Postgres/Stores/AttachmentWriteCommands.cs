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

    public static async Task<int> BindConfirmedToMessageAsync(
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
            return 0;

        var distinctIds = attachmentIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (distinctIds.Length == 0)
            return 0;
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
               AND status = @confirmed_status;
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

        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
