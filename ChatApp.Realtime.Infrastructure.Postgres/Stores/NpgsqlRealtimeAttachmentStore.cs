using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeAttachmentStore : IRealtimeAttachmentStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly ILogger<NpgsqlRealtimeAttachmentStore> _logger;

    public NpgsqlRealtimeAttachmentStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        ILogger<NpgsqlRealtimeAttachmentStore> logger)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        _logger = logger;
    }

    public async Task<RealtimeAttachmentRecord> InsertConfirmedAsync(
        RealtimeAttachmentRecord attachment,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(attachment);
        ArgumentException.ThrowIfNullOrWhiteSpace(attachment.AttachmentId);
        ArgumentException.ThrowIfNullOrWhiteSpace(attachment.ObjectKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(attachment.ContentType);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(attachment.UploaderUserId);
        ArgumentOutOfRangeException.ThrowIfNegative(attachment.SizeBytes);

        var now = attachment.CreatedAtMs > 0
            ? attachment.CreatedAtMs
            : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var confirmedAt = attachment.ConfirmedAtMs ?? now;
        var clientAttachmentId = string.IsNullOrWhiteSpace(attachment.ClientAttachmentId)
            ? null
            : attachment.ClientAttachmentId.Trim();

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        if (clientAttachmentId is not null)
        {
            var existing = await TryGetByUploaderClientAsync(
                    connection,
                    attachment.UploaderUserId,
                    clientAttachmentId,
                    ct)
                .ConfigureAwait(false);
            if (existing is not null)
            {
                if (!string.Equals(existing.ObjectKey, attachment.ObjectKey, StringComparison.Ordinal)
                    || !string.Equals(existing.AttachmentId, attachment.AttachmentId, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        "client_attachment_id 已存在但 attachment_id/object_key 不一致。");
                }

                return existing;
            }
        }

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.AttachmentsTableSql} (
                 attachment_id,
                 uploader_user_id,
                 object_key,
                 public_url,
                 content_type,
                 size_bytes,
                 original_name,
                 status,
                 message_id,
                 conversation_id,
                 client_attachment_id,
                 created_at_ms,
                 confirmed_at_ms,
                 bound_at_ms
             )
             VALUES (
                 @attachment_id,
                 @uploader_user_id,
                 @object_key,
                 @public_url,
                 @content_type,
                 @size_bytes,
                 @original_name,
                 @status,
                 NULL,
                 NULL,
                 @client_attachment_id,
                 @created_at_ms,
                 @confirmed_at_ms,
                 NULL
             )
             ON CONFLICT (attachment_id) DO NOTHING;
             """,
            connection);
        command.Parameters.AddWithValue("attachment_id", attachment.AttachmentId);
        command.Parameters.AddWithValue("uploader_user_id", attachment.UploaderUserId);
        command.Parameters.AddWithValue("object_key", attachment.ObjectKey);
        command.Parameters.AddWithValue("public_url", (object?)attachment.PublicUrl ?? DBNull.Value);
        command.Parameters.AddWithValue("content_type", attachment.ContentType);
        command.Parameters.AddWithValue("size_bytes", attachment.SizeBytes);
        command.Parameters.AddWithValue("original_name", (object?)attachment.OriginalName ?? DBNull.Value);
        command.Parameters.AddWithValue("status", (short)AttachmentStatus.Confirmed);
        command.Parameters.AddWithValue(
            "client_attachment_id",
            (object?)clientAttachmentId ?? DBNull.Value);
        command.Parameters.AddWithValue("created_at_ms", now);
        command.Parameters.AddWithValue("confirmed_at_ms", confirmedAt);

        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (affected == 0)
        {
            var byId = await TryGetByIdAsync(connection, attachment.AttachmentId, ct)
                .ConfigureAwait(false);
            if (byId is not null)
                return byId;
            throw new InvalidOperationException("附件写入冲突且无法读取已有行。");
        }

        _logger.LogDebug(
            "已写入确认附件。附件={AttachmentId}；上传用户={UploaderUserId}",
            attachment.AttachmentId,
            attachment.UploaderUserId);

        return new RealtimeAttachmentRecord
        {
            AttachmentId = attachment.AttachmentId,
            UploaderUserId = attachment.UploaderUserId,
            ObjectKey = attachment.ObjectKey,
            PublicUrl = attachment.PublicUrl,
            ContentType = attachment.ContentType,
            SizeBytes = attachment.SizeBytes,
            OriginalName = attachment.OriginalName,
            Status = AttachmentStatus.Confirmed,
            ClientAttachmentId = clientAttachmentId,
            CreatedAtMs = now,
            ConfirmedAtMs = confirmedAt
        };
    }

    public async Task<int> BindToMessageAsync(
        string messageId,
        string? conversationId,
        long uploaderUserId,
        IReadOnlyList<string> attachmentIds,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(uploaderUserId);
        ArgumentNullException.ThrowIfNull(attachmentIds);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        var bound = await AttachmentWriteCommands.BindConfirmedToMessageAsync(
                connection,
                transaction,
                _databaseSchema,
                messageId,
                conversationId,
                uploaderUserId,
                attachmentIds,
                ct)
            .ConfigureAwait(false);

        var expected = attachmentIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .Count();
        if (bound != expected)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            throw new InvalidOperationException(
                $"附件绑定失败：期望 {expected}，实际 {bound}。");
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return bound;
    }

    public async Task<IReadOnlyList<RealtimeAttachmentRecord>> ListByMessageIdsAsync(
        IReadOnlyList<string> messageIds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(messageIds);
        if (messageIds.Count == 0)
            return [];

        var ids = messageIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (ids.Length == 0)
            return [];

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT attachment_id, uploader_user_id, object_key, public_url, content_type,
                    size_bytes, original_name, status, message_id, conversation_id,
                    client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms
             FROM {_databaseSchema.AttachmentsTableSql}
             WHERE message_id = ANY(@message_ids)
             ORDER BY message_id, created_at_ms, attachment_id;
             """,
            connection);
        var param = command.Parameters.Add("message_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
        param.Value = ids;

        return await ReadAllAsync(command, ct).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<RealtimeAttachmentRecord>> ListForUserExportAsync(
        long userId,
        string? afterAttachmentId,
        int take,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        take = Math.Clamp(take, 1, 1000);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        var sql = string.IsNullOrWhiteSpace(afterAttachmentId)
            ? $"""
               SELECT attachment_id, uploader_user_id, object_key, public_url, content_type,
                      size_bytes, original_name, status, message_id, conversation_id,
                      client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms
               FROM {_databaseSchema.AttachmentsTableSql}
               WHERE uploader_user_id = @user_id
               ORDER BY attachment_id
               LIMIT @take;
               """
            : $"""
               SELECT attachment_id, uploader_user_id, object_key, public_url, content_type,
                      size_bytes, original_name, status, message_id, conversation_id,
                      client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms
               FROM {_databaseSchema.AttachmentsTableSql}
               WHERE uploader_user_id = @user_id
                 AND attachment_id > @after_id
               ORDER BY attachment_id
               LIMIT @take;
               """;

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("take", take);
        if (!string.IsNullOrWhiteSpace(afterAttachmentId))
            command.Parameters.AddWithValue("after_id", afterAttachmentId);

        return await ReadAllAsync(command, ct).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<string>> ListObjectKeysByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        batchSize = Math.Clamp(batchSize, 1, 5_000);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        var objectKeys = new List<string>();
        string? afterKey = null;
        while (true)
        {
            await using var command = new NpgsqlCommand(
                afterKey is null
                    ? $"""
                       SELECT object_key
                       FROM {_databaseSchema.AttachmentsTableSql}
                       WHERE uploader_user_id = @user_id
                       ORDER BY object_key
                       LIMIT @batch_size;
                       """
                    : $"""
                       SELECT object_key
                       FROM {_databaseSchema.AttachmentsTableSql}
                       WHERE uploader_user_id = @user_id
                         AND object_key > @after_key
                       ORDER BY object_key
                       LIMIT @batch_size;
                       """,
                connection);
            command.Parameters.AddWithValue("user_id", userId);
            command.Parameters.AddWithValue("batch_size", batchSize);
            if (afterKey is not null)
                command.Parameters.AddWithValue("after_key", afterKey);

            var batchCount = 0;
            string? lastKey = null;
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                lastKey = reader.GetString(0);
                objectKeys.Add(lastKey);
                batchCount++;
            }

            if (batchCount <= 0 || lastKey is null)
                break;
            afterKey = lastKey;
        }

        return objectKeys;
    }

    public async Task<IReadOnlyList<string>> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        batchSize = Math.Clamp(batchSize, 1, 5_000);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        var objectKeys = new List<string>();
        while (true)
        {
            await using var command = new NpgsqlCommand(
                $"""
                 DELETE FROM {_databaseSchema.AttachmentsTableSql}
                 WHERE ctid IN (
                     SELECT ctid FROM {_databaseSchema.AttachmentsTableSql}
                     WHERE uploader_user_id = @user_id
                     LIMIT @batch_size
                 )
                 RETURNING object_key;
                 """,
                connection);
            command.Parameters.AddWithValue("user_id", userId);
            command.Parameters.AddWithValue("batch_size", batchSize);

            var batchCount = 0;
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                objectKeys.Add(reader.GetString(0));
                batchCount++;
            }

            if (batchCount <= 0)
                break;
        }

        if (objectKeys.Count > 0)
        {
            _logger.LogInformation(
                "已清理用户附件元数据。用户={UserId}；删除行={Deleted}",
                userId,
                objectKeys.Count);
        }

        return objectKeys;
    }

    private async Task<RealtimeAttachmentRecord?> TryGetByIdAsync(
        NpgsqlConnection connection,
        string attachmentId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT attachment_id, uploader_user_id, object_key, public_url, content_type,
                    size_bytes, original_name, status, message_id, conversation_id,
                    client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms
             FROM {_databaseSchema.AttachmentsTableSql}
             WHERE attachment_id = @attachment_id;
             """,
            connection);
        command.Parameters.AddWithValue("attachment_id", attachmentId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return Map(reader);
    }

    private async Task<RealtimeAttachmentRecord?> TryGetByUploaderClientAsync(
        NpgsqlConnection connection,
        long uploaderUserId,
        string clientAttachmentId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT attachment_id, uploader_user_id, object_key, public_url, content_type,
                    size_bytes, original_name, status, message_id, conversation_id,
                    client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms
             FROM {_databaseSchema.AttachmentsTableSql}
             WHERE uploader_user_id = @uploader_user_id
               AND client_attachment_id = @client_attachment_id;
             """,
            connection);
        command.Parameters.AddWithValue("uploader_user_id", uploaderUserId);
        command.Parameters.AddWithValue("client_attachment_id", clientAttachmentId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return Map(reader);
    }

    private static async Task<IReadOnlyList<RealtimeAttachmentRecord>> ReadAllAsync(
        NpgsqlCommand command,
        CancellationToken ct)
    {
        var list = new List<RealtimeAttachmentRecord>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            list.Add(Map(reader));
        return list;
    }

    private static RealtimeAttachmentRecord Map(NpgsqlDataReader reader) => new()
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
        BoundAtMs = reader.IsDBNull(13) ? null : reader.GetInt64(13)
    };
}
