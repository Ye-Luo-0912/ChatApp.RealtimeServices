using ChatApp.Realtime.Abstractions.Attachments;
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
                 bound_at_ms,
                 state_version
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
                 NULL,
                 0
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
            ConfirmedAtMs = confirmedAt,
            StateVersion = 0
        };
    }

    public async Task<AttachmentFinalizePersistResult> FinalizeUploadAsync(
        long actorUserId,
        string attachmentId,
        long sizeBytes,
        string? contentHash,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(actorUserId);
        ArgumentException.ThrowIfNullOrWhiteSpace(attachmentId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sizeBytes);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var hash = string.IsNullOrWhiteSpace(contentHash) ? null : contentHash.Trim();

        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.AttachmentsTableSql}
             SET status = @status,
                 size_bytes = @size_bytes,
                 content_hash = @content_hash,
                 confirmed_at_ms = @confirmed_at_ms,
                 state_version = state_version + 1
             WHERE attachment_id = @attachment_id
               AND uploader_user_id = @uploader_user_id
               AND status = @ticketed
             RETURNING attachment_id, uploader_user_id, object_key, public_url, content_type,
                       size_bytes, original_name, status, message_id, conversation_id,
                       client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                       content_hash, state_version;
             """,
            connection);
        command.Parameters.AddWithValue("status", (short)AttachmentStatus.Uploaded);
        command.Parameters.AddWithValue("size_bytes", sizeBytes);
        command.Parameters.AddWithValue("content_hash", (object?)hash ?? DBNull.Value);
        command.Parameters.AddWithValue("confirmed_at_ms", now);
        command.Parameters.AddWithValue("attachment_id", attachmentId);
        command.Parameters.AddWithValue("uploader_user_id", actorUserId);
        command.Parameters.AddWithValue("ticketed", (short)AttachmentStatus.Ticketed);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return AttachmentFinalizePersistResult.Ok(Map(reader));
        }

        var existing = await TryGetByIdAsync(connection, attachmentId, ct).ConfigureAwait(false);
        if (existing is null)
            return AttachmentFinalizePersistResult.Fail("not_found", "附件不存在。");
        if (existing.UploaderUserId != actorUserId)
            return AttachmentFinalizePersistResult.Fail("forbidden", "无权确认此附件。");
        if (existing.Status == AttachmentStatus.Uploaded)
        {
            if (existing.SizeBytes != sizeBytes)
                return AttachmentFinalizePersistResult.Fail("size_mismatch", "附件大小与已确认记录不一致。");
            return AttachmentFinalizePersistResult.Ok(existing);
        }
        return AttachmentFinalizePersistResult.Fail("invalid_state", $"附件当前状态 {existing.Status} 不允许确认上传。");
    }

    public async Task<AttachmentScanTransitionResult> BeginScanAsync(
        string attachmentId,
        long expectedStateVersion,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(attachmentId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.AttachmentsTableSql}
             SET status = @scanning,
                 state_version = state_version + 1
             WHERE attachment_id = @attachment_id
               AND status = @uploaded
               AND state_version = @expected_version
             RETURNING attachment_id, uploader_user_id, object_key, public_url, content_type,
                       size_bytes, original_name, status, message_id, conversation_id,
                       client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                       content_hash, state_version;
             """,
            connection);
        command.Parameters.AddWithValue("scanning", (short)AttachmentStatus.Scanning);
        command.Parameters.AddWithValue("uploaded", (short)AttachmentStatus.Uploaded);
        command.Parameters.AddWithValue("expected_version", expectedStateVersion);
        command.Parameters.AddWithValue("attachment_id", attachmentId);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
            return AttachmentScanTransitionResult.Ok(Map(reader));

        var existing = await TryGetByIdAsync(connection, attachmentId, ct).ConfigureAwait(false);
        if (existing is null)
            return AttachmentScanTransitionResult.Fail("not_found", "附件不存在。");
        if (existing.Status == AttachmentStatus.Scanning)
            return AttachmentScanTransitionResult.Ok(existing);
        return AttachmentScanTransitionResult.Fail(
            "invalid_state",
            $"附件当前状态 {existing.Status} 不允许开始扫描（且版本 {existing.StateVersion} 与期望 {expectedStateVersion} 不符）。");
    }

    public async Task<AttachmentScanTransitionResult> CompleteScanAsync(
        string attachmentId,
        long expectedStateVersion,
        AttachmentScanVerdict verdict,
        long sizeBytes,
        string? contentHash,
        string? contentType,
        string? reason,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(attachmentId);

        var targetStatus = verdict == AttachmentScanVerdict.Pass
            ? AttachmentStatus.Available
            : AttachmentStatus.Rejected;
        var hash = string.IsNullOrWhiteSpace(contentHash) ? null : contentHash.Trim();
        var mime = string.IsNullOrWhiteSpace(contentType) ? null : contentType.Trim();
        var rejectReason = string.IsNullOrWhiteSpace(reason) ? null : reason.Trim();

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.AttachmentsTableSql}
             SET status = @status,
                 size_bytes = @size_bytes,
                 content_hash = COALESCE(@content_hash, content_hash),
                 content_type = COALESCE(@content_type, content_type),
                 original_name = COALESCE(@reason, original_name),
                 state_version = state_version + 1
             WHERE attachment_id = @attachment_id
               AND status = @scanning
               AND state_version = @expected_version
             RETURNING attachment_id, uploader_user_id, object_key, public_url, content_type,
                       size_bytes, original_name, status, message_id, conversation_id,
                       client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                       content_hash, state_version;
             """,
            connection);
        command.Parameters.AddWithValue("status", (short)targetStatus);
        command.Parameters.AddWithValue("size_bytes", sizeBytes);
        command.Parameters.AddWithValue("content_hash", (object?)hash ?? DBNull.Value);
        command.Parameters.AddWithValue("content_type", (object?)mime ?? DBNull.Value);
        command.Parameters.AddWithValue("reason", (object?)rejectReason ?? DBNull.Value);
        command.Parameters.AddWithValue("scanning", (short)AttachmentStatus.Scanning);
        command.Parameters.AddWithValue("expected_version", expectedStateVersion);
        command.Parameters.AddWithValue("attachment_id", attachmentId);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
            return AttachmentScanTransitionResult.Ok(Map(reader));

        var existing = await TryGetByIdAsync(connection, attachmentId, ct).ConfigureAwait(false);
        if (existing is null)
            return AttachmentScanTransitionResult.Fail("not_found", "附件不存在。");
        return AttachmentScanTransitionResult.Fail(
            "stale_state_version",
            $"扫描结果过期：附件当前状态 {existing.Status}、版本 {existing.StateVersion}，与期望 {expectedStateVersion} 不符。");
    }

    public async Task<bool> MarkExpiredAsync(
        string attachmentId,
        long expectedStateVersion,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(attachmentId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.AttachmentsTableSql}
             SET status = @expired,
                 state_version = state_version + 1
             WHERE attachment_id = @attachment_id
               AND message_id IS NULL
               AND status IN (@ticketed, @uploaded, @scanning)
               AND state_version = @expected_version;
             """,
            connection);
        command.Parameters.AddWithValue("expired", (short)AttachmentStatus.Expired);
        command.Parameters.AddWithValue("ticketed", (short)AttachmentStatus.Ticketed);
        command.Parameters.AddWithValue("uploaded", (short)AttachmentStatus.Uploaded);
        command.Parameters.AddWithValue("scanning", (short)AttachmentStatus.Scanning);
        command.Parameters.AddWithValue("expected_version", expectedStateVersion);
        command.Parameters.AddWithValue("attachment_id", attachmentId);

        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        return affected > 0;
    }

    public async Task<IReadOnlyList<RealtimeAttachmentRecord>> ListExpiryCandidatesAsync(
        long cutoffMs,
        int take,
        CancellationToken ct = default)
    {
        take = Math.Clamp(take, 1, 1000);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             SELECT attachment_id, uploader_user_id, object_key, public_url, content_type,
                    size_bytes, original_name, status, message_id, conversation_id,
                    client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                    content_hash, state_version
             FROM {_databaseSchema.AttachmentsTableSql}
             WHERE message_id IS NULL
               AND status IN (@ticketed, @uploaded, @scanning)
               AND created_at_ms < @cutoff_ms
             ORDER BY created_at_ms
             LIMIT @take;
             """,
            connection);
        command.Parameters.AddWithValue("ticketed", (short)AttachmentStatus.Ticketed);
        command.Parameters.AddWithValue("uploaded", (short)AttachmentStatus.Uploaded);
        command.Parameters.AddWithValue("scanning", (short)AttachmentStatus.Scanning);
        command.Parameters.AddWithValue("cutoff_ms", cutoffMs);
        command.Parameters.AddWithValue("take", take);

        return await ReadAllAsync(command, ct).ConfigureAwait(false);
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

        var boundRecords = await AttachmentWriteCommands.BindConfirmedToMessageAsync(
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
        if (boundRecords.Count != expected)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            throw new InvalidOperationException(
                $"附件绑定失败：期望 {expected}，实际 {boundRecords.Count}。");
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return boundRecords.Count;
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
                    client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                    content_hash, state_version
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
                      client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                      content_hash, state_version
               FROM {_databaseSchema.AttachmentsTableSql}
               WHERE uploader_user_id = @user_id
               ORDER BY attachment_id
               LIMIT @take;
               """
            : $"""
               SELECT attachment_id, uploader_user_id, object_key, public_url, content_type,
                      size_bytes, original_name, status, message_id, conversation_id,
                      client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                      content_hash, state_version
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

    public async Task<int> DeleteByAttachmentIdsAsync(
        IReadOnlyList<string> attachmentIds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(attachmentIds);
        var ids = attachmentIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (ids.Length == 0)
            return 0;

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
            DELETE FROM {_databaseSchema.AttachmentsTableSql}
            WHERE attachment_id = ANY(@attachment_ids);
            """,
            connection);
        var param = command.Parameters.Add("attachment_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
        param.Value = ids;

        var affected = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (affected > 0)
        {
            _logger.LogDebug(
                "已按 attachment_id 批量删除附件元数据。删除行={Deleted}",
                affected);
        }
        return affected;
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
                    client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                    content_hash, state_version
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
                    client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                    content_hash, state_version
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
        BoundAtMs = reader.IsDBNull(13) ? null : reader.GetInt64(13),
        ContentHash = reader.FieldCount > 14 && !reader.IsDBNull(14)
            ? reader.GetString(14)
            : null,
        StateVersion = reader.FieldCount > 15 ? reader.GetInt64(15) : 0
    };
}