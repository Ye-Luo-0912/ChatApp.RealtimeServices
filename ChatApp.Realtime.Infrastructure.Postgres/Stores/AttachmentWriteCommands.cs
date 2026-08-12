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
    /// 发送前附件绑定校验并绑定（VOICE-MSG-1）。
    /// <para>
    /// 语义：对每个请求附件做发送前可用性校验——<see cref="AttachmentStatus.Available"/>
    /// 或 legacy <see cref="AttachmentStatus.Confirmed"/> 视为可绑定；扫描中、拒绝、过期、
    /// 已绑定冲突、非本人或不存在返回各自稳定错误码。任一附件不可绑定即整体失败返回
    /// <see cref="AttachmentBindResult.Fail"/>，绝不绑定子集或静默跳过。
    /// </para>
    /// </summary>
    public static async Task<AttachmentBindResult> BindConfirmedToMessageAsync(
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
            return AttachmentBindResult.Ok([]);

        var distinctIds = attachmentIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (distinctIds.Length == 0)
            return AttachmentBindResult.Ok([]);
        if (distinctIds.Length > MaxAttachmentsPerMessage)
            throw new InvalidOperationException(
                $"单条消息附件数不能超过 {MaxAttachmentsPerMessage}。");

        // Step 1：一次性读取请求集合内全部附件（按归属过滤）的当前状态，逐一分类。
        var bindable = new List<string>(distinctIds.Length);
        var errors = new Dictionary<string, AttachmentBindErrorCode>();
        await using (var probe = new NpgsqlCommand(
            $"""
             SELECT attachment_id, status
             FROM {schema.AttachmentsTableSql}
             WHERE attachment_id = ANY(@attachment_ids)
               AND uploader_user_id = @uploader_user_id;
             """,
            connection,
            transaction))
        {
            probe.Parameters.AddWithValue("uploader_user_id", uploaderUserId);
            var probeIdsParam = probe.Parameters.Add(
                "attachment_ids",
                NpgsqlDbType.Array | NpgsqlDbType.Text);
            probeIdsParam.Value = distinctIds;

            var found = new Dictionary<string, AttachmentStatus>(StringComparer.Ordinal);
            await using var probeReader = await probe.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await probeReader.ReadAsync(ct).ConfigureAwait(false))
            {
                found[probeReader.GetString(0)] = (AttachmentStatus)probeReader.GetInt16(1);
            }

            foreach (var id in distinctIds)
            {
                if (!found.TryGetValue(id, out var status))
                {
                    errors[id] = AttachmentBindErrorCode.NotFound;
                    continue;
                }

                switch (status)
                {
                    case AttachmentStatus.Available:
                    case AttachmentStatus.Confirmed:
                        bindable.Add(id);
                        break;
                    case AttachmentStatus.Scanning:
                        errors[id] = AttachmentBindErrorCode.Scanning;
                        break;
                    case AttachmentStatus.Rejected:
                        errors[id] = AttachmentBindErrorCode.Rejected;
                        break;
                    case AttachmentStatus.Expired:
                        errors[id] = AttachmentBindErrorCode.Expired;
                        break;
                    case AttachmentStatus.Bound:
                        errors[id] = AttachmentBindErrorCode.AlreadyBound;
                        break;
                    default:
                        errors[id] = AttachmentBindErrorCode.InvalidState;
                        break;
                }
            }
        }

        // 任一不可绑定即整体失败：不绑定子集，由调用方回滚事务。
        if (errors.Count > 0)
            return AttachmentBindResult.Fail(errors);

        // Step 2：全部可绑定，一次 UPDATE 并 RETURNING 取回线协议字段（含语音元数据）。
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
               AND status IN (@available_status, @confirmed_status)
             RETURNING attachment_id, uploader_user_id, object_key, public_url, content_type,
                       size_bytes, original_name, status, message_id, conversation_id,
                       client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                       content_hash, is_voice, voice_codec, voice_container,
                       voice_duration_ms, voice_sample_rate_hz, voice_channels;
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
        command.Parameters.AddWithValue(
            "available_status",
            (short)AttachmentStatus.Available);
        command.Parameters.AddWithValue(
            "confirmed_status",
            (short)AttachmentStatus.Confirmed);
        var idsParam = command.Parameters.Add(
            "attachment_ids",
            NpgsqlDbType.Array | NpgsqlDbType.Text);
        idsParam.Value = bindable;

        var records = new List<RealtimeAttachmentRecord>(bindable.Count);
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
                    : null,
                IsVoice = reader.FieldCount > 15 && reader.GetBoolean(15),
                VoiceCodec = reader.FieldCount > 16 && !reader.IsDBNull(16)
                    ? reader.GetString(16)
                    : null,
                VoiceContainer = reader.FieldCount > 17 && !reader.IsDBNull(17)
                    ? reader.GetString(17)
                    : null,
                VoiceDurationMs = reader.FieldCount > 18 && !reader.IsDBNull(18)
                    ? reader.GetInt64(18)
                    : null,
                VoiceSampleRateHz = reader.FieldCount > 19 && !reader.IsDBNull(19)
                    ? reader.GetInt32(19)
                    : null,
                VoiceChannels = reader.FieldCount > 20 && !reader.IsDBNull(20)
                    ? reader.GetInt16(20)
                    : null
            });
        }

        return AttachmentBindResult.Ok(records);
    }
}