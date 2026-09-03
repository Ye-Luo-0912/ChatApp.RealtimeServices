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
    /// <para>
    /// VOICE-MSG-2：<paramref name="attachmentMetadata"/>（发送方元数据快照，仅消息里出现的附件）
    /// 中语音字段成组有效（is_voice=true 且 codec/container/duration/sample_rate/channels 均非空且为正）的
    /// 附件，在绑定同一 UPDATE 内把语音 6 字段与可选波形 <c>voice_waveform_peaks</c> 写入附件行
    /// （sender 值优先，COALESCE 回退注册表现值；波形仅在语音声明完整时随写）；
    /// 残缺/越界语音声明按无元数据处理（保消息必达、不触碰 ck_attachments_voice_metadata 约束），
    /// 非语音附件的注册表现值不受影响。
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
        IReadOnlyList<AttachmentRef>? attachmentMetadata = null,
        CancellationToken ct = default)
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

        // VOICE-MSG-2：按可绑定 id 集合构建语音元数据 unnest 数组（与 bindable 顺序对齐）。
        // 完整语音集 → is_voice=true + 5 元数据 + 可选 waveform；其余（无/残缺语音声明）→
        // 全 NULL（COALESCE 保留现值）。
        var voiceMetadata = BuildVoiceMetadataArrays(
            bindable,
            attachmentMetadata);

        // Step 2：全部可绑定，一次 UPDATE 并 RETURNING 取回线协议字段（含语音元数据与波形）。
        // 语音元数据经 FROM unnest 与绑定同语句写入：sender 值优先，NULL 回退注册表现值。
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand(
            BuildBindSqlCommandText(schema.AttachmentsTableSql),
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
        AddVoiceMetadataParameters(command, voiceMetadata);

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
                    : null,
                VoiceWaveformPeaks = reader.FieldCount > 21 && !reader.IsDBNull(21)
                    ? reader.GetFieldValue<byte[]>(21)
                    : null
            });
        }

        return AttachmentBindResult.Ok(records);
    }

    /// <summary>语音元数据 unnest 行：与可绑定 id 集合逐位对齐；无语音声明的位全 NULL。</summary>
    internal sealed record VoiceMetadataArrays(
        string[] AttachmentIds,
        bool?[] IsVoice,
        string?[] VoiceCodec,
        string?[] VoiceContainer,
        long?[] VoiceDurationMs,
        int?[] VoiceSampleRateHz,
        short?[] VoiceChannels,
        byte[]?[] VoiceWaveformPeaks);

    /// <summary>voice_waveform_peaks 有界上限（与 wire 二进制 bytea 域预算一致，64 KiB）。</summary>
    internal const int MaxWaveformPeaksBytes = 64 * 1024;

    /// <summary>
    /// 按可绑定 id 顺序构建语音元数据数组。仅"完整语音声明"（is_voice=true 且
    /// codec/container 非空白、duration/sample_rate/channels 为正，codec/container 截断到列宽 32）
    /// 产生非 NULL 位；其余（无元数据、非语音、残缺声明）全 NULL，绑定时不触碰语音列。
    /// <para>
    /// 波形（可选）仅在语音声明完整时随写：空数组视为无波形；超过
    /// <see cref="MaxWaveformPeaksBytes"/> 的越界波形按无波形处理（丢弃波形本身，
    /// 不影响已有效的语音 6 字段写入）。
    /// </para>
    /// </summary>
    internal static VoiceMetadataArrays BuildVoiceMetadataArrays(
        IReadOnlyList<string> bindableIds,
        IReadOnlyList<AttachmentRef>? attachmentMetadata)
    {
        var count = bindableIds.Count;
        var isVoice = new bool?[count];
        var codec = new string?[count];
        var container = new string?[count];
        var durationMs = new long?[count];
        var sampleRateHz = new int?[count];
        var channels = new short?[count];
        var waveformPeaks = new byte[]?[count];

        if (attachmentMetadata is not { Count: > 0 })
        {
            return new VoiceMetadataArrays(
                bindableIds.ToArray(), isVoice, codec, container, durationMs, sampleRateHz,
                channels, waveformPeaks);
        }

        // 元数据快照可能与请求集合非严格对齐（旧网关/重复 id）：按 id 建索引，仅对可绑定 id 生效。
        var byId = new Dictionary<string, AttachmentRef>(StringComparer.Ordinal);
        foreach (var reference in attachmentMetadata)
        {
            if (reference?.AttachmentId is { Length: > 0 } id)
                byId[id] = reference;
        }

        for (var i = 0; i < count; i++)
        {
            if (!byId.TryGetValue(bindableIds[i], out var reference)
                || !reference.IsVoice)
            {
                continue;
            }

            var trimmedCodec = string.IsNullOrWhiteSpace(reference.VoiceCodec)
                ? null
                : reference.VoiceCodec.Trim();
            var trimmedContainer = string.IsNullOrWhiteSpace(reference.VoiceContainer)
                ? null
                : reference.VoiceContainer.Trim();
            var valid = trimmedCodec is not null
                        && trimmedContainer is not null
                        && reference.VoiceDurationMs is > 0
                        && reference.VoiceSampleRateHz is > 0
                        && reference.VoiceChannels is > 0;
            if (!valid)
            {
                // 残缺语音声明：ck_attachments_voice_metadata 禁止 is_voice=true 且元数据缺失。
                // 按无元数据处理（含波形），保消息必达。
                continue;
            }

            isVoice[i] = true;
            codec[i] = trimmedCodec!.Length > VoiceCodecColumnLength
                ? trimmedCodec[..VoiceCodecColumnLength]
                : trimmedCodec;
            container[i] = trimmedContainer!.Length > VoiceCodecColumnLength
                ? trimmedContainer[..VoiceCodecColumnLength]
                : trimmedContainer;
            durationMs[i] = reference.VoiceDurationMs;
            sampleRateHz[i] = reference.VoiceSampleRateHz;
            channels[i] = reference.VoiceChannels;
            waveformPeaks[i] = reference.VoiceWaveformPeaks is { Length: > 0 } peaks
                               && peaks.Length <= MaxWaveformPeaksBytes
                ? peaks
                : null;
        }

        return new VoiceMetadataArrays(
            bindableIds.ToArray(), isVoice, codec, container, durationMs, sampleRateHz,
            channels, waveformPeaks);
    }

    /// <summary>voice_codec/voice_container 列宽（Migration065）。</summary>
    private const int VoiceCodecColumnLength = 32;

    /// <summary>
    /// 绑定 UPDATE 语句文本（语音 6 字段 + 可选波形列经 unnest 同语句写入，
    /// RETURNING 带回全部线协议字段）。独立成方法供纯 SQL 层单测校验语句形状。
    /// </summary>
    internal static string BuildBindSqlCommandText(string attachmentsTableSql) => $"""
         UPDATE {attachmentsTableSql} AS a
         SET message_id = @message_id,
             conversation_id = @conversation_id,
             status = @bound_status,
             bound_at_ms = @bound_at_ms,
             is_voice = COALESCE(m.is_voice, a.is_voice),
             voice_codec = COALESCE(m.voice_codec, a.voice_codec),
             voice_container = COALESCE(m.voice_container, a.voice_container),
             voice_duration_ms = COALESCE(m.voice_duration_ms, a.voice_duration_ms),
             voice_sample_rate_hz = COALESCE(m.voice_sample_rate_hz, a.voice_sample_rate_hz),
             voice_channels = COALESCE(m.voice_channels, a.voice_channels),
             voice_waveform_peaks = COALESCE(m.voice_waveform_peaks, a.voice_waveform_peaks)
         FROM unnest(
                  @m_attachment_ids::text[],
                  @m_is_voice::boolean[],
                  @m_voice_codec::text[],
                  @m_voice_container::text[],
                  @m_voice_duration_ms::bigint[],
                  @m_voice_sample_rate_hz::integer[],
                  @m_voice_channels::smallint[],
                  @m_voice_waveform_peaks::bytea[])
              AS m(attachment_id, is_voice, voice_codec, voice_container,
                   voice_duration_ms, voice_sample_rate_hz, voice_channels,
                   voice_waveform_peaks)
         WHERE a.attachment_id = m.attachment_id
           AND a.attachment_id = ANY(@attachment_ids)
           AND a.uploader_user_id = @uploader_user_id
           AND a.status IN (@available_status, @confirmed_status)
         RETURNING a.attachment_id, a.uploader_user_id, a.object_key, a.public_url,
                   a.content_type, a.size_bytes, a.original_name, a.status,
                   a.message_id, a.conversation_id,
                   a.client_attachment_id, a.created_at_ms, a.confirmed_at_ms, a.bound_at_ms,
                   a.content_hash, a.is_voice, a.voice_codec, a.voice_container,
                   a.voice_duration_ms, a.voice_sample_rate_hz, a.voice_channels,
                   a.voice_waveform_peaks;
         """;

    private static void AddVoiceMetadataParameters(
        NpgsqlCommand command,
        VoiceMetadataArrays metadata)
    {
        void AddArray(string name, NpgsqlDbType type, object value)
        {
            var parameter = command.Parameters.Add(name, NpgsqlDbType.Array | type);
            parameter.Value = value;
        }

        AddArray("m_attachment_ids", NpgsqlDbType.Text, metadata.AttachmentIds);
        AddArray("m_is_voice", NpgsqlDbType.Boolean, metadata.IsVoice);
        AddArray("m_voice_codec", NpgsqlDbType.Text, metadata.VoiceCodec);
        AddArray("m_voice_container", NpgsqlDbType.Text, metadata.VoiceContainer);
        AddArray("m_voice_duration_ms", NpgsqlDbType.Bigint, metadata.VoiceDurationMs);
        AddArray("m_voice_sample_rate_hz", NpgsqlDbType.Integer, metadata.VoiceSampleRateHz);
        AddArray("m_voice_channels", NpgsqlDbType.Smallint, metadata.VoiceChannels);
        AddArray("m_voice_waveform_peaks", NpgsqlDbType.Bytea, metadata.VoiceWaveformPeaks);
    }
}