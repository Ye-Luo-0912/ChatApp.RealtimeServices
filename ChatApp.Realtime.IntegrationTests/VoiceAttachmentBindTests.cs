using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.IntegrationTests;

/// <summary>
/// P1：VOICE-MSG-1（语音附件消息闭环）发送前绑定校验聚焦集成测试。
/// <para>
/// 覆盖需求 2：
/// - 正常：Available（含语音元数据）与 legacy Confirmed 附件可绑定为 Bound，取回语音元数据；
/// - 重复：同一 attachment id 请求多次按去重语义只绑定一次；
/// - 扫描状态变化：扫描中/拒绝/过期/已绑定冲突/缺失或非本人返回各自稳定错误码（绝不静默跳过）；
/// - 部分失败：存在任一不可绑定附件即整体失败，不绑定子集、可绑定附件保持原状（绝不绕过安全状态）。
/// 覆盖需求 3：attachments 表仅存有界语音元数据（codec/container/duration/sample_rate/channels），
/// 无音频包列；绑定后读回的线协议元数据即为全部音频信息。
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class VoiceAttachmentBindTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Bind_AvailableVoiceAttachment_Succeeds_AndCarriesVoiceMetadata()
    {
        var (client, schema) = await CreateStoreAsync("rt_voice_bind_ok");
        var store = new NpgsqlRealtimeAttachmentStore(
            client, schema, NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        await InsertAsync(
            client, schema,
            AttachmentRow("att-voice-1", "k/voice-1", uploader: 1001, status: AttachmentStatus.Available,
                isVoice: true, codec: "opus", container: "ogg", durationMs: 3_200, sampleRateHz: 48_000,
                channels: 1));

        var bound = await store.BindToMessageAsync(
            "msg-1", "dm:1001:1002", uploaderUserId: 1001, ["att-voice-1"]);

        Assert.Equal(1, bound);
        var row = await GetAsync(client, schema, "att-voice-1");
        Assert.Equal(AttachmentStatus.Bound, row.Status);
        Assert.Equal("msg-1", row.MessageId);
        Assert.True(row.IsVoice);
        Assert.Equal("opus", row.VoiceCodec);
        Assert.Equal("ogg", row.VoiceContainer);
        Assert.Equal(3_200L, row.VoiceDurationMs);
        Assert.Equal(48_000, row.VoiceSampleRateHz);
        Assert.Equal((short)1, row.VoiceChannels);
    }

    [Fact]
    public async Task Bind_LegacyConfirmedAttachment_StillSucceeds()
    {
        var (client, schema) = await CreateStoreAsync("rt_voice_bind_confirmed");
        var store = new NpgsqlRealtimeAttachmentStore(
            client, schema, NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        await InsertAsync(
            client, schema,
            AttachmentRow("att-legacy-1", "k/legacy-1", uploader: 1001,
                status: AttachmentStatus.Confirmed));

        var bound = await store.BindToMessageAsync(
            "msg-1", "dm:1001:1002", uploaderUserId: 1001, ["att-legacy-1"]);

        Assert.Equal(1, bound);
        Assert.Equal(AttachmentStatus.Bound,
            (await GetAsync(client, schema, "att-legacy-1")).Status);
    }

    [Fact]
    public async Task Bind_DuplicateAttachmentId_DeduplicatesToSingleBind()
    {
        var (client, schema) = await CreateStoreAsync("rt_voice_bind_dedup");
        var store = new NpgsqlRealtimeAttachmentStore(
            client, schema, NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        await InsertAsync(
            client, schema,
            AttachmentRow("att-dedup-1", "k/dedup-1", uploader: 1001,
                status: AttachmentStatus.Available));

        var bound = await store.BindToMessageAsync(
            "msg-1", "dm:1001:1002", uploaderUserId: 1001,
            ["att-dedup-1", "att-dedup-1", "att-dedup-1"]);

        Assert.Equal(1, bound);
        Assert.Equal(AttachmentStatus.Bound,
            (await GetAsync(client, schema, "att-dedup-1")).Status);
    }

    [Theory]
    [InlineData(AttachmentStatus.Scanning, "attachment_scanning")]
    [InlineData(AttachmentStatus.Rejected, "attachment_rejected")]
    [InlineData(AttachmentStatus.Expired, "attachment_expired")]
    [InlineData(AttachmentStatus.Bound, "attachment_already_bound")]
    [InlineData(AttachmentStatus.Ticketed, "attachment_invalid_state")]
    [InlineData(AttachmentStatus.Uploaded, "attachment_invalid_state")]
    public async Task Bind_NonReadyState_ThrowsStableErrorCode(
        AttachmentStatus status, string expectedStableCode)
    {
        var (client, schema) = await CreateStoreAsync($"rt_voice_bind_{status}");
        var store = new NpgsqlRealtimeAttachmentStore(
            client, schema, NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        await InsertAsync(
            client, schema,
            AttachmentRow($"att-state-{(short)status}", $"k/state-{(short)status}",
                uploader: 1001, status: status));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.BindToMessageAsync(
                "msg-1", "dm:1001:1002", uploaderUserId: 1001,
                [$"att-state-{(short)status}"]));

        Assert.Contains(expectedStableCode, ex.Message);
        // 失败不改变状态（不绕过安全状态）
        Assert.Equal(status,
            (await GetAsync(client, schema, $"att-state-{(short)status}")).Status);
    }

    [Fact]
    public async Task Bind_MissingOrForeignOwner_ThrowsNotFound()
    {
        var (client, schema) = await CreateStoreAsync("rt_voice_bind_missing");
        var store = new NpgsqlRealtimeAttachmentStore(
            client, schema, NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        await InsertAsync(
            client, schema,
            AttachmentRow("att-other-1", "k/other-1", uploader: 2001,
                status: AttachmentStatus.Available));

        // 不存在的 id
        var missing = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.BindToMessageAsync(
                "msg-1", "dm:1001:1002", uploaderUserId: 1001, ["att-does-not-exist"]));
        Assert.Contains("attachment_not_found", missing.Message);

        // 属于他人（uploader=2001）：对 1001 而言按不存在处理
        var foreign = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.BindToMessageAsync(
                "msg-1", "dm:1001:1002", uploaderUserId: 1001, ["att-other-1"]));
        Assert.Contains("attachment_not_found", foreign.Message);
        // 他人附件未被绑定
        Assert.Equal(AttachmentStatus.Available,
            (await GetAsync(client, schema, "att-other-1")).Status);
    }

    [Fact]
    public async Task Bind_MixedBindableAndScanning_WholeFails_NoPartialBind()
    {
        var (client, schema) = await CreateStoreAsync("rt_voice_bind_mixed");
        var store = new NpgsqlRealtimeAttachmentStore(
            client, schema, NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        await InsertAsync(
            client, schema,
            AttachmentRow("att-good-1", "k/good-1", uploader: 1001,
                status: AttachmentStatus.Available, isVoice: true, codec: "aac",
                container: "m4a", durationMs: 1_000, sampleRateHz: 44_100, channels: 2));
        await InsertAsync(
            client, schema,
            AttachmentRow("att-scanning-1", "k/scanning-1", uploader: 1001,
                status: AttachmentStatus.Scanning));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            store.BindToMessageAsync(
                "msg-1", "dm:1001:1002", uploaderUserId: 1001,
                ["att-good-1", "att-scanning-1"]));

        Assert.Contains("attachment_scanning", ex.Message);
        // 整体失败：可绑定附件必须保持 Available（未绑定子集、不绕过安全状态）
        Assert.Equal(AttachmentStatus.Available,
            (await GetAsync(client, schema, "att-good-1")).Status);
        Assert.Null((await GetAsync(client, schema, "att-good-1")).MessageId);
        Assert.Equal(AttachmentStatus.Scanning,
            (await GetAsync(client, schema, "att-scanning-1")).Status);
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateStoreAsync(
        string schemaName)
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);
        return (client, schema);
    }

    private static RealtimeAttachmentRecord AttachmentRow(
        string id,
        string objectKey,
        long uploader,
        AttachmentStatus status,
        bool isVoice = false,
        string? codec = null,
        string? container = null,
        long? durationMs = null,
        int? sampleRateHz = null,
        short? channels = null) => new()
    {
        AttachmentId = id,
        UploaderUserId = uploader,
        ObjectKey = objectKey,
        ContentType = isVoice ? "audio/ogg" : "application/octet-stream",
        SizeBytes = 1_024,
        OriginalName = isVoice ? "voice.ogg" : $"{id}.bin",
        Status = status,
        ClientAttachmentId = $"c-{id}",
        CreatedAtMs = 1_000,
        ConfirmedAtMs = status is AttachmentStatus.Confirmed or AttachmentStatus.Uploaded
            or AttachmentStatus.Scanning or AttachmentStatus.Available
            ? 2_000
            : null,
        BoundAtMs = status == AttachmentStatus.Bound ? 3_000 : null,
        IsVoice = isVoice,
        VoiceCodec = isVoice ? codec : null,
        VoiceContainer = isVoice ? container : null,
        VoiceDurationMs = isVoice ? durationMs : null,
        VoiceSampleRateHz = isVoice ? sampleRateHz : null,
        VoiceChannels = isVoice ? channels : null
    };

    private static async Task InsertAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        RealtimeAttachmentRecord row)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.AttachmentsTableSql} (
                 attachment_id, uploader_user_id, object_key, public_url, content_type,
                 size_bytes, original_name, status, message_id, conversation_id,
                 client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                 is_voice, voice_codec, voice_container, voice_duration_ms,
                 voice_sample_rate_hz, voice_channels
             ) VALUES (
                 @id, @uploader, @object_key, @public_url, @content_type,
                 @size, @original_name, @status, @message_id, @conversation_id,
                 @client_id, @created, @confirmed, @bound,
                 @is_voice, @codec, @container, @duration,
                 @sample_rate, @channels
             );
             """,
            connection);
        cmd.Parameters.AddWithValue("id", row.AttachmentId);
        cmd.Parameters.AddWithValue("uploader", row.UploaderUserId);
        cmd.Parameters.AddWithValue("object_key", row.ObjectKey);
        cmd.Parameters.AddWithValue("public_url", DBNull.Value);
        cmd.Parameters.AddWithValue("content_type", row.ContentType);
        cmd.Parameters.AddWithValue("size", row.SizeBytes);
        cmd.Parameters.AddWithValue("original_name", (object?)row.OriginalName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("status", (short)row.Status);
        cmd.Parameters.AddWithValue("message_id", (object?)row.MessageId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("conversation_id", DBNull.Value);
        cmd.Parameters.AddWithValue("client_id", (object?)row.ClientAttachmentId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("created", row.CreatedAtMs);
        cmd.Parameters.AddWithValue("confirmed", row.ConfirmedAtMs is null ? DBNull.Value : row.ConfirmedAtMs.Value);
        cmd.Parameters.AddWithValue("bound", row.BoundAtMs is null ? DBNull.Value : row.BoundAtMs.Value);
        cmd.Parameters.AddWithValue("is_voice", row.IsVoice);
        cmd.Parameters.AddWithValue("codec", row.VoiceCodec is null ? DBNull.Value : row.VoiceCodec);
        cmd.Parameters.AddWithValue("container", row.VoiceContainer is null ? DBNull.Value : row.VoiceContainer);
        cmd.Parameters.AddWithValue("duration", row.VoiceDurationMs is null ? DBNull.Value : row.VoiceDurationMs.Value);
        cmd.Parameters.AddWithValue("sample_rate", row.VoiceSampleRateHz is null ? DBNull.Value : row.VoiceSampleRateHz.Value);
        cmd.Parameters.AddWithValue("channels", row.VoiceChannels is null ? DBNull.Value : row.VoiceChannels.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<RealtimeAttachmentRecord> GetAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string attachmentId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT attachment_id, uploader_user_id, object_key, public_url, content_type,
                    size_bytes, original_name, status, message_id, conversation_id,
                    client_attachment_id, created_at_ms, confirmed_at_ms, bound_at_ms,
                    is_voice, voice_codec, voice_container, voice_duration_ms,
                    voice_sample_rate_hz, voice_channels
             FROM {schema.AttachmentsTableSql}
             WHERE attachment_id = @id;
             """,
            connection);
        cmd.Parameters.AddWithValue("id", attachmentId);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), "attachment row not found");
        var row = new RealtimeAttachmentRecord
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
            IsVoice = reader.GetBoolean(14),
            VoiceCodec = reader.IsDBNull(15) ? null : reader.GetString(15),
            VoiceContainer = reader.IsDBNull(16) ? null : reader.GetString(16),
            VoiceDurationMs = reader.IsDBNull(17) ? null : reader.GetInt64(17),
            VoiceSampleRateHz = reader.IsDBNull(18) ? null : reader.GetInt32(18),
            VoiceChannels = reader.IsDBNull(19) ? null : reader.GetInt16(19)
        };
        return row;
    }
}