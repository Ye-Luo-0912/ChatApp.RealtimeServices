using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 语音波形采样峰值包络列（VOICE-MSG-2 waveform）。
/// <para>
/// 为 <c>realtime.attachments</c> 增加可选 <c>voice_waveform_peaks</c>（BYTEA，可空）：
/// 每字节 0–255 归一化幅度，由录音端降采样生成，仅在语音附件绑定消息时随发送方
/// 完整语音元数据快照写入（见 <c>AttachmentWriteCommands</c>）。缺省/空表示无波形
/// （旧客户端/旧录音），消费端必须以进度条降级渲染；无 CHECK 约束（可空列，
/// 完整性由 <c>ck_attachments_voice_metadata</c> 管辖的语音 6 字段约束不变）。
/// </para>
/// </summary>
public sealed class Migration070_VoiceAttachmentWaveform : IRealtimeSchemaMigration
{
    public int Version => 70;
    public string Name => "voice_attachment_waveform";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var attachments = schema.AttachmentsTableSql;

        var sql = $"""
                   ALTER TABLE {attachments}
                   ADD COLUMN IF NOT EXISTS "voice_waveform_peaks" bytea NULL;
                   """;

        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
