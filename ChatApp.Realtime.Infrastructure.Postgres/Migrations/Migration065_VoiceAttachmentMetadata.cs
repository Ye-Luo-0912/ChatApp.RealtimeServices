using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 语音附件元数据列（VOICE-MSG-1）。
/// <para>
/// 语音作为普通附件消息进入上传/扫描/绑定/Outbox 流程，但 Postgres 只保存有界元数据与对象引用，
/// 不保存音频包。本迁移为 <c>realtime.attachments</c> 增加 <c>is_voice</c> 与可选语音元数据列，
/// 并对语音行强制必须有界、合理的 codec/duration/sample_rate/channels，防止无界或缺失元数据入库。
/// </para>
/// </summary>
public sealed class Migration065_VoiceAttachmentMetadata : IRealtimeSchemaMigration
{
    public int Version => 65;
    public string Name => "voice_attachment_metadata";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var attachments = schema.AttachmentsTableSql;

        var commands = new[]
        {
            $"""
             ALTER TABLE {attachments}
             ADD COLUMN IF NOT EXISTS "is_voice" boolean NOT NULL DEFAULT FALSE;
             """,
            $"""
             ALTER TABLE {attachments}
             ADD COLUMN IF NOT EXISTS "voice_codec" character varying(32) NULL;
             """,
            $"""
             ALTER TABLE {attachments}
             ADD COLUMN IF NOT EXISTS "voice_container" character varying(32) NULL;
             """,
            $"""
             ALTER TABLE {attachments}
             ADD COLUMN IF NOT EXISTS "voice_duration_ms" bigint NULL;
             """,
            $"""
             ALTER TABLE {attachments}
             ADD COLUMN IF NOT EXISTS "voice_sample_rate_hz" integer NULL;
             """,
            $"""
             ALTER TABLE {attachments}
             ADD COLUMN IF NOT EXISTS "voice_channels" smallint NULL;
             """,
            $"""
             ALTER TABLE {attachments}
             DROP CONSTRAINT IF EXISTS "ck_attachments_voice_metadata";
             """,
            $"""
             ALTER TABLE {attachments}
             ADD CONSTRAINT "ck_attachments_voice_metadata"
             CHECK (
                 "is_voice" = FALSE
                 OR (
                     "voice_duration_ms" IS NOT NULL AND "voice_duration_ms" > 0
                     AND "voice_sample_rate_hz" IS NOT NULL AND "voice_sample_rate_hz" > 0
                     AND "voice_channels" IS NOT NULL AND "voice_channels" > 0
                     AND "voice_codec" IS NOT NULL
                     AND "voice_container" IS NOT NULL
                 )
             );
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}