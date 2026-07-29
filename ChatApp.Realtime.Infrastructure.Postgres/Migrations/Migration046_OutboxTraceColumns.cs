using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// P0-8：为 Outbox 表添加 trace_parent、trace_state、occurred_at_ms 列。
/// <para>
/// 新记录写入时将 W3C trace context 和事件时间戳持久化到独立列，
/// Claim 路径直接从列读取，避免 <c>JsonDocument.Parse(payload_utf8)</c> 提取 trace context。
/// 旧记录这些列为 NULL，Claim 路径回退到 JSON 解析。
/// </para>
/// </summary>
public sealed class Migration046_OutboxTraceColumns : IRealtimeSchemaMigration
{
    public int Version => 46;
    public string Name => "outbox_trace_columns";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            $"""
             ALTER TABLE {schema.OutboxTableSql}
             ADD COLUMN IF NOT EXISTS trace_parent text NULL,
             ADD COLUMN IF NOT EXISTS trace_state text NULL,
             ADD COLUMN IF NOT EXISTS occurred_at_ms bigint NULL;
             """,
            connection,
            transaction);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
