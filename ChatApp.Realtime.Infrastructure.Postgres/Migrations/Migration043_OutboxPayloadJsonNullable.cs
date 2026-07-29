using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 五：允许 payload_json 列为 NULL，停止双写后新记录仅写 payload_utf8。
/// <para>
/// 旧记录的 payload_json 仍保留，Claim 路径在其 payload_utf8 为 NULL 时回退反序列化 payload_json。
/// </para>
/// </summary>
public sealed class Migration043_OutboxPayloadJsonNullable : IRealtimeSchemaMigration
{
    public int Version => 43;
    public string Name => "outbox_payload_json_nullable";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand(
            $"""
             ALTER TABLE {schema.OutboxTableSql}
             ALTER COLUMN payload_json DROP NOT NULL;
             """,
            connection,
            transaction);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
