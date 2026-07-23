using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 设备级同步游标：�?(user, device, conversation) 保存 catch-up 水位�?
/// </summary>
public sealed class Migration008_DeviceSyncCursors : IRealtimeSchemaMigration
{
    public int Version => 8;
    public string Name => "device_sync_cursors";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var table = schema.DeviceSyncCursorsTableSql;
        var commands = new[]
        {
            $"""
             CREATE TABLE IF NOT EXISTS {table} (
                 "user_id" bigint NOT NULL,
                 "device_id_hash" bigint NOT NULL,
                 "conversation_id" character varying(64) NOT NULL,
                 "after_received_at_ms" bigint NOT NULL,
                 "after_message_id" character varying(64) NOT NULL,
                 "updated_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("user_id", "device_id_hash", "conversation_id")
             );
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_device_sync_cursors_user_device_updated"
                 ON {table} ("user_id", "device_id_hash", "updated_at_ms" DESC);
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
