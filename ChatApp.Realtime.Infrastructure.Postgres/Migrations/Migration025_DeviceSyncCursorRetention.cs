using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Perf-3：设备游标保留治理。
/// - 增加 last_seen_at_ms 列，记录设备最后一次活跃时间，用于 inactive 清理；
/// - 增加 (user_id, last_seen_at_ms) 索引，支持按用户批量清理长期未活跃设备游标。
/// </summary>
public sealed class Migration025_DeviceSyncCursorRetention : IRealtimeSchemaMigration
{
    public int Version => 25;
    public string Name => "device_sync_cursor_retention";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var table = schema.DeviceSyncCursorsTableSql;

        // last_seen_at_ms：设备最后活跃时间。首次迁移时回填为 updated_at_ms。
        await using var addColumn = new NpgsqlCommand(
            $"""
             ALTER TABLE {table}
                 ADD COLUMN IF NOT EXISTS "last_seen_at_ms" bigint NOT NULL DEFAULT 0;
             """,
            connection,
            transaction);
        await addColumn.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await using var backfill = new NpgsqlCommand(
            $"""
             UPDATE {table}
             SET "last_seen_at_ms" = "updated_at_ms"
             WHERE "last_seen_at_ms" = 0;
             """,
            connection,
            transaction);
        await backfill.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await using var index = new NpgsqlCommand(
            $"""
             CREATE INDEX IF NOT EXISTS "ix_device_sync_cursors_user_last_seen"
                 ON {table} ("user_id", "last_seen_at_ms");
             """,
            connection,
            transaction);
        await index.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
