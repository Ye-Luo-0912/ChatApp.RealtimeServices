using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Reliability-1：设备游标水位列改名。
/// 将 after_received_at_ms 重命名为 after_changed_at_ms，使列名与实际语义一致——
/// 该列存储的是 changed_at_ms（变更水位，涵盖编辑/撤回/Reaction）而非 received_at_ms。
/// 旧数据直接保留（值本身已是 changed_at_ms 语义，只是列名误导）。
/// </summary>
public sealed class Migration030_DeviceSyncCursorChangedAtColumn : IRealtimeSchemaMigration
{
    public int Version => 30;
    public string Name => "device_sync_cursor_changed_at_column";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var table = schema.DeviceSyncCursorsTableSql;

        // PostgreSQL 的 RENAME COLUMN 不支持 IF EXISTS，使用 catalog 检查后再改名。
        // 不能读取 current_setting('search_path') 判断 schema：测试/多租户 schema
        // 通过限定表名访问，并不保证 search_path 已切到目标 schema。
        await using var renameColumn = new NpgsqlCommand(
            $"""
             DO $$
             BEGIN
                 IF EXISTS (
                     SELECT 1 FROM information_schema.columns
                     WHERE table_schema = '{schema.Schema}'
                       AND table_name = 'device_sync_cursors'
                       AND column_name = 'after_received_at_ms'
                 ) AND NOT EXISTS (
                     SELECT 1 FROM information_schema.columns
                     WHERE table_schema = '{schema.Schema}'
                       AND table_name = 'device_sync_cursors'
                       AND column_name = 'after_changed_at_ms'
                 ) THEN
                     ALTER TABLE {table}
                         RENAME COLUMN "after_received_at_ms" TO "after_changed_at_ms";
                 END IF;
             END $$;
             """,
            connection,
            transaction);
        await renameColumn.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
