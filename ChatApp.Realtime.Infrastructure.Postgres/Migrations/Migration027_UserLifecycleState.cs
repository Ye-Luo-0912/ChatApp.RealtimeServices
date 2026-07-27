using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 在 user_deletion_tombstones 表中增加 state 列（smallint NOT NULL DEFAULT 1）。
/// 0=Active（不使用），1=Deleting，2=Deleted。
/// 已有行默认为 1（Deleting），因为现有 tombstone 均在清理开始时写入。
/// </summary>
public sealed class Migration027_UserLifecycleState : IRealtimeSchemaMigration
{
    public int Version => 27;
    public string Name => "user_lifecycle_state";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             ALTER TABLE {schema.UserDeletionTombstonesTableSql}
                 ADD COLUMN IF NOT EXISTS "state" smallint NOT NULL DEFAULT 1;
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
