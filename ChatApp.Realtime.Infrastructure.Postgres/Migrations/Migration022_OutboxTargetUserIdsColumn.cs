using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>Outbox 增加 target_user_ids 列，用于群聊聚合事件的多目标投递与清理。</summary>
public sealed class Migration022_OutboxTargetUserIdsColumn : IRealtimeSchemaMigration
{
    public int Version => 22;
    public string Name => "outbox_target_user_ids_column";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var outbox = schema.OutboxTableSql;

        var commands = new[]
        {
            $"""
             ALTER TABLE {outbox}
             ADD COLUMN IF NOT EXISTS "target_user_ids" bigint[] NULL;
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_outbox_target_user_ids"
             ON {outbox} USING GIN ("target_user_ids");
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}