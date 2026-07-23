using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Outbox Pending/Dead partial indexes for stats (avoid scanning Published).
/// RequiresTransaction=false: CREATE INDEX CONCURRENTLY cannot run in a transaction.
/// </summary>
public sealed class Migration011_OutboxStatsPartialIndexes : IRealtimeSchemaMigration
{
    public int Version => 11;
    public string Name => "outbox_stats_partial_indexes";
    public bool RequiresTransaction => false;

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
             CREATE INDEX CONCURRENTLY IF NOT EXISTS "ix_outbox_pending_created"
             ON {outbox} ("created_at_ms")
             WHERE "status" = 0;
             """,
            $"""
             CREATE INDEX CONCURRENTLY IF NOT EXISTS "ix_outbox_pending_attempts"
             ON {outbox} ("attempt_count")
             WHERE "status" = 0;
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
