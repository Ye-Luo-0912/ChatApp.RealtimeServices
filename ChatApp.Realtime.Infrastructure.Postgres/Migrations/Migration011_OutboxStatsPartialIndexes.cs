using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Outbox Pending/Dead partial indexes for stats (avoid scanning Published).
/// RequiresTransaction=false: CREATE INDEX CONCURRENTLY cannot run in a transaction.
/// </summary>
/// <remarks>
/// LongTerm-3：通过 <see cref="ConcurrentIndexHelper"/> 检查 pg_index.indisvalid，
/// INVALID 时自动 DROP INDEX CONCURRENTLY 后重建，避免中断留下的 INVALID 索引被
/// IF NOT EXISTS 误判为已完成。
/// </remarks>
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

        await ConcurrentIndexHelper.EnsureValidAsync(
                connection,
                schema.QuotedSchema,
                schema.Schema,
                "ix_outbox_pending_created",
                $"""
                 CREATE INDEX CONCURRENTLY "ix_outbox_pending_created"
                 ON {outbox} ("created_at_ms")
                 WHERE "status" = 0;
                 """,
                cancellationToken)
            .ConfigureAwait(false);

        await ConcurrentIndexHelper.EnsureValidAsync(
                connection,
                schema.QuotedSchema,
                schema.Schema,
                "ix_outbox_pending_attempts",
                $"""
                 CREATE INDEX CONCURRENTLY "ix_outbox_pending_attempts"
                 ON {outbox} ("attempt_count")
                 WHERE "status" = 0;
                 """,
                cancellationToken)
            .ConfigureAwait(false);
    }
}
