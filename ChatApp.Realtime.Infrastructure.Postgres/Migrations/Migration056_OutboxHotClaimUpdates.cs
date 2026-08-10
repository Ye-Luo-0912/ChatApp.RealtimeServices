using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 允许 Outbox claim 更新走 PostgreSQL HOT：attempt_count 原部分索引会让每次 claim
/// 都重写索引，即使状态仍是 Pending。该索引只服务低频运维 MAX(attempt_count)，正式
/// 8 小时运行 Pending 峰值很小，顺序扫描活跃集合比每条消息维护索引更划算。
/// </summary>
public sealed class Migration056_OutboxHotClaimUpdates : IRealtimeSchemaMigration
{
    public int Version => 56;
    public string Name => "outbox_hot_claim_updates";
    public bool RequiresTransaction => false;

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using (var drop = new NpgsqlCommand(
            $"DROP INDEX CONCURRENTLY IF EXISTS {schema.QuotedSchema}.\"ix_outbox_pending_attempts\";",
            connection))
        {
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 为刚插入后很快被 claim 的 tuple 预留少量页内空间，提高 HOT 命中率。
        await using var fillFactor = new NpgsqlCommand(
            $"ALTER TABLE {schema.OutboxTableSql} SET (fillfactor = 90);",
            connection);
        await fillFactor.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
