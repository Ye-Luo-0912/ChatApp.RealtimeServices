using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 删除 Outbox Pending 的重复 created_at 索引，降低每条事件插入的索引和 WAL 放大。
/// <para>
/// 恢复领取仍由 <c>ix_outbox_pending(next_attempt_at_ms, created_at_ms)</c> 支撑；
/// Pending 运维统计/列表是低频路径，且活跃集合通常很小，不值得为每条事件维护第二棵
/// 部分索引。Dead、Published 清理及按目标用户查询索引不受影响。
/// </para>
/// </summary>
public sealed class Migration059_RemoveRedundantPendingCreatedIndex : IRealtimeSchemaMigration
{
    public int Version => 59;
    public string Name => "remove_redundant_pending_created_index";
    public bool RequiresTransaction => false;

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"DROP INDEX CONCURRENTLY IF EXISTS {schema.QuotedSchema}.\"ix_outbox_pending_created\";",
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
