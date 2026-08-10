using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 删除写热表上的严格冗余索引，降低每条消息和 Outbox 插入的 WAL/磁盘放大。
/// <para>
/// <c>ux_messages_conversation_sequence</c> 与旧非唯一索引列序相同，并覆盖所有运行时
/// 非 NULL 序列查询；<c>ix_outbox_target_user_event_type</c> 的左前缀覆盖单列用户查询。
/// 唯一约束、历史查询、账号清理和死信/Published 索引均保留。
/// </para>
/// </summary>
public sealed class Migration057_RemoveRedundantHotIndexes : IRealtimeSchemaMigration
{
    public int Version => 57;
    public string Name => "remove_redundant_hot_indexes";
    public bool RequiresTransaction => false;

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await DropIndexConcurrentlyAsync(
                connection,
                schema,
                "ix_messages_conversation_sequence",
                cancellationToken)
            .ConfigureAwait(false);
        await DropIndexConcurrentlyAsync(
                connection,
                schema,
                "ix_outbox_target_user_id",
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task DropIndexConcurrentlyAsync(
        NpgsqlConnection connection,
        RealtimeDatabaseSchema schema,
        string indexName,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"DROP INDEX CONCURRENTLY IF EXISTS {schema.QuotedSchema}.\"{indexName}\";",
            connection);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
