using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// OUTBOX-DB-1：删除 messages 表上两个从未被查询使用的引用索引，降低每条消息写入的 WAL/磁盘放大。
/// <para>
/// <c>ix_messages_reply_to</c>（reply_to_message_id 部分索引）与
/// <c>ix_messages_forwarded_from</c>（forwarded_from_message_id 部分索引）由 Migration013/015
/// 随字段一并创建，但代码库中没有任何读取路径按这两列过滤/排序/连接：
/// 历史读取全部走 <c>ux_messages_conversation_sequence</c> 等会话级索引，
/// reply_to_*/forwarded_from_* 仅作为展示性引用随行读出（SELECT 投影）与撤回降级时置 NULL（UPDATE）。
/// 两条路径都不需要这两列上的索引，因此它们只贡献插入/更新的索引写放大。
/// </para>
/// <para>
/// 与 Migration057/066 一致使用 <c>DROP INDEX CONCURRENTLY</c>，故 <see cref="RequiresTransaction"/>
/// 为 false；<c>IF EXISTS</c> 保证迁移可重入。
/// </para>
/// </summary>
public sealed class Migration068_RemoveUnusedReplyForwardIndexes : IRealtimeSchemaMigration
{
    public int Version => 68;
    public string Name => "remove_unused_reply_forward_indexes";
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
                "ix_messages_reply_to",
                cancellationToken)
            .ConfigureAwait(false);
        await DropIndexConcurrentlyAsync(
                connection,
                schema,
                "ix_messages_forwarded_from",
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
