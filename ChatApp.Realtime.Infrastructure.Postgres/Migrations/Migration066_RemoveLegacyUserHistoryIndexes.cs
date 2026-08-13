using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// OUTBOX-DB-1：删除 messages 表上两个 legacy 每用户历史索引，降低每条消息插入的 WAL/磁盘放大。
/// <para>
/// <c>ix_messages_receiver_history</c>（receiver_user_id, received_at_ms, message_id）与
/// <c>ix_messages_sender_history</c>（sender_user_id, received_at_ms, message_id）是旧版
/// 每用户收件箱历史查询的索引，每条消息（sender 与 receiver 均非空）都会写入其中。
/// 现有历史读取路径已全部改为按会话查询（<c>ix_messages_conversation_history</c> /
/// <c>ux_messages_conversation_sequence</c> / <c>ix_messages_sender_sequence_lookup</c>），
/// 不再依赖这两个每用户索引；账号删除（DeleteByUserAsync）本就走 ctid 分批删除，
/// 删除后改用顺序扫描对该低频操作可接受。
/// </para>
/// <para>
/// 与 Migration057 一致使用 <c>DROP INDEX CONCURRENTLY</c>，故 <see cref="RequiresTransaction"/>
/// 为 false；<c>IF EXISTS</c> 保证迁移可重入。
/// </para>
/// </summary>
public sealed class Migration066_RemoveLegacyUserHistoryIndexes : IRealtimeSchemaMigration
{
    public int Version => 66;
    public string Name => "remove_legacy_user_history_indexes";
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
                "ix_messages_receiver_history",
                cancellationToken)
            .ConfigureAwait(false);
        await DropIndexConcurrentlyAsync(
                connection,
                schema,
                "ix_messages_sender_history",
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