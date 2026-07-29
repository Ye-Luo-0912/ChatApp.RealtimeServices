using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// P0-1：重建会话列表索引，移除成员行的 last_message_at_ms 列。
/// <para>
/// 列表排序改用 conversations.last_message_at_ms（群消息热路径不再更新成员行），
/// 旧索引包含 last_message_at_ms 已无意义且增加维护成本。新索引改为部分索引
/// （WHERE left_at_ms IS NULL），仅覆盖活跃成员行，减小体积并匹配列表查询谓词。
/// </para>
/// <para>
/// RequiresTransaction=false：CREATE/DROP INDEX CONCURRENTLY 不可在事务内执行。
/// 通过 ConcurrentIndexHelper 处理构建被中断后遗留的 INVALID 索引。
/// </para>
/// </summary>
public sealed class Migration044_ConversationListIndexUpdate : IRealtimeSchemaMigration
{
    private const string IndexName = "ix_conversation_members_user_pinned_list";

    public int Version => 44;
    public string Name => "conversation_list_index_update";
    public bool RequiresTransaction => false;

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var members = schema.ConversationMembersTableSql;
        var quotedSchema = schema.QuotedSchema;

        // 1. DROP 旧索引（包含 last_message_at_ms，不再需要）。
        // CONCURRENTLY IF EXISTS：避免索引不存在时报错，且不阻塞写入。
        await using var drop = new NpgsqlCommand(
            $"DROP INDEX CONCURRENTLY IF EXISTS {quotedSchema}.\"{IndexName}\";",
            connection);
        await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // 2. CREATE 新索引：不含 last_message_at_ms，加 WHERE left_at_ms IS NULL 部分索引。
        //    LongTerm-3：通过 ConcurrentIndexHelper 检查 indisvalid，INVALID 时自动 DROP 后重建。
        await ConcurrentIndexHelper.EnsureValidAsync(
                connection,
                quotedSchema,
                schema.Schema,
                IndexName,
                $"""
                 CREATE INDEX CONCURRENTLY "{IndexName}"
                     ON {members} (
                         "user_id",
                         "is_pinned" DESC,
                         "pinned_at_ms" DESC NULLS LAST,
                         "conversation_id" DESC
                     )
                     WHERE "left_at_ms" IS NULL;
                 """,
                cancellationToken)
            .ConfigureAwait(false);
    }
}
