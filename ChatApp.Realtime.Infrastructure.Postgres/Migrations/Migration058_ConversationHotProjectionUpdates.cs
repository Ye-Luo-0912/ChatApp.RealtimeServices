using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 让每条消息触发的 conversation tip 更新能够使用 PostgreSQL HOT。
/// <para>
/// 会话列表先按 user_id 从 conversation_members 取候选，再联接 conversations 并排序；
/// 全局 <c>(last_message_at_ms, conversation_id)</c> 索引不覆盖 user/pinned 谓词，正式
/// 8 小时运行扫描次数为 0，却让 230 万次 tip 更新全部无法 HOT。
/// </para>
/// </summary>
public sealed class Migration058_ConversationHotProjectionUpdates : IRealtimeSchemaMigration
{
    public int Version => 58;
    public string Name => "conversation_hot_projection_updates";
    public bool RequiresTransaction => false;

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using (var drop = new NpgsqlCommand(
            $"DROP INDEX CONCURRENTLY IF EXISTS {schema.QuotedSchema}.\"ix_conversations_last_message_list\";",
            connection))
        {
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // tip 行非常热，保留页内空间给 HOT 版本链，避免 tuple/索引/WAL 放大。
        await using var fillFactor = new NpgsqlCommand(
            $"ALTER TABLE {schema.ConversationsTableSql} SET (fillfactor = 80);",
            connection);
        await fillFactor.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
