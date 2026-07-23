using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 会话列表按最后消息时间倒序的覆盖索引�?
/// </summary>
public sealed class Migration006_ConversationListIndex : IRealtimeSchemaMigration
{
    public int Version => 6;
    public string Name => "conversation_list_index";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             CREATE INDEX IF NOT EXISTS "ix_conversations_last_message_list"
                 ON {schema.ConversationsTableSql}
                 ("last_message_at_ms" DESC NULLS LAST, "conversation_id" DESC);
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
