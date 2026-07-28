using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// Perf-2：Outbox 表持久化 RealtimeEvent 的会话级受众路由字段。
/// <para>
/// 新增列：
/// - <c>audience_kind</c>：事件受众类型（0=User, 1=Conversation），决定 Publisher 路由策略。
/// - <c>conversation_id</c>：会话级路由使用的会话编号，仅当 audience_kind=1 时有效。
/// </para>
/// <para>
/// 配合 <c>ix_outbox_conversation_id</c> 部分索引，支持按会话批量扫描/重放聚合事件，
/// 使 GroupProjectionDelta.AddBroadcast 产出的会话级聚合事件可被高效检索。
/// </para>
/// </summary>
public sealed class Migration033_OutboxConversationAudience : IRealtimeSchemaMigration
{
    public int Version => 33;
    public string Name => "outbox_conversation_audience";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var outbox = schema.OutboxTableSql;

        await using var command = new NpgsqlCommand(
            $"""
            ALTER TABLE {outbox}
                ADD COLUMN IF NOT EXISTS "audience_kind" smallint NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS "conversation_id" varchar(128) NULL;

            CREATE INDEX IF NOT EXISTS "ix_outbox_conversation_id"
            ON {outbox} (conversation_id)
            WHERE conversation_id IS NOT NULL;
            """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
