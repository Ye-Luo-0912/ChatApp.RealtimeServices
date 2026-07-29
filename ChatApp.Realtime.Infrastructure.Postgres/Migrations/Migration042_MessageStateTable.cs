using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 七-3：拆分独立的消息状态行，避免 Reaction 热点锁住 messages 正文行。
/// <para>
/// 热门消息上的所有 Reaction 原先通过 <c>SELECT ... FROM messages FOR UPDATE</c> 串行化，
/// 锁住整条 messages 记录，阻塞编辑/撤回等其他写入。本迁移新增 <c>message_state</c> 表，
/// 仅承载 <c>changed_at_ms</c> 用于 Reaction 串行化锁；messages 正文行不再被 FOR UPDATE 锁定。
/// </para>
/// <para>
/// Reaction 操作仍会更新 <c>messages.changed_at_ms</c>（供 Sync/History 增量查询使用），
/// 但该 UPDATE 是行级写锁，与 FOR UPDATE 持有整个事务相比显著降低了对正文行的占用时长。
/// <c>message_state</c> 的 <c>changed_at_ms</c> 列保留用于将来可能的进一步解耦，当前未直接读取。
/// </para>
/// <para>
/// message_id 类型与 messages 表保持一致（character varying(64)），避免类型不匹配导致索引失效。
/// </para>
/// </summary>
public sealed class Migration042_MessageStateTable : IRealtimeSchemaMigration
{
    public int Version => 42;
    public string Name => "message_state_table";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var stateTable = schema.MessageStateTableSql;

        // 1. 创建 message_state 表（独立于 messages 正文行）
        await using var createCmd = new NpgsqlCommand(
            $"""
            CREATE TABLE IF NOT EXISTS {stateTable} (
                "message_id" character varying(64) NOT NULL PRIMARY KEY,
                "changed_at_ms" bigint NOT NULL DEFAULT 0
            );
            """,
            connection,
            transaction);
        await createCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // 2. 回填：从 messages 表复制 changed_at_ms，为已存在消息预置状态行
        await using var backfillCmd = new NpgsqlCommand(
            $"""
            INSERT INTO {stateTable} ("message_id", "changed_at_ms")
            SELECT "message_id", COALESCE("changed_at_ms", 0)
            FROM {schema.MessagesTableSql}
            WHERE "changed_at_ms" IS NOT NULL
            ON CONFLICT ("message_id") DO NOTHING;
            """,
            connection,
            transaction);
        await backfillCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
