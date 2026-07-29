using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// P0-6：message_state 增加 recalled_at_ms 列并建立到 messages 的外键。
/// <para>
/// 问题：message_state 表原先无 FK 到 messages，Retention 删 messages 后 message_state 行成为孤儿。
/// 同时 Reaction 流程先无锁读 messages.recalled_at_ms，再锁 message_state，撤回可在两步间提交，
/// 导致 Reaction 在已撤回消息上落地。本迁移在 message_state 上增加 recalled_at_ms 列（从 messages 回填），
/// 并添加 FK ON DELETE CASCADE，使 Retention 删除 messages 时自动级联清理 message_state。
/// </para>
/// <para>
/// messages 表的 recalled_at_ms 列保留（不删除），其他查询仍可读取。
/// Reaction 操作改为从 message_state 读取 recalled_at_ms（在 FOR UPDATE 锁下），
/// 消除"无锁读 messages → 锁 message_state"两步间的撤回竞态。
/// </para>
/// </summary>
public sealed class Migration047_MessageStateFkAndRecall : IRealtimeSchemaMigration
{
    public int Version => 47;
    public string Name => "message_state_fk_and_recall";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        // 1. 添加 recalled_at_ms 列到 message_state（NULL 表示未撤回）
        await using (var addColCmd = new NpgsqlCommand(
                        $"""
                        ALTER TABLE {schema.MessageStateTableSql}
                        ADD COLUMN IF NOT EXISTS "recalled_at_ms" bigint NULL;
                        """,
                        connection,
                        transaction))
        {
            await addColCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 2. 回填 recalled_at_ms 从 messages 表（仅未撤回消息的 message_state 行保持 NULL）
        await using (var backfillCmd = new NpgsqlCommand(
                        $"""
                        UPDATE {schema.MessageStateTableSql} AS s
                        SET "recalled_at_ms" = m."recalled_at_ms"
                        FROM {schema.MessagesTableSql} AS m
                        WHERE s."message_id" = m."message_id"
                          AND m."recalled_at_ms" IS NOT NULL;
                        """,
                        connection,
                        transaction))
        {
            await backfillCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // 3. 添加 FK ON DELETE CASCADE：Retention 删 messages 时自动清理 message_state 孤儿行。
        // P0-6：FK 设为 DEFERRABLE INITIALLY DEFERRED，使 Recall/Reaction 可在事务内先 INSERT
        // message_state 行（确保行存在并加锁）再确认 messages 是否存在；不存在时回滚即可，
        // 不会因立即 FK 校验失败而抛异常。CASCADE 行为不受 DEFERRABLE 影响。
        // 注：PostgreSQL 的 ADD CONSTRAINT 不支持 IF NOT EXISTS（仅 ADD COLUMN 支持）；
        // 迁移幂等性由 RealtimeSchemaMigrationRunner 按 Version 记录已应用状态保证。
        await using (var fkCmd = new NpgsqlCommand(
                        $"""
                        ALTER TABLE {schema.MessageStateTableSql}
                        ADD CONSTRAINT fk_message_state_messages
                        FOREIGN KEY ("message_id") REFERENCES {schema.MessagesTableSql}("message_id")
                        ON DELETE CASCADE
                        DEFERRABLE INITIALLY DEFERRED;
                        """,
                        connection,
                        transaction))
        {
            await fkCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
