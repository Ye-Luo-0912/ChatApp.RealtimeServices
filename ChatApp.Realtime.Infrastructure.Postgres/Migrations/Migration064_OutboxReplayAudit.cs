using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// OUTBOX-RECOVERY-1：死信重放审计表。每次把 Dead 行重置为 Pending 时，在同一事务内
/// 记录原始 event_id、重放前尝试次数、失败分类（last_error）、操作者/原因以及新 checkpoint
/// （重置后的 next_attempt 时间戳）。运维可从审计条目追到恢复结果，而不会丢失重放前状态。
/// </summary>
public sealed class Migration064_OutboxReplayAudit : IRealtimeSchemaMigration
{
    public int Version => 64;
    public string Name => "outbox_replay_audit";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        var commands = new[]
        {
            $"""
             CREATE TABLE IF NOT EXISTS {schema.OutboxReplayAuditTableSql} (
                 "event_id" character varying(64) NOT NULL,
                 "replayed_at_ms" bigint NOT NULL,
                 "prior_attempt_count" integer NOT NULL,
                 "prior_last_error" character varying(2048) NULL,
                 "operator" character varying(128) NOT NULL,
                 "reason" character varying(512) NULL,
                 "new_checkpoint_ms" bigint NOT NULL,
                 PRIMARY KEY ("event_id", "replayed_at_ms")
             );
             """,
            $"""
             CREATE INDEX IF NOT EXISTS "ix_outbox_replay_audit_replayed_at"
             ON {schema.OutboxReplayAuditTableSql} ("replayed_at_ms");
             """
        };

        foreach (var sql in commands)
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}