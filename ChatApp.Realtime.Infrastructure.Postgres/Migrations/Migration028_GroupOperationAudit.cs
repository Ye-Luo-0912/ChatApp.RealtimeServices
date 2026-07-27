using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

/// <summary>
/// 创建 group_operation_audit 审计表。
/// 记录所有群管理操作：创建、加人、移除、退群、角色变更。
/// </summary>
public sealed class Migration028_GroupOperationAudit : IRealtimeSchemaMigration
{
    public int Version => 28;
    public string Name => "group_operation_audit";

    public async Task ApplyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        RealtimeDatabaseSchema schema,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {schema.GroupOperationAuditTableSql} (
                 "audit_id" bigserial NOT NULL PRIMARY KEY,
                 "actor_user_id" bigint NOT NULL,
                 "conversation_id" character varying(64) NULL,
                 "operation" smallint NOT NULL,
                 "target_user_id" bigint NULL,
                 "previous_role" smallint NULL,
                 "new_role" smallint NULL,
                 "request_id" character varying(64) NOT NULL,
                 "actor_session_id" character varying(128) NULL,
                 "succeeded" boolean NOT NULL,
                 "error_code" character varying(64) NULL,
                 "occurred_at_ms" bigint NOT NULL
             );

             CREATE INDEX IF NOT EXISTS "ix_group_operation_audit_actor_time"
                 ON {schema.GroupOperationAuditTableSql} ("actor_user_id", "occurred_at_ms" DESC);

             CREATE INDEX IF NOT EXISTS "ix_group_operation_audit_conversation_time"
                 ON {schema.GroupOperationAuditTableSql} ("conversation_id", "occurred_at_ms" DESC);
             """,
            connection,
            transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
