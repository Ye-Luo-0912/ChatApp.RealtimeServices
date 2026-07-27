using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL 群操作审计存储。
/// 审计写入为 best-effort：异常被捕获并记录，不阻断主流程。
/// </summary>
public sealed class NpgsqlGroupOperationAuditStore(
    RealtimeDatabaseClient databaseClient,
    RealtimeDatabaseSchema databaseSchema,
    ILogger<NpgsqlGroupOperationAuditStore> logger) : IGroupOperationAuditStore
{
    public async Task RecordAsync(GroupOperationAuditEntry entry, CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured)
            return;

        try
        {
            await using var connection = await databaseClient.GetDataSource()
                .OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            await using var command = new NpgsqlCommand(
                $"""
                 INSERT INTO {databaseSchema.GroupOperationAuditTableSql}
                     (actor_user_id, conversation_id, operation, target_user_id,
                      previous_role, new_role, request_id, actor_session_id,
                      succeeded, error_code, occurred_at_ms)
                 VALUES
                     (@actor_user_id, @conversation_id, @operation, @target_user_id,
                      @previous_role, @new_role, @request_id, @actor_session_id,
                      @succeeded, @error_code, @occurred_at_ms);
                 """,
                connection);

            command.Parameters.AddWithValue("actor_user_id", entry.ActorUserId);
            command.Parameters.AddWithValue("conversation_id", (object?)entry.ConversationId ?? DBNull.Value);
            command.Parameters.AddWithValue("operation", (byte)entry.Operation);
            command.Parameters.AddWithValue("target_user_id", (object?)entry.TargetUserId ?? DBNull.Value);
            command.Parameters.AddWithValue("previous_role", entry.PreviousRole.HasValue ? (byte)entry.PreviousRole.Value : DBNull.Value);
            command.Parameters.AddWithValue("new_role", entry.NewRole.HasValue ? (byte)entry.NewRole.Value : DBNull.Value);
            command.Parameters.AddWithValue("request_id", entry.RequestId);
            command.Parameters.AddWithValue("actor_session_id", (object?)entry.ActorSessionId ?? DBNull.Value);
            command.Parameters.AddWithValue("succeeded", entry.Succeeded);
            command.Parameters.AddWithValue("error_code", (object?)entry.ErrorCode ?? DBNull.Value);
            command.Parameters.AddWithValue("occurred_at_ms", entry.OccurredAtMs);

            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex,
                "群操作审计写入失败（不阻断主流程）。actor={ActorUserId}; op={Operation}; request={RequestId}",
                entry.ActorUserId,
                entry.Operation,
                entry.RequestId);
        }
    }
}
