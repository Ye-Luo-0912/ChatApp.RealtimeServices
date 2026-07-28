using System.Data.Common;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL 群操作审计存储。
/// <para>
/// 提供两条写入路径：
/// <list type="bullet">
/// <item><see cref="RecordAsync"/>：事务外 best-effort 写入，独立连接，异常被吞掉并记录日志。
/// 用于失败尝试审计（业务事务已回滚，无法在事务内记录）。</item>
/// <item><see cref="RecordInTransactionAsync"/>：业务事务内写入（审计 Outbox），复用调用方连接与事务，
/// 异常向上抛出导致业务事务回滚，保证“业务变更成功 ⇒ 审计已记录”的原子性。</item>
/// </list>
/// </para>
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

            await ExecuteInsertAsync(
                connection,
                transaction: null,
                entry,
                ct).ConfigureAwait(false);
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

    /// <summary>
    /// 在业务事务内记录审计（审计 Outbox）。复用调用方已有的连接与事务。
    /// <para>
    /// 与 <see cref="RecordAsync"/> 不同：事务内失败不再被吞掉——审计异常向上抛出，
    /// 让整个业务事务回滚，保证审计记录与业务变更同生共死。
    /// </para>
    /// </summary>
    public async Task RecordInTransactionAsync(
        GroupOperationAuditEntry entry,
        DbConnection connection,
        DbTransaction transaction,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured)
            return;

        // Abstractions 层使用 System.Data.Common 抽象；Npgsql 实现层向下转型。
        var npgsqlConnection = (NpgsqlConnection)connection;
        var npgsqlTransaction = (NpgsqlTransaction)transaction;

        // 审计 Outbox：事务内失败向上抛出，让调用方回滚整个事务。
        await ExecuteInsertAsync(
            npgsqlConnection,
            npgsqlTransaction,
            entry,
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 共享 INSERT 实现：RecordAsync（无事务）与 RecordInTransactionAsync（有事务）复用。
    /// </summary>
    private async Task ExecuteInsertAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        GroupOperationAuditEntry entry,
        CancellationToken ct)
    {
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
            connection,
            transaction);

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
}
