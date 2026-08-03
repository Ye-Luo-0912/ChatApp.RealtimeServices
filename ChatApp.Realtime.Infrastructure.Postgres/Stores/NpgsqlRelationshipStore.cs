using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Outbox;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL 关系域权威实现。
/// <para>
/// 好友请求 / 友谊使用 realtime schema 表（Migration052），
/// 黑名单复用 public."T_BlockRecords"（与 <see cref="NpgsqlBlockListStore"/> 共享）。
/// </para>
/// <para>
/// 幂等：relationship_mutation_requests 表按 (actor_user_id, request_id) 去重，
/// 仅记录成功结果（失败不记录，重复失败无害）。
/// </para>
/// <para>
/// 事件发布：每次成功 mutation 在同一事务内通过 <see cref="OutboxInsertHelper"/>
/// 写入 realtime.outbox 表，由 <c>OutboxPublisherWorker</c> 异步发布到 NATS。
/// 事件类型：<see cref="RealtimeEventType.FriendRequestListChanged"/> /
/// <see cref="RealtimeEventType.FriendListChanged"/> /
/// <see cref="RealtimeEventType.BlockedListChanged"/>。
/// </para>
/// </summary>
public sealed class NpgsqlRelationshipStore : IRelationshipStore
{
    private const int FriendRequestStatusPending = 0;
    private const int FriendRequestStatusAccepted = 1;
    private const int FriendRequestStatusDeclined = 2;
    private const int DefaultPageSize = 50;
    private const int MaxPageSize = 200;

    private const string ResourceFriendRequest = "friend-request";
    private const string ResourceFriendship = "friendship";
    private const string ResourceBlockedUser = "blocked-user";

    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _schema;

    public NpgsqlRelationshipStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema schema)
    {
        _databaseClient = databaseClient;
        _schema = schema;
    }

    public async Task<RelationshipMutatePersistResult> SendFriendRequestAsync(
        string requestId, long actorUserId, long targetUserId, string? message,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default)
    {
        if (actorUserId == targetUserId)
            return RelationshipMutatePersistResult.Fail("cannot_friend_self", "不能向自己发送好友请求。");

        var (idempotentResult, fingerprint) = await TryReadIdempotencyAsync(
            actorUserId, requestId, (int)RelationshipOperation.SendFriendRequest, ct).ConfigureAwait(false);
        if (idempotentResult is not null)
            return idempotentResult.Value;

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            var (low, high) = CanonicalPair(actorUserId, targetUserId);
            await using (var checkFriend = new NpgsqlCommand(
                $"SELECT 1 FROM {_schema.FriendshipsTableSql} WHERE \"user_id_low\" = @low AND \"user_id_high\" = @high LIMIT 1",
                connection, transaction))
            {
                checkFriend.Parameters.AddWithValue("low", low);
                checkFriend.Parameters.AddWithValue("high", high);
                if (await checkFriend.ExecuteScalarAsync(ct).ConfigureAwait(false) is not null)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return RelationshipMutatePersistResult.Fail("already_friends", "已经是好友关系。");
                }
            }

            await using (var checkPending = new NpgsqlCommand(
                $"SELECT 1 FROM {_schema.FriendRequestsTableSql} WHERE \"requester_id\" = @r1 AND \"target_id\" = @t1 AND \"status\" = 0 " +
                $"UNION ALL " +
                $"SELECT 1 FROM {_schema.FriendRequestsTableSql} WHERE \"requester_id\" = @r2 AND \"target_id\" = @t2 AND \"status\" = 0 LIMIT 1",
                connection, transaction))
            {
                checkPending.Parameters.AddWithValue("r1", actorUserId);
                checkPending.Parameters.AddWithValue("t1", targetUserId);
                checkPending.Parameters.AddWithValue("r2", targetUserId);
                checkPending.Parameters.AddWithValue("t2", actorUserId);
                if (await checkPending.ExecuteScalarAsync(ct).ConfigureAwait(false) is not null)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return RelationshipMutatePersistResult.Fail("pending_request_exists", "已存在待处理的好友请求。");
                }
            }

            await using (var insert = new NpgsqlCommand(
                $"INSERT INTO {_schema.FriendRequestsTableSql} (\"request_id\", \"requester_id\", \"target_id\", \"message\", \"status\", \"created_at_ms\") " +
                $"VALUES (@request_id, @requester_id, @target_id, @message, @status, @created_at_ms)",
                connection, transaction))
            {
                insert.Parameters.AddWithValue("request_id", requestId);
                insert.Parameters.AddWithValue("requester_id", actorUserId);
                insert.Parameters.AddWithValue("target_id", targetUserId);
                insert.Parameters.AddWithValue("message", (object?)message ?? DBNull.Value);
                insert.Parameters.AddWithValue("status", (short)FriendRequestStatusPending);
                insert.Parameters.AddWithValue("created_at_ms", occurredAtMs);
                await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // 变更日志：target 的好友请求列表新增一条 Pending
            await AppendChangeLogAsync(
                connection, transaction,
                targetUserId, RelationshipListType.FriendRequests, RelationshipChangeOperation.Upsert,
                requestId, "Pending", message, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);

            // 事件发布：通知双方好友请求列表变更
            var events = new List<RealtimeEvent>(2)
            {
                CreateRelationshipEvent(
                    RealtimeEventType.FriendRequestListChanged,
                    targetUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendRequest, "Pending", requestId, message,
                    RelationshipEventIdFactory.CreateFriendRequestListChangedEventId(
                        targetUserId, actorUserId, requestId, occurredAtMs)),
                CreateRelationshipEvent(
                    RealtimeEventType.FriendRequestListChanged,
                    actorUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendRequest, "Pending", requestId, message,
                    RelationshipEventIdFactory.CreateFriendRequestListChangedEventId(
                        actorUserId, actorUserId, requestId, occurredAtMs)),
            };
            await OutboxInsertHelper.InsertManyAsync(connection, transaction, _schema, events, ct).ConfigureAwait(false);

            await RecordIdempotencyAsync(connection, transaction, actorUserId, requestId,
                (int)RelationshipOperation.SendFriendRequest, fingerprint, requestId, true, null, occurredAtMs, ct).ConfigureAwait(false);

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return RelationshipMutatePersistResult.Ok(requestId, targetUserId);
        }
        catch (PostgresException ex) when (ex.SqlState == "23505")
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return RelationshipMutatePersistResult.Fail("duplicate_request", "好友请求已存在。");
        }
    }

    public async Task<RelationshipMutatePersistResult> AcceptFriendRequestAsync(
        string requestId, long actorUserId, string requestIdToRespond,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default)
    {
        var (idempotentResult, fingerprint) = await TryReadIdempotencyAsync(
            actorUserId, requestId, (int)RelationshipOperation.AcceptFriendRequest, ct).ConfigureAwait(false);
        if (idempotentResult is not null)
            return idempotentResult.Value;

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            long requesterId;
            long targetId;
            await using (var load = new NpgsqlCommand(
                $"SELECT \"requester_id\", \"target_id\", \"status\" FROM {_schema.FriendRequestsTableSql} " +
                $"WHERE \"request_id\" = @request_id FOR UPDATE",
                connection, transaction))
            {
                load.Parameters.AddWithValue("request_id", requestIdToRespond);
                await using var reader = await load.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return RelationshipMutatePersistResult.Fail("request_not_found", "好友请求不存在。");
                }
                requesterId = reader.GetInt64(0);
                targetId = reader.GetInt64(1);
                var status = reader.GetInt16(2);
                if (status != FriendRequestStatusPending)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return RelationshipMutatePersistResult.Fail("request_not_pending", "好友请求已处理。");
                }
            }

            if (targetId != actorUserId)
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return RelationshipMutatePersistResult.Fail("not_request_target", "只有被请求方可以接受好友请求。");
            }

            await using (var update = new NpgsqlCommand(
                $"UPDATE {_schema.FriendRequestsTableSql} SET \"status\" = @status, \"responded_at_ms\" = @responded_at_ms " +
                $"WHERE \"request_id\" = @request_id",
                connection, transaction))
            {
                update.Parameters.AddWithValue("status", (short)FriendRequestStatusAccepted);
                update.Parameters.AddWithValue("responded_at_ms", occurredAtMs);
                update.Parameters.AddWithValue("request_id", requestIdToRespond);
                await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            var friendshipId = Guid.CreateVersion7().ToString("N");
            var (low, high) = CanonicalPair(requesterId, targetId);
            await using (var insertFriendship = new NpgsqlCommand(
                $"INSERT INTO {_schema.FriendshipsTableSql} (\"friendship_id\", \"user_id_low\", \"user_id_high\", \"created_at_ms\") " +
                $"VALUES (@friendship_id, @low, @high, @created_at_ms)",
                connection, transaction))
            {
                insertFriendship.Parameters.AddWithValue("friendship_id", friendshipId);
                insertFriendship.Parameters.AddWithValue("low", low);
                insertFriendship.Parameters.AddWithValue("high", high);
                insertFriendship.Parameters.AddWithValue("created_at_ms", occurredAtMs);
                await insertFriendship.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // 变更日志：接受者的待处理请求删除；请求者与接受者的好友列表新增
            await AppendChangeLogAsync(
                connection, transaction,
                actorUserId, RelationshipListType.FriendRequests, RelationshipChangeOperation.Delete,
                requestIdToRespond, null, null, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);
            await AppendChangeLogAsync(
                connection, transaction,
                requesterId, RelationshipListType.Friends, RelationshipChangeOperation.Upsert,
                friendshipId, "Accepted", null, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);
            await AppendChangeLogAsync(
                connection, transaction,
                actorUserId, RelationshipListType.Friends, RelationshipChangeOperation.Upsert,
                friendshipId, "Accepted", null, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);

            // 事件发布：好友请求状态变更通知双方 + 好友列表变更通知双方
            var events = new List<RealtimeEvent>(4)
            {
                // 好友请求列表：请求者收到 Accepted
                CreateRelationshipEvent(
                    RealtimeEventType.FriendRequestListChanged,
                    requesterId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendRequest, "Accepted", requestIdToRespond, null,
                    RelationshipEventIdFactory.CreateFriendRequestListChangedEventId(
                        requesterId, actorUserId, requestIdToRespond, occurredAtMs)),
                // 好友请求列表：接受者收到 Accepted
                CreateRelationshipEvent(
                    RealtimeEventType.FriendRequestListChanged,
                    actorUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendRequest, "Accepted", requestIdToRespond, null,
                    RelationshipEventIdFactory.CreateFriendRequestListChangedEventId(
                        actorUserId, actorUserId, requestIdToRespond, occurredAtMs)),
                // 好友列表：请求者新增好友
                CreateRelationshipEvent(
                    RealtimeEventType.FriendListChanged,
                    requesterId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendship, "changed", friendshipId, null,
                    RelationshipEventIdFactory.CreateFriendListChangedEventId(
                        requesterId, actorUserId, friendshipId, occurredAtMs)),
                // 好友列表：接受者新增好友
                CreateRelationshipEvent(
                    RealtimeEventType.FriendListChanged,
                    actorUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendship, "changed", friendshipId, null,
                    RelationshipEventIdFactory.CreateFriendListChangedEventId(
                        actorUserId, actorUserId, friendshipId, occurredAtMs)),
            };
            await OutboxInsertHelper.InsertManyAsync(connection, transaction, _schema, events, ct).ConfigureAwait(false);

            await RecordIdempotencyAsync(connection, transaction, actorUserId, requestId,
                (int)RelationshipOperation.AcceptFriendRequest, fingerprint, friendshipId, true, null, occurredAtMs, ct).ConfigureAwait(false);

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return RelationshipMutatePersistResult.Ok(friendshipId, requesterId);
        }
        catch (PostgresException ex) when (ex.SqlState == "23505")
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return RelationshipMutatePersistResult.Fail("already_friends", "已经是好友关系。");
        }
    }

    public async Task<RelationshipMutatePersistResult> DeclineFriendRequestAsync(
        string requestId, long actorUserId, string requestIdToRespond,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default)
    {
        var (idempotentResult, fingerprint) = await TryReadIdempotencyAsync(
            actorUserId, requestId, (int)RelationshipOperation.DeclineFriendRequest, ct).ConfigureAwait(false);
        if (idempotentResult is not null)
            return idempotentResult.Value;

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            long requesterId;
            long targetId;
            await using (var load = new NpgsqlCommand(
                $"SELECT \"requester_id\", \"target_id\", \"status\" FROM {_schema.FriendRequestsTableSql} " +
                $"WHERE \"request_id\" = @request_id FOR UPDATE",
                connection, transaction))
            {
                load.Parameters.AddWithValue("request_id", requestIdToRespond);
                await using var reader = await load.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return RelationshipMutatePersistResult.Fail("request_not_found", "好友请求不存在。");
                }
                requesterId = reader.GetInt64(0);
                targetId = reader.GetInt64(1);
                var status = reader.GetInt16(2);
                if (status != FriendRequestStatusPending)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return RelationshipMutatePersistResult.Fail("request_not_pending", "好友请求已处理。");
                }
            }

            if (targetId != actorUserId)
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return RelationshipMutatePersistResult.Fail("not_request_target", "只有被请求方可以拒绝好友请求。");
            }

            await using (var update = new NpgsqlCommand(
                $"UPDATE {_schema.FriendRequestsTableSql} SET \"status\" = @status, \"responded_at_ms\" = @responded_at_ms " +
                $"WHERE \"request_id\" = @request_id",
                connection, transaction))
            {
                update.Parameters.AddWithValue("status", (short)FriendRequestStatusDeclined);
                update.Parameters.AddWithValue("responded_at_ms", occurredAtMs);
                update.Parameters.AddWithValue("request_id", requestIdToRespond);
                await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // 变更日志：拒绝者的待处理请求删除
            await AppendChangeLogAsync(
                connection, transaction,
                actorUserId, RelationshipListType.FriendRequests, RelationshipChangeOperation.Delete,
                requestIdToRespond, null, null, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);

            // 事件发布：通知双方好友请求被拒绝
            var events = new List<RealtimeEvent>(2)
            {
                CreateRelationshipEvent(
                    RealtimeEventType.FriendRequestListChanged,
                    requesterId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendRequest, "Declined", requestIdToRespond, null,
                    RelationshipEventIdFactory.CreateFriendRequestListChangedEventId(
                        requesterId, actorUserId, requestIdToRespond, occurredAtMs)),
                CreateRelationshipEvent(
                    RealtimeEventType.FriendRequestListChanged,
                    actorUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendRequest, "Declined", requestIdToRespond, null,
                    RelationshipEventIdFactory.CreateFriendRequestListChangedEventId(
                        actorUserId, actorUserId, requestIdToRespond, occurredAtMs)),
            };
            await OutboxInsertHelper.InsertManyAsync(connection, transaction, _schema, events, ct).ConfigureAwait(false);

            await RecordIdempotencyAsync(connection, transaction, actorUserId, requestId,
                (int)RelationshipOperation.DeclineFriendRequest, fingerprint, requestIdToRespond, true, null, occurredAtMs, ct).ConfigureAwait(false);

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return RelationshipMutatePersistResult.Ok(requestIdToRespond, null);
        }
        catch (Exception)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
    }

    public async Task<RelationshipMutatePersistResult> RemoveFriendAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default)
    {
        var (idempotentResult, fingerprint) = await TryReadIdempotencyAsync(
            actorUserId, requestId, (int)RelationshipOperation.RemoveFriend, ct).ConfigureAwait(false);
        if (idempotentResult is not null)
            return idempotentResult.Value;

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            var (low, high) = CanonicalPair(actorUserId, targetUserId);
            string friendshipId;
            await using (var select = new NpgsqlCommand(
                $"SELECT \"friendship_id\" FROM {_schema.FriendshipsTableSql} WHERE \"user_id_low\" = @low AND \"user_id_high\" = @high",
                connection, transaction))
            {
                select.Parameters.AddWithValue("low", low);
                select.Parameters.AddWithValue("high", high);
                var found = await select.ExecuteScalarAsync(ct).ConfigureAwait(false);
                if (found is null)
                {
                    await transaction.RollbackAsync(ct).ConfigureAwait(false);
                    return RelationshipMutatePersistResult.Fail("not_friends", "不是好友关系。");
                }
                friendshipId = (string)found;
            }

            await using (var delete = new NpgsqlCommand(
                $"DELETE FROM {_schema.FriendshipsTableSql} WHERE \"user_id_low\" = @low AND \"user_id_high\" = @high",
                connection, transaction))
            {
                delete.Parameters.AddWithValue("low", low);
                delete.Parameters.AddWithValue("high", high);
                await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // 变更日志：双方好友列表删除
            await AppendChangeLogAsync(
                connection, transaction,
                actorUserId, RelationshipListType.Friends, RelationshipChangeOperation.Delete,
                friendshipId, null, null, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);
            await AppendChangeLogAsync(
                connection, transaction,
                targetUserId, RelationshipListType.Friends, RelationshipChangeOperation.Delete,
                friendshipId, null, null, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);

            // 事件发布：通知双方好友列表变更（删除）
            var events = new List<RealtimeEvent>(2)
            {
                CreateRelationshipEvent(
                    RealtimeEventType.FriendListChanged,
                    actorUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendship, "deleted", null, null,
                    RelationshipEventIdFactory.CreateFriendListChangedEventId(
                        actorUserId, actorUserId, null, occurredAtMs)),
                CreateRelationshipEvent(
                    RealtimeEventType.FriendListChanged,
                    targetUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceFriendship, "deleted", null, null,
                    RelationshipEventIdFactory.CreateFriendListChangedEventId(
                        targetUserId, actorUserId, null, occurredAtMs)),
            };
            await OutboxInsertHelper.InsertManyAsync(connection, transaction, _schema, events, ct).ConfigureAwait(false);

            await RecordIdempotencyAsync(connection, transaction, actorUserId, requestId,
                (int)RelationshipOperation.RemoveFriend, fingerprint, null, true, null, occurredAtMs, ct).ConfigureAwait(false);

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return RelationshipMutatePersistResult.Ok(null, targetUserId);
        }
        catch (Exception)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
    }

    public async Task<RelationshipMutatePersistResult> BlockUserAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default)
    {
        if (actorUserId == targetUserId)
            return RelationshipMutatePersistResult.Fail("cannot_block_self", "不能拉黑自己。");

        var (idempotentResult, fingerprint) = await TryReadIdempotencyAsync(
            actorUserId, requestId, (int)RelationshipOperation.BlockUser, ct).ConfigureAwait(false);
        if (idempotentResult is not null)
            return idempotentResult.Value;

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            await using (var insert = new NpgsqlCommand(
                """INSERT INTO public."T_BlockRecords" ("BlockerId", "BlockedUserId") VALUES (@blocker, @blocked) ON CONFLICT DO NOTHING""",
                connection, transaction))
            {
                insert.Parameters.AddWithValue("blocker", actorUserId);
                insert.Parameters.AddWithValue("blocked", targetUserId);
                await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // 变更日志：操作者黑名单新增
            await AppendChangeLogAsync(
                connection, transaction,
                actorUserId, RelationshipListType.BlockedUsers, RelationshipChangeOperation.Upsert,
                targetUserId.ToString(), "Blocked", null, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);

            // 事件发布：通知操作者黑名单变更（不通知被拉黑方）
            var events = new List<RealtimeEvent>(1)
            {
                CreateRelationshipEvent(
                    RealtimeEventType.BlockedListChanged,
                    actorUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceBlockedUser, "blocked", targetUserId.ToString(), null,
                    RelationshipEventIdFactory.CreateBlockedListChangedEventId(
                        actorUserId, targetUserId, "blocked", occurredAtMs)),
            };
            await OutboxInsertHelper.InsertManyAsync(connection, transaction, _schema, events, ct).ConfigureAwait(false);

            await RecordIdempotencyAsync(connection, transaction, actorUserId, requestId,
                (int)RelationshipOperation.BlockUser, fingerprint, null, true, null, occurredAtMs, ct).ConfigureAwait(false);

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return RelationshipMutatePersistResult.Ok(null, targetUserId);
        }
        catch (Exception)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
    }

    public async Task<RelationshipMutatePersistResult> UnblockUserAsync(
        string requestId, long actorUserId, long targetUserId,
        string? actorSessionId, long occurredAtMs, CancellationToken ct = default)
    {
        var (idempotentResult, fingerprint) = await TryReadIdempotencyAsync(
            actorUserId, requestId, (int)RelationshipOperation.UnblockUser, ct).ConfigureAwait(false);
        if (idempotentResult is not null)
            return idempotentResult.Value;

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            await using (var delete = new NpgsqlCommand(
                """DELETE FROM public."T_BlockRecords" WHERE "BlockerId" = @blocker AND "BlockedUserId" = @blocked""",
                connection, transaction))
            {
                delete.Parameters.AddWithValue("blocker", actorUserId);
                delete.Parameters.AddWithValue("blocked", targetUserId);
                await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // 变更日志：操作者黑名单删除
            await AppendChangeLogAsync(
                connection, transaction,
                actorUserId, RelationshipListType.BlockedUsers, RelationshipChangeOperation.Delete,
                targetUserId.ToString(), null, null, occurredAtMs, occurredAtMs, requestId, ct).ConfigureAwait(false);

            // 事件发布：通知操作者黑名单变更（不通知被取消拉黑方）
            var events = new List<RealtimeEvent>(1)
            {
                CreateRelationshipEvent(
                    RealtimeEventType.BlockedListChanged,
                    actorUserId, actorUserId, actorSessionId, occurredAtMs,
                    ResourceBlockedUser, "unblocked", targetUserId.ToString(), null,
                    RelationshipEventIdFactory.CreateBlockedListChangedEventId(
                        actorUserId, targetUserId, "unblocked", occurredAtMs)),
            };
            await OutboxInsertHelper.InsertManyAsync(connection, transaction, _schema, events, ct).ConfigureAwait(false);

            await RecordIdempotencyAsync(connection, transaction, actorUserId, requestId,
                (int)RelationshipOperation.UnblockUser, fingerprint, null, true, null, occurredAtMs, ct).ConfigureAwait(false);

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return RelationshipMutatePersistResult.Ok(null, targetUserId);
        }
        catch (Exception)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
    }

    public async Task<IReadOnlyList<RelationshipListItem>> ListFriendsAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default)
    {
        var size = ClampPageSize(pageSize);
        var offset = DecodeCursor(cursor);

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        var afterClause = afterChangedAtMs > 0
            ? " AND f.\"created_at_ms\" > @after"
            : string.Empty;
        await using var command = new NpgsqlCommand(
            $"SELECT f.\"friendship_id\", CASE WHEN f.\"user_id_low\" = @uid THEN f.\"user_id_high\" ELSE f.\"user_id_low\" END AS friend_id, " +
            $"f.\"created_at_ms\" FROM {_schema.FriendshipsTableSql} f " +
            $"WHERE (f.\"user_id_low\" = @uid OR f.\"user_id_high\" = @uid){afterClause} " +
            $"ORDER BY f.\"created_at_ms\" DESC, friend_id DESC LIMIT @limit OFFSET @offset",
            connection);
        command.Parameters.AddWithValue("uid", actorUserId);
        command.Parameters.AddWithValue("limit", size + 1);
        command.Parameters.AddWithValue("offset", offset);
        if (afterChangedAtMs > 0)
            command.Parameters.AddWithValue("after", afterChangedAtMs);

        var results = new List<RelationshipListItem>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (results.Count >= size) break;
            results.Add(new RelationshipListItem
            {
                UserId = reader.GetInt64(1),
                ResourceId = reader.GetString(0),
                Status = "Accepted",
                CreatedAtMs = reader.GetInt64(2)
            });
        }
        return results;
    }

    public async Task<IReadOnlyList<RelationshipListItem>> ListFriendRequestsAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default)
    {
        var size = ClampPageSize(pageSize);
        var offset = DecodeCursor(cursor);

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        var afterClause = afterChangedAtMs > 0
            ? " AND \"created_at_ms\" > @after"
            : string.Empty;
        await using var command = new NpgsqlCommand(
            $"SELECT \"request_id\", \"requester_id\", \"message\", \"created_at_ms\" FROM {_schema.FriendRequestsTableSql} " +
            $"WHERE \"target_id\" = @uid AND \"status\" = 0{afterClause} " +
            $"ORDER BY \"created_at_ms\" DESC, \"requester_id\" DESC LIMIT @limit OFFSET @offset",
            connection);
        command.Parameters.AddWithValue("uid", actorUserId);
        command.Parameters.AddWithValue("limit", size + 1);
        command.Parameters.AddWithValue("offset", offset);
        if (afterChangedAtMs > 0)
            command.Parameters.AddWithValue("after", afterChangedAtMs);

        var results = new List<RelationshipListItem>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (results.Count >= size) break;
            results.Add(new RelationshipListItem
            {
                UserId = reader.GetInt64(1),
                ResourceId = reader.GetString(0),
                Status = "Pending",
                Message = reader.IsDBNull(2) ? null : reader.GetString(2),
                CreatedAtMs = reader.GetInt64(3)
            });
        }
        return results;
    }

    public async Task<IReadOnlyList<RelationshipListItem>> ListBlockedUsersAsync(
        long actorUserId, int? pageSize, string? cursor,
        long afterChangedAtMs = 0, CancellationToken ct = default)
    {
        // Block-list table (T_BlockRecords) has no change timestamp; watermark is
        // ignored here and the caller diffs client-side.
        _ = afterChangedAtMs;
        var size = ClampPageSize(pageSize);
        var offset = DecodeCursor(cursor);

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            """SELECT "BlockedUserId" FROM public."T_BlockRecords" WHERE "BlockerId" = @blocker ORDER BY "BlockedUserId" DESC LIMIT @limit OFFSET @offset""",
            connection);
        command.Parameters.AddWithValue("blocker", actorUserId);
        command.Parameters.AddWithValue("limit", size + 1);
        command.Parameters.AddWithValue("offset", offset);

        var results = new List<RelationshipListItem>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (results.Count >= size) break;
            results.Add(new RelationshipListItem
            {
                UserId = reader.GetInt64(0),
                ResourceId = reader.GetInt64(0).ToString(),
                Status = "Blocked",
                CreatedAtMs = 0
            });
        }
        return results;
    }

    public async Task<IReadOnlyList<RelationshipChangeLogEntry>> ListChangesAsync(
        long userId, RelationshipListType listType, long afterSequence, int limit, CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"SELECT \"change_sequence\",\"operation\",\"resource_id\",\"status\",\"message\",\"created_at_ms\",\"occurred_at_ms\",\"request_id\" " +
            $"FROM {_schema.RelationshipChangeLogTableSql} " +
            $"WHERE \"user_id\"=@uid AND \"list_type\"=@lt AND \"change_sequence\">@after " +
            $"ORDER BY \"change_sequence\" ASC LIMIT @limit",
            connection);
        command.Parameters.AddWithValue("uid", userId);
        command.Parameters.AddWithValue("lt", (short)listType);
        command.Parameters.AddWithValue("after", afterSequence);
        command.Parameters.AddWithValue("limit", limit + 1);

        var results = new List<RelationshipChangeLogEntry>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            results.Add(new RelationshipChangeLogEntry
            {
                ChangeSequence = reader.GetInt64(0),
                Operation = (RelationshipChangeOperation)reader.GetInt16(1),
                ResourceId = reader.GetString(2),
                UserId = userId,
                Status = reader.IsDBNull(3) ? null : reader.GetString(3),
                Message = reader.IsDBNull(4) ? null : reader.GetString(4),
                CreatedAtMs = reader.GetInt64(5),
                OccurredAtMs = reader.GetInt64(6),
                RequestId = reader.IsDBNull(7) ? null : reader.GetString(7)
            });
        }
        return results;
    }

    public async Task<long> GetRelationshipRetentionFloorAsync(
        long userId, RelationshipListType listType, CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"SELECT COALESCE(MIN(\"change_sequence\"), 0) FROM {_schema.RelationshipChangeLogTableSql} " +
            $"WHERE \"user_id\"=@uid AND \"list_type\"=@lt",
            connection);
        command.Parameters.AddWithValue("uid", userId);
        command.Parameters.AddWithValue("lt", (short)listType);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is long value ? value : 0L;
    }

    private async Task AppendChangeLogAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction,
        long userId, RelationshipListType listType, RelationshipChangeOperation operation,
        string resourceId, string? status, string? message, long createdAtMs, long occurredAtMs, string? requestId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_schema.RelationshipChangeLogTableSql}
             ("change_sequence", "user_id", "list_type", "operation", "resource_id", "status", "message", "created_at_ms", "occurred_at_ms", "request_id")
             VALUES (nextval('{_schema.RelationshipChangeLogSequenceSql}'), @user_id, @list_type, @operation, @resource_id, @status, @message, @created_at_ms, @occurred_at_ms, @request_id)
             """,
            connection, transaction);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("list_type", (short)listType);
        command.Parameters.AddWithValue("operation", (short)operation);
        command.Parameters.AddWithValue("resource_id", resourceId);
        command.Parameters.AddWithValue("status", (object?)status ?? DBNull.Value);
        command.Parameters.AddWithValue("message", (object?)message ?? DBNull.Value);
        command.Parameters.AddWithValue("created_at_ms", createdAtMs);
        command.Parameters.AddWithValue("occurred_at_ms", occurredAtMs);
        command.Parameters.AddWithValue("request_id", (object?)requestId ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
    private async Task<(RelationshipMutatePersistResult? Result, string Fingerprint)> TryReadIdempotencyAsync(
        long actorUserId, string requestId, int operation, CancellationToken ct)
    {
        var fingerprint = ComputeFingerprint(actorUserId, requestId, operation);
        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"SELECT \"succeeded\", \"error_code\", \"resource_id\" FROM {_schema.RelationshipMutationRequestsTableSql} " +
            $"WHERE \"actor_user_id\" = @actor AND \"request_id\" = @request_id",
            connection);
        command.Parameters.AddWithValue("actor", actorUserId);
        command.Parameters.AddWithValue("request_id", requestId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var succeeded = reader.GetBoolean(0);
            var errorCode = reader.IsDBNull(1) ? null : reader.GetString(1);
            var resourceId = reader.IsDBNull(2) ? null : reader.GetString(2);
            if (succeeded)
                return (RelationshipMutatePersistResult.Ok(resourceId, null), fingerprint);
            return (RelationshipMutatePersistResult.Fail(errorCode ?? "unknown", "之前的请求已失败。"), fingerprint);
        }
        return (null, fingerprint);
    }

    private async Task RecordIdempotencyAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction,
        long actorUserId, string requestId, int operation, string fingerprint,
        string? resourceId, bool succeeded, string? errorCode, long occurredAtMs, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"INSERT INTO {_schema.RelationshipMutationRequestsTableSql} " +
            $"(\"actor_user_id\", \"request_id\", \"operation\", \"request_fingerprint\", \"resource_id\", \"succeeded\", \"error_code\", \"created_at_ms\") " +
            $"VALUES (@actor, @request_id, @operation, @fingerprint, @resource_id, @succeeded, @error_code, @created_at_ms) " +
            $"ON CONFLICT (\"actor_user_id\", \"request_id\") DO NOTHING",
            connection, transaction);
        command.Parameters.AddWithValue("actor", actorUserId);
        command.Parameters.AddWithValue("request_id", requestId);
        command.Parameters.AddWithValue("operation", (short)operation);
        command.Parameters.AddWithValue("fingerprint", fingerprint);
        command.Parameters.AddWithValue("resource_id", (object?)resourceId ?? DBNull.Value);
        command.Parameters.AddWithValue("succeeded", succeeded);
        command.Parameters.AddWithValue("error_code", (object?)errorCode ?? DBNull.Value);
        command.Parameters.AddWithValue("created_at_ms", occurredAtMs);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 构造关系域实时事件。PayloadJson 使用 <see cref="RealtimeDomainNotificationPayload"/>，
    /// 与 Gateway 侧 <c>RelationshipListHandler</c> 解析格式一致。
    /// </summary>
    private static RealtimeEvent CreateRelationshipEvent(
        RealtimeEventType type,
        long targetUserId,
        long actorUserId,
        string? actorSessionId,
        long occurredAtMs,
        string resource,
        string action,
        string? resourceId,
        string? message,
        string eventId)
    {
        var payload = new RealtimeDomainNotificationPayload
        {
            Resource = resource,
            Action = action,
            ResourceId = resourceId,
            Message = message,
        };
        return new RealtimeEvent
        {
            EventId = eventId,
            Type = type,
            TargetUserId = targetUserId,
            ActorUserId = actorUserId,
            SessionId = actorSessionId,
            PayloadJson = JsonSerializer.Serialize(
                payload, RealtimeJsonSerializerContext.Default.RealtimeDomainNotificationPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = RealtimeTraceContext.CaptureTraceParent(),
            TraceState = RealtimeTraceContext.CaptureTraceState(),
        };
    }

    private static string ComputeFingerprint(long actorUserId, string requestId, int operation) =>
        $"{actorUserId}:{requestId}:{operation}";

    private static (long Low, long High) CanonicalPair(long a, long b) =>
        a <= b ? (a, b) : (b, a);

    private static int ClampPageSize(int? pageSize) =>
        Math.Clamp(pageSize is null or 0 ? DefaultPageSize : pageSize.Value, 1, MaxPageSize);

    private static int DecodeCursor(string? cursor)
    {
        if (string.IsNullOrWhiteSpace(cursor)) return 0;
        try
        {
            var bytes = Convert.FromBase64String(cursor);
            return BitConverter.ToInt32(bytes, 0);
        }
        catch
        {
            return 0;
        }
    }
}
