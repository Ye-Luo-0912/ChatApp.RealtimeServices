using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Protocol;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeGroupStore : IRealtimeGroupStore
{
    public const int MaxMembersPerGroup = 200;
    public const int MaxAddMembersPerRequest = 50;

    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;

    public NpgsqlRealtimeGroupStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
    }

    public async Task<GroupCreatePersistResult> CreateGroupAsync(
        long creatorUserId,
        string conversationId,
        string title,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        var members = NormalizeCreateMembers(creatorUserId, memberUserIds);
        if (members.Count > MaxMembersPerGroup)
        {
            return GroupCreatePersistResult.Fail(
                "too_many_members",
                $"群成员数不能超过 {MaxMembersPerGroup}。");
        }
        if (members.Count > RealtimeWireLimits.MaxTargetUserIdsPerEvent)
        {
            return GroupCreatePersistResult.Fail(
                "too_many_members",
                $"群成员数超过单事件目标上限 {RealtimeWireLimits.MaxTargetUserIdsPerEvent}。");
        }

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        await using (var insertConv = new NpgsqlCommand(
                         $"""
                          INSERT INTO {_databaseSchema.ConversationsTableSql} (
                              conversation_id, type, created_at_ms, updated_at_ms,
                              title, created_by_user_id
                          ) VALUES (
                              @conversation_id, @type, @occurred_at_ms, @occurred_at_ms,
                              @title, @created_by
                          );
                          """,
                         connection,
                         transaction))
        {
            insertConv.Parameters.AddWithValue("conversation_id", conversationId);
            insertConv.Parameters.AddWithValue("type", (short)ConversationType.Group);
            insertConv.Parameters.AddWithValue("occurred_at_ms", occurredAtMs);
            insertConv.Parameters.AddWithValue("title", title);
            insertConv.Parameters.AddWithValue("created_by", creatorUserId);
            try
            {
                await insertConv.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return GroupCreatePersistResult.Fail(
                    "conversation_id_conflict",
                    "会话编号冲突，请重试。");
            }
        }

        var memberItems = new List<ConversationMemberItem>(members.Count);
        var userIds = new long[members.Count];
        var roles = new short[members.Count];
        for (var i = 0; i < members.Count; i++)
        {
            var userId = members[i];
            var role = userId == creatorUserId
                ? ConversationMemberRole.Owner
                : ConversationMemberRole.Member;
            userIds[i] = userId;
            roles[i] = (short)role;
            memberItems.Add(new ConversationMemberItem
            {
                UserId = userId,
                Role = role,
                JoinedAtMs = occurredAtMs
            });
        }

        await InsertMembersBatchAsync(
                connection,
                transaction,
                conversationId,
                userIds,
                roles,
                occurredAtMs,
                ct)
            .ConfigureAwait(false);

        var events = new List<RealtimeEvent>(2);
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var targetUserIds = memberItems.Select(m => m.UserId).ToArray();

        events.Add(CreateConversationCreatedAggregatedEvent(
            conversationId,
            title,
            creatorUserId,
            targetUserIds,
            occurredAtMs,
            actorSessionId,
            traceParent,
            traceState,
            causeToken: $"group-created:{occurredAtMs}"));

        var addedMembers = memberItems.Where(m => m.UserId != creatorUserId).ToList();
        if (addedMembers.Count > 0)
        {
            events.Add(CreateMembersAddedEvent(
                conversationId,
                title,
                addedMembers,
                creatorUserId,
                targetUserIds,
                occurredAtMs,
                actorSessionId,
                traceParent,
                traceState));
        }

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupCreatePersistResult.Ok(conversationId, title, memberItems);
    }

    public async Task<GroupMutatePersistResult> AddMembersAsync(
        long actorUserId,
        string conversationId,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        var toAdd = NormalizeDistinctPositive(memberUserIds);
        if (toAdd.Count == 0)
            return GroupMutatePersistResult.Fail("invalid_members", "至少需要一名有效成员。");
        if (toAdd.Count > MaxAddMembersPerRequest)
        {
            return GroupMutatePersistResult.Fail(
                "too_many_members",
                $"单次添加不能超过 {MaxAddMembersPerRequest} 人。");
        }
        if (toAdd.Count > RealtimeWireLimits.MaxMembersPerGroupChange)
        {
            return GroupMutatePersistResult.Fail(
                "too_many_members",
                $"单次成员变更超过上限 {RealtimeWireLimits.MaxMembersPerGroupChange}，请分批。");
        }

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var (title, actorRole, existingCount) = await LoadGroupContextAsync(
                connection,
                transaction,
                conversationId,
                actorUserId,
                ct)
            .ConfigureAwait(false);
        if (title is null || actorRole is null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("not_found", "群不存在或当前用户不是成员。");
        }

        if (!CanManageMembers(actorRole.Value))
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("forbidden", "仅 Owner / Admin 可添加成员。");
        }

        if (existingCount + toAdd.Count > MaxMembersPerGroup)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(
                "too_many_members",
                $"群成员数不能超过 {MaxMembersPerGroup}。");
        }

        var candidates = toAdd.Where(id => id != actorUserId).ToArray();
        var insertedUserIds = await TryInsertMembersBatchAsync(
                connection,
                transaction,
                conversationId,
                candidates,
                ConversationMemberRole.Member,
                occurredAtMs,
                ct)
            .ConfigureAwait(false);

        if (insertedUserIds.Count == 0)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Ok(conversationId, title);
        }

        var allMembers = await ListMembersInTxAsync(connection, transaction, conversationId, ct)
            .ConfigureAwait(false);
        if (allMembers.Count > RealtimeWireLimits.MaxTargetUserIdsPerEvent)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(
                "too_many_members",
                $"群成员数超过单事件目标上限 {RealtimeWireLimits.MaxTargetUserIdsPerEvent}。");
        }

        var newlyAdded = insertedUserIds
            .Select(id => new ConversationMemberItem
            {
                UserId = id,
                Role = ConversationMemberRole.Member,
                JoinedAtMs = occurredAtMs
            })
            .ToList();

        var events = new List<RealtimeEvent>(2);
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var allTargets = allMembers.Select(m => m.UserId).ToArray();

        events.Add(CreateMembersAddedEvent(
            conversationId,
            title,
            newlyAdded,
            actorUserId,
            allTargets,
            occurredAtMs,
            actorSessionId,
            traceParent,
            traceState));

        // 新成员需要看到 ConversationCreated 以初始化会话列表。
        events.Add(CreateConversationCreatedAggregatedEvent(
            conversationId,
            title,
            actorUserId,
            newlyAdded.Select(m => m.UserId).ToArray(),
            occurredAtMs,
            actorSessionId,
            traceParent,
            traceState,
            causeToken: $"join-batch:{occurredAtMs}"));

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title, allMembers);
    }

    public async Task<GroupMutatePersistResult> RemoveMemberAsync(
        long actorUserId,
        string conversationId,
        long targetUserId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        if (targetUserId <= 0)
            return GroupMutatePersistResult.Fail("invalid_target", "目标用户编号无效。");
        if (targetUserId == actorUserId)
            return GroupMutatePersistResult.Fail("invalid_target", "不能移除自己，请使用退群。");

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var (title, actorRole, _) = await LoadGroupContextAsync(
                connection,
                transaction,
                conversationId,
                actorUserId,
                ct)
            .ConfigureAwait(false);
        if (title is null || actorRole is null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("not_found", "群不存在或当前用户不是成员。");
        }

        if (!CanManageMembers(actorRole.Value))
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("forbidden", "仅 Owner / Admin 可移除成员。");
        }

        var targetRole = await TryGetRoleAsync(connection, transaction, conversationId, targetUserId, ct)
            .ConfigureAwait(false);
        if (targetRole is null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("not_found", "目标用户不是群成员。");
        }

        if (targetRole == ConversationMemberRole.Owner)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("forbidden", "不能直接移除 Owner。");
        }

        if (actorRole == ConversationMemberRole.Admin
            && targetRole == ConversationMemberRole.Admin)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("forbidden", "Admin 不能移除其他 Admin。");
        }

        var notifyTargets = await ListMemberUserIdsInTxAsync(connection, transaction, conversationId, ct)
            .ConfigureAwait(false);
        if (notifyTargets.Count > RealtimeWireLimits.MaxTargetUserIdsPerEvent)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(
                "too_many_members",
                $"群成员数超过单事件目标上限 {RealtimeWireLimits.MaxTargetUserIdsPerEvent}。");
        }

        await using (var delete = new NpgsqlCommand(
                         $"""
                          DELETE FROM {_databaseSchema.ConversationMembersTableSql}
                          WHERE conversation_id = @conversation_id AND user_id = @user_id;
                          """,
                         connection,
                         transaction))
        {
            delete.Parameters.AddWithValue("conversation_id", conversationId);
            delete.Parameters.AddWithValue("user_id", targetUserId);
            await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var evt = CreateMemberRemovedAggregatedEvent(
            conversationId,
            targetUserId,
            actorUserId,
            notifyTargets.ToArray(),
            occurredAtMs,
            actorSessionId,
            traceParent,
            traceState);

        await InsertOutboxManyAsync(connection, transaction, [evt], ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title);
    }

    public async Task<GroupMutatePersistResult> LeaveAsync(
        long actorUserId,
        string conversationId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var (title, actorRole, _) = await LoadGroupContextAsync(
                connection,
                transaction,
                conversationId,
                actorUserId,
                ct)
            .ConfigureAwait(false);
        if (title is null || actorRole is null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("not_found", "群不存在或当前用户不是成员。");
        }

        if (actorRole == ConversationMemberRole.Owner)
        {
            var otherOwners = await CountOwnersExcludingAsync(
                    connection,
                    transaction,
                    conversationId,
                    actorUserId,
                    ct)
                .ConfigureAwait(false);
            if (otherOwners == 0)
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return GroupMutatePersistResult.Fail(
                    "owner_must_transfer",
                    "Owner 退群前须先转让所有权。");
            }
        }

        var notifyTargets = await ListMemberUserIdsInTxAsync(connection, transaction, conversationId, ct)
            .ConfigureAwait(false);
        if (notifyTargets.Count > RealtimeWireLimits.MaxTargetUserIdsPerEvent)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(
                "too_many_members",
                $"群成员数超过单事件目标上限 {RealtimeWireLimits.MaxTargetUserIdsPerEvent}。");
        }

        await using (var delete = new NpgsqlCommand(
                         $"""
                          DELETE FROM {_databaseSchema.ConversationMembersTableSql}
                          WHERE conversation_id = @conversation_id AND user_id = @user_id;
                          """,
                         connection,
                         transaction))
        {
            delete.Parameters.AddWithValue("conversation_id", conversationId);
            delete.Parameters.AddWithValue("user_id", actorUserId);
            await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var evt = CreateMemberLeftAggregatedEvent(
            conversationId,
            actorUserId,
            notifyTargets.ToArray(),
            occurredAtMs,
            actorSessionId,
            traceParent,
            traceState);

        await InsertOutboxManyAsync(connection, transaction, [evt], ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title);
    }

    public async Task<GroupMutatePersistResult> ChangeRoleAsync(
        long actorUserId,
        string conversationId,
        long targetUserId,
        ConversationMemberRole newRole,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        if (targetUserId <= 0)
            return GroupMutatePersistResult.Fail("invalid_target", "目标用户编号无效。");
        if (newRole is not (ConversationMemberRole.Owner
            or ConversationMemberRole.Admin
            or ConversationMemberRole.Member))
        {
            return GroupMutatePersistResult.Fail("invalid_role", "角色无效。");
        }

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var (title, actorRole, _) = await LoadGroupContextAsync(
                connection,
                transaction,
                conversationId,
                actorUserId,
                ct)
            .ConfigureAwait(false);
        if (title is null || actorRole is null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("not_found", "群不存在或当前用户不是成员。");
        }

        if (actorRole != ConversationMemberRole.Owner)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("forbidden", "仅 Owner 可变更角色或转让所有权。");
        }

        var previousRole = await TryGetRoleAsync(connection, transaction, conversationId, targetUserId, ct)
            .ConfigureAwait(false);
        if (previousRole is null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("not_found", "目标用户不是群成员。");
        }

        if (previousRole == newRole)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Ok(conversationId, title);
        }

        if (newRole == ConversationMemberRole.Owner)
        {
            if (targetUserId == actorUserId)
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
                return GroupMutatePersistResult.Fail("invalid_target", "不能转让给自己。");
            }

            await using (var demote = new NpgsqlCommand(
                             $"""
                              UPDATE {_databaseSchema.ConversationMembersTableSql}
                              SET role = @admin_role
                              WHERE conversation_id = @conversation_id AND user_id = @actor_user_id;
                              """,
                             connection,
                             transaction))
            {
                demote.Parameters.AddWithValue("admin_role", (short)ConversationMemberRole.Admin);
                demote.Parameters.AddWithValue("conversation_id", conversationId);
                demote.Parameters.AddWithValue("actor_user_id", actorUserId);
                await demote.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }
        else if (targetUserId == actorUserId)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(
                "forbidden",
                "Owner 不能直接降级自己；请先转让所有权。");
        }

        await using (var update = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.ConversationMembersTableSql}
                          SET role = @new_role
                          WHERE conversation_id = @conversation_id AND user_id = @user_id;
                          """,
                         connection,
                         transaction))
        {
            update.Parameters.AddWithValue("new_role", (short)newRole);
            update.Parameters.AddWithValue("conversation_id", conversationId);
            update.Parameters.AddWithValue("user_id", targetUserId);
            await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var notifyTargets = await ListMemberUserIdsInTxAsync(connection, transaction, conversationId, ct)
            .ConfigureAwait(false);
        if (notifyTargets.Count > RealtimeWireLimits.MaxTargetUserIdsPerEvent)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(
                "too_many_members",
                $"群成员数超过单事件目标上限 {RealtimeWireLimits.MaxTargetUserIdsPerEvent}。");
        }

        var events = new List<RealtimeEvent>(newRole == ConversationMemberRole.Owner ? 2 : 1);
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var targets = notifyTargets.ToArray();

        events.Add(CreateRoleChangedAggregatedEvent(
            conversationId,
            targetUserId,
            previousRole.Value,
            newRole,
            actorUserId,
            targets,
            occurredAtMs,
            actorSessionId,
            traceParent,
            traceState));

        if (newRole == ConversationMemberRole.Owner)
        {
            events.Add(CreateRoleChangedAggregatedEvent(
                conversationId,
                actorUserId,
                ConversationMemberRole.Owner,
                ConversationMemberRole.Admin,
                actorUserId,
                targets,
                occurredAtMs,
                actorSessionId,
                traceParent,
                traceState));
        }

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title);
    }

    public async Task<IReadOnlyList<ConversationMemberItem>> ListMembersAsync(
        long actorUserId,
        string conversationId,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        var isMember = await IsActiveMemberAsync(conversationId, actorUserId, ct).ConfigureAwait(false);
        if (!isMember)
            return Array.Empty<ConversationMemberItem>();

        return await ListMembersInTxAsync(connection, transaction: null, conversationId, ct)
            .ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<long>> ListActiveMemberUserIdsAsync(
        string conversationId,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        return await ListMemberUserIdsInTxAsync(connection, transaction: null, conversationId, ct)
            .ConfigureAwait(false);
    }

    public async Task<bool> IsActiveMemberAsync(
        string conversationId,
        long userId,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT 1
             FROM {_databaseSchema.ConversationMembersTableSql}
             WHERE conversation_id = @conversation_id AND user_id = @user_id
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);
        var scalar = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return scalar is not null;
    }

    private async Task<(string? Title, ConversationMemberRole? ActorRole, int MemberCount)> LoadGroupContextAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long actorUserId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT c.title, m.role,
                    (SELECT COUNT(*)::int FROM {_databaseSchema.ConversationMembersTableSql} AS x
                     WHERE x.conversation_id = c.conversation_id) AS member_count
             FROM {_databaseSchema.ConversationsTableSql} AS c
             INNER JOIN {_databaseSchema.ConversationMembersTableSql} AS m
                 ON m.conversation_id = c.conversation_id AND m.user_id = @actor_user_id
             WHERE c.conversation_id = @conversation_id
               AND c.type = @group_type
             FOR UPDATE OF m;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("actor_user_id", actorUserId);
        command.Parameters.AddWithValue("group_type", (short)ConversationType.Group);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return (null, null, 0);

        var title = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
        var role = (ConversationMemberRole)reader.GetInt16(1);
        var count = reader.GetInt32(2);
        return (title, role, count);
    }

    /// <summary>
    /// 使用 UNNEST 一次性写入全部成员（建群场景，已知全部新增）。
    /// </summary>
    private async Task InsertMembersBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long[] userIds,
        short[] roles,
        long joinedAtMs,
        CancellationToken ct)
    {
        if (userIds.Length == 0)
            return;

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.ConversationMembersTableSql} (
                 conversation_id, user_id, peer_user_id, joined_at_ms, role, last_message_at_ms
             )
             SELECT @conversation_id, t.user_id, NULL, @joined_at_ms, t.role, @joined_at_ms
             FROM UNNEST(@user_ids, @roles) AS t(user_id, role)
             ON CONFLICT (conversation_id, user_id) DO NOTHING;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("joined_at_ms", joinedAtMs);
        command.Parameters.Add(new NpgsqlParameter("user_ids", NpgsqlDbType.Bigint | NpgsqlDbType.Array)
        {
            Value = userIds
        });
        command.Parameters.Add(new NpgsqlParameter("roles", NpgsqlDbType.Smallint | NpgsqlDbType.Array)
        {
            Value = roles
        });
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 使用 UNNEST 批量尝试写入成员，返回实际新增的 user_id 列表（ON CONFLICT DO NOTHING + RETURNING）。
    /// </summary>
    private async Task<IReadOnlyList<long>> TryInsertMembersBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long[] userIds,
        ConversationMemberRole role,
        long joinedAtMs,
        CancellationToken ct)
    {
        if (userIds.Length == 0)
            return Array.Empty<long>();

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.ConversationMembersTableSql} (
                 conversation_id, user_id, peer_user_id, joined_at_ms, role, last_message_at_ms
             )
             SELECT @conversation_id, t.user_id, NULL, @joined_at_ms, @role, @joined_at_ms
             FROM UNNEST(@user_ids) AS t(user_id)
             ON CONFLICT (conversation_id, user_id) DO NOTHING
             RETURNING user_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("joined_at_ms", joinedAtMs);
        command.Parameters.AddWithValue("role", (short)role);
        command.Parameters.Add(new NpgsqlParameter("user_ids", NpgsqlDbType.Bigint | NpgsqlDbType.Array)
        {
            Value = userIds
        });

        var inserted = new List<long>(userIds.Length);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            inserted.Add(reader.GetInt64(0));
        return inserted;
    }

    private async Task<ConversationMemberRole?> TryGetRoleAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long userId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT role
             FROM {_databaseSchema.ConversationMembersTableSql}
             WHERE conversation_id = @conversation_id AND user_id = @user_id
             FOR UPDATE;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);
        var scalar = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return scalar is short role ? (ConversationMemberRole)role : null;
    }

    private async Task<int> CountOwnersExcludingAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long excludeUserId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM {_databaseSchema.ConversationMembersTableSql}
             WHERE conversation_id = @conversation_id
               AND role = @owner_role
               AND user_id <> @exclude_user_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("owner_role", (short)ConversationMemberRole.Owner);
        command.Parameters.AddWithValue("exclude_user_id", excludeUserId);
        var scalar = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return scalar is int count ? count : 0;
    }

    private async Task<IReadOnlyList<ConversationMemberItem>> ListMembersInTxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string conversationId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT user_id, role, joined_at_ms
             FROM {_databaseSchema.ConversationMembersTableSql}
             WHERE conversation_id = @conversation_id
             ORDER BY role ASC, joined_at_ms ASC, user_id ASC;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        var items = new List<ConversationMemberItem>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new ConversationMemberItem
            {
                UserId = reader.GetInt64(0),
                Role = (ConversationMemberRole)reader.GetInt16(1),
                JoinedAtMs = reader.GetInt64(2)
            });
        }

        return items;
    }

    private async Task<IReadOnlyList<long>> ListMemberUserIdsInTxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string conversationId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT user_id
             FROM {_databaseSchema.ConversationMembersTableSql}
             WHERE conversation_id = @conversation_id
             ORDER BY user_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        var ids = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            ids.Add(reader.GetInt64(0));
        return ids;
    }

    private static bool CanManageMembers(ConversationMemberRole role) =>
        role is ConversationMemberRole.Owner or ConversationMemberRole.Admin;

    private static List<long> NormalizeCreateMembers(long creatorUserId, IReadOnlyList<long> memberUserIds)
    {
        var set = new SortedSet<long> { creatorUserId };
        foreach (var id in NormalizeDistinctPositive(memberUserIds))
            set.Add(id);
        return set.ToList();
    }

    private static List<long> NormalizeDistinctPositive(IReadOnlyList<long>? userIds)
    {
        if (userIds is null || userIds.Count == 0)
            return [];
        var set = new SortedSet<long>();
        foreach (var id in userIds)
        {
            if (id > 0)
                set.Add(id);
        }

        return set.ToList();
    }

    private static RealtimeEvent CreateConversationCreatedAggregatedEvent(
        string conversationId,
        string title,
        long actorUserId,
        long[] targetUserIds,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState,
        string causeToken)
    {
        var payload = new RealtimeConversationChangedPayload
        {
            ConversationId = conversationId,
            Type = ConversationType.Group,
            PeerUserId = null,
            Title = title,
            LastMessageId = null,
            LastMessagePreview = null,
            LastMessageAtMs = null,
            LastSenderUserId = null
        };

        return new RealtimeEvent
        {
            EventId = ConversationEventIdFactory.CreateConversationCreatedAggregatedEventId(
                conversationId,
                causeToken,
                occurredAtMs),
            Type = RealtimeEventType.ConversationListChanged,
            TargetUserId = targetUserIds.Length > 0 ? targetUserIds[0] : actorUserId,
            ActorUserId = actorUserId,
            SessionId = actorSessionId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeConversationChangedPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState,
            TargetUserIds = targetUserIds
        };
    }

    private static RealtimeEvent CreateMembersAddedEvent(
        string conversationId,
        string title,
        IReadOnlyList<ConversationMemberItem> addedMembers,
        long actorUserId,
        long[] targetUserIds,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState)
    {
        var payload = new RealtimeMembersAddedPayload
        {
            ConversationId = conversationId,
            Members = addedMembers,
            ActorUserId = actorUserId,
            Title = title,
            OccurredAtMs = occurredAtMs
        };

        var addedUserIds = addedMembers.Select(m => m.UserId).ToArray();

        return new RealtimeEvent
        {
            EventId = GroupEventIdFactory.CreateMembersAddedEventId(
                conversationId,
                addedUserIds,
                occurredAtMs),
            Type = RealtimeEventType.MembersAdded,
            TargetUserId = targetUserIds.Length > 0 ? targetUserIds[0] : actorUserId,
            ActorUserId = actorUserId,
            SessionId = actorSessionId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeMembersAddedPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState,
            TargetUserIds = targetUserIds
        };
    }

    private static RealtimeEvent CreateMemberLeftAggregatedEvent(
        string conversationId,
        long leftUserId,
        long[] targetUserIds,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState) =>
        new()
        {
            EventId = GroupEventIdFactory.CreateMemberLeftAggregatedEventId(
                conversationId,
                leftUserId,
                occurredAtMs),
            Type = RealtimeEventType.MemberLeft,
            TargetUserId = targetUserIds.Length > 0 ? targetUserIds[0] : leftUserId,
            ActorUserId = leftUserId,
            SessionId = actorSessionId,
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeMemberLeftPayload
                {
                    ConversationId = conversationId,
                    UserId = leftUserId,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeMemberLeftPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState,
            TargetUserIds = targetUserIds
        };

    private static RealtimeEvent CreateMemberRemovedAggregatedEvent(
        string conversationId,
        long removedUserId,
        long actorUserId,
        long[] targetUserIds,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState) =>
        new()
        {
            EventId = GroupEventIdFactory.CreateMemberRemovedAggregatedEventId(
                conversationId,
                removedUserId,
                occurredAtMs),
            Type = RealtimeEventType.MemberRemoved,
            TargetUserId = targetUserIds.Length > 0 ? targetUserIds[0] : removedUserId,
            ActorUserId = actorUserId,
            SessionId = actorSessionId,
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeMemberRemovedPayload
                {
                    ConversationId = conversationId,
                    UserId = removedUserId,
                    ActorUserId = actorUserId,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeMemberRemovedPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState,
            TargetUserIds = targetUserIds
        };

    private static RealtimeEvent CreateRoleChangedAggregatedEvent(
        string conversationId,
        long userId,
        ConversationMemberRole previousRole,
        ConversationMemberRole newRole,
        long actorUserId,
        long[] targetUserIds,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState) =>
        new()
        {
            EventId = GroupEventIdFactory.CreateRoleChangedAggregatedEventId(
                conversationId,
                userId,
                newRole,
                occurredAtMs),
            Type = RealtimeEventType.RoleChanged,
            TargetUserId = targetUserIds.Length > 0 ? targetUserIds[0] : userId,
            ActorUserId = actorUserId,
            SessionId = actorSessionId,
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeRoleChangedPayload
                {
                    ConversationId = conversationId,
                    UserId = userId,
                    NewRole = newRole,
                    PreviousRole = previousRole,
                    ActorUserId = actorUserId,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeRoleChangedPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState,
            TargetUserIds = targetUserIds
        };

    private async Task InsertOutboxManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<RealtimeEvent> events,
        CancellationToken ct)
    {
        if (events.Count == 0)
            return;

        if (events.Count > RealtimeWireLimits.MaxEventsPerTransaction)
        {
            throw new InvalidOperationException(
                $"单事务 Outbox 事件数 {events.Count} 超过上限 {RealtimeWireLimits.MaxEventsPerTransaction}。");
        }

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        // 聚合后事件数大幅下降，可使用更大 chunk 减少往返。
        const int chunkSize = 100;
        for (var offset = 0; offset < events.Count; offset += chunkSize)
        {
            var count = Math.Min(chunkSize, events.Count - offset);
            await using var command = new NpgsqlCommand
            {
                Connection = connection,
                Transaction = transaction
            };
            var values = new List<string>(count);
            for (var i = 0; i < count; i++)
            {
                var evt = events[offset + i];
                var payloadJson = JsonSerializer.Serialize(
                    evt,
                    RealtimeJsonSerializerContext.Default.RealtimeEvent);

                if (payloadJson.Length > RealtimeWireLimits.MaxOutboxPayloadBytes)
                {
                    throw new InvalidOperationException(
                        $"Outbox payload 字节数 {payloadJson.Length} 超过上限 {RealtimeWireLimits.MaxOutboxPayloadBytes}；" +
                        $"事件 {evt.EventId} 无法写入。");
                }

                var hasTargetUserIds = evt.TargetUserIds is { Length: > 0 };
                values.Add(
                    $"(@event_id_{i}, @payload_json_{i}, @target_user_id_{i}, @event_type_{i}, @status, @created_at_ms, @next_attempt_at_ms, 0, @target_user_ids_{i})");
                command.Parameters.AddWithValue($"event_id_{i}", evt.EventId);
                command.Parameters.AddWithValue($"payload_json_{i}", payloadJson);
                command.Parameters.AddWithValue($"target_user_id_{i}", evt.TargetUserId);
                command.Parameters.AddWithValue($"event_type_{i}", (short)evt.Type);
                var targetIdsParam = new NpgsqlParameter(
                    $"target_user_ids_{i}",
                    NpgsqlDbType.Bigint | NpgsqlDbType.Array)
                {
                    Value = hasTargetUserIds ? (object)evt.TargetUserIds! : DBNull.Value
                };
                command.Parameters.Add(targetIdsParam);
            }

            command.Parameters.AddWithValue("status", (short)RealtimeOutboxStatus.Pending);
            command.Parameters.AddWithValue("created_at_ms", now);
            command.Parameters.AddWithValue("next_attempt_at_ms", now);
            command.CommandText =
                $"""
                 INSERT INTO {_databaseSchema.OutboxTableSql} (
                     event_id, payload_json, target_user_id, event_type, status,
                     created_at_ms, next_attempt_at_ms, attempt_count, target_user_ids
                 ) VALUES {string.Join(", ", values)}
                 ON CONFLICT (event_id) DO NOTHING;
                 """;
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }
}
