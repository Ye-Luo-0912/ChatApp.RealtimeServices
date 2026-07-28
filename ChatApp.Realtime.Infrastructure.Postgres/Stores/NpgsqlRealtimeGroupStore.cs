using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Protocol;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeGroupStore : IRealtimeGroupStore
{
    public const int MaxMembersPerGroup = 200;
    public const int MaxAddMembersPerRequest = 50;

    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly IGroupOperationAuditStore? _auditStore;
    private readonly IMembershipPeriodStore? _membershipPeriodStore;

    public NpgsqlRealtimeGroupStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        IGroupOperationAuditStore? auditStore = null,
        IMembershipPeriodStore? membershipPeriodStore = null)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        // 审计 Outbox：可选注入。生产 DI 路径由 RealtimePostgresRegistration 注入
        // NpgsqlGroupOperationAuditStore；未注入时（测试场景）跳过事务内审计。
        _auditStore = auditStore;
        // Membership periods：可选注入。生产 DI 路径由 RealtimePostgresRegistration 注入
        // NpgsqlMembershipPeriodStore；未注入时（测试场景）跳过 membership period 记录。
        _membershipPeriodStore = membershipPeriodStore;
    }

    /// <summary>
    /// P0-2：在事务内获取 actor 的共享 advisory lock 并检查生命周期状态。
    /// 返回 null 表示通过（用户活跃），返回非空 string 表示错误码。
    /// </summary>
    private async Task<string?> CheckActorLifecycleInTxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long actorUserId,
        CancellationToken ct)
    {
        if (!await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveAsync(
                connection, transaction, _databaseSchema, actorUserId, ct)
            .ConfigureAwait(false))
            return "user_deleted";
        return null;
    }

    /// <summary>
    /// Membership periods：在业务事务内批量记录多个成员入群。
    /// 未注入 <see cref="IMembershipPeriodStore"/> 时（测试场景）为空操作。
    /// 使用 UNNEST 单条 SQL，避免逐成员往返。
    /// </summary>
    private async Task RecordMembershipJoinsInTransactionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        IReadOnlyList<long> userIds,
        long joinedAtMs,
        CancellationToken ct)
    {
        if (_membershipPeriodStore is null || userIds.Count == 0)
            return;

        await _membershipPeriodStore
            .RecordJoinsBatchInTransactionAsync(
                connection, transaction, conversationId, joinedAtMs, userIds, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Membership periods：在业务事务内记录单个成员离群。
    /// 未注入 <see cref="IMembershipPeriodStore"/> 时（测试场景）为空操作。
    /// </summary>
    private async Task RecordMembershipLeaveInTransactionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long userId,
        long leftAtMs,
        string leftReason,
        CancellationToken ct)
    {
        if (_membershipPeriodStore is null)
            return;

        await _membershipPeriodStore
            .RecordLeaveInTransactionAsync(
                connection, transaction, conversationId, userId, leftAtMs, leftReason, ct)
            .ConfigureAwait(false);
    }

    public async Task<GroupCreatePersistResult> CreateGroupAsync(
        string requestId,
        long creatorUserId,
        string conversationId,
        string title,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        var fingerprint = ComputeGroupFingerprint(
            1,
            $"{title}\n{string.Join(',', memberUserIds)}");

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

        var existing = await TryReadGroupMutationRequestAsync(
                connection,
                transaction,
                creatorUserId,
                requestId,
                ct)
            .ConfigureAwait(false);
        if (existing is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            if (existing.Fingerprint != fingerprint)
                return GroupCreatePersistResult.Fail(
                    "request_conflict",
                    "请求编号已用于不同操作。");
            return GroupCreatePersistResult.Ok(existing.ConversationId!, null, null);
        }

        // P0-2 / P0-4：事务内一次性获取 actor + 全部目标用户的共享 advisory lock 并检查生命周期状态。
        // 按 userId 升序获取锁，避免死锁。防止已注销用户建群或被加入群，同时消除目标用户
        // 在检查后开始删除的 TOCTOU 竞态。
        // TODO: 用户存在性验证需要 users 表（当前 realtime schema 无独立 users 表，仅依赖 tombstone 检查删除状态）。
        var createLifecycleIds = new List<long>(members.Count + 1) { creatorUserId };
        createLifecycleIds.AddRange(members);
        if (!await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveManyAsync(
                connection, transaction, _databaseSchema, createLifecycleIds, ct).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupCreatePersistResult.Fail("user_deleted", "用户已注销，操作被拒绝。");
        }

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

        // Membership periods：记录所有初始成员入群（与群创建同事务）。
        await RecordMembershipJoinsInTransactionAsync(
                connection, transaction, conversationId, members, occurredAtMs, ct)
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
        await InsertGroupMutationRequestAsync(
                connection,
                transaction,
                creatorUserId,
                requestId,
                operation: 1,
                fingerprint,
                conversationId,
                succeeded: true,
                errorCode: null,
                ct)
            .ConfigureAwait(false);
        // 审计 Outbox：在业务事务内写入审计，失败则整个事务回滚。
        await RecordAuditInTransactionAsync(
                connection, transaction,
                GroupConversationOperation.Create, creatorUserId, conversationId,
                targetUserId: null, previousRole: null, newRole: null,
                requestId, actorSessionId, succeeded: true, errorCode: null, occurredAtMs, ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupCreatePersistResult.Ok(conversationId, title, memberItems);
    }

    public async Task<GroupMutatePersistResult> AddMembersAsync(
        string requestId,
        long actorUserId,
        string conversationId,
        IReadOnlyList<long> memberUserIds,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        var fingerprint = ComputeGroupFingerprint(
            2,
            $"{conversationId}\n{string.Join(',', memberUserIds)}");

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

        var existing = await TryReadGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                ct)
            .ConfigureAwait(false);
        if (existing is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            if (existing.Fingerprint != fingerprint)
                return GroupMutatePersistResult.Fail(
                    "request_conflict",
                    "请求编号已用于不同操作。");
            return GroupMutatePersistResult.Ok(existing.ConversationId!, null, null);
        }

        // P0-2 / P0-4：事务内一次性获取 actor + 全部目标用户的共享 advisory lock 并检查生命周期状态。
        // 按 userId 升序获取锁，避免死锁。消除目标用户在检查后开始删除的 TOCTOU 竞态。
        // TODO: 用户存在性验证需要 users 表（当前 realtime schema 无独立 users 表，仅依赖 tombstone 检查删除状态）。
        var addLifecycleIds = new List<long>(toAdd.Count + 1) { actorUserId };
        addLifecycleIds.AddRange(toAdd);
        if (!await UserLifecycleAdvisoryLock.AcquireSharedAndCheckActiveManyAsync(
                connection, transaction, _databaseSchema, addLifecycleIds, ct).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail("user_deleted", "用户已注销，操作被拒绝。");
        }

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

        // Membership periods：记录新入群成员（与加成员同事务）。
        await RecordMembershipJoinsInTransactionAsync(
                connection, transaction, conversationId, insertedUserIds, occurredAtMs, ct)
            .ConfigureAwait(false);

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
        await InsertGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                operation: 2,
                fingerprint,
                conversationId,
                succeeded: true,
                errorCode: null,
                ct)
            .ConfigureAwait(false);
        // 审计 Outbox：在业务事务内写入审计，失败则整个事务回滚。
        await RecordAuditInTransactionAsync(
                connection, transaction,
                GroupConversationOperation.AddMembers, actorUserId, conversationId,
                targetUserId: null, previousRole: null, newRole: null,
                requestId, actorSessionId, succeeded: true, errorCode: null, occurredAtMs, ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title, allMembers);
    }

    public async Task<GroupMutatePersistResult> RemoveMemberAsync(
        string requestId,
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

        var fingerprint = ComputeGroupFingerprint(3, $"{conversationId}\n{targetUserId}");

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        // P0-2：事务内检查 actor 生命周期。
        var removeActorError = await CheckActorLifecycleInTxAsync(
            connection, transaction, actorUserId, ct).ConfigureAwait(false);
        if (removeActorError is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(removeActorError, "用户已注销，操作被拒绝。");
        }

        var existing = await TryReadGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                ct)
            .ConfigureAwait(false);
        if (existing is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            if (existing.Fingerprint != fingerprint)
                return GroupMutatePersistResult.Fail(
                    "request_conflict",
                    "请求编号已用于不同操作。");
            return GroupMutatePersistResult.Ok(existing.ConversationId!, null, null);
        }

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

        await using (var softDelete = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.ConversationMembersTableSql} AS m
                          SET left_at_ms = @occurred_at_ms,
                              left_sequence = c.last_sequence,
                              left_message_id = c.last_message_id,
                              left_message_preview = c.last_message_preview,
                              left_message_at_ms = c.last_message_at_ms,
                              left_sender_user_id = c.last_sender_user_id,
                              sent_count_at_leave = m.sent_count
                          FROM {_databaseSchema.ConversationsTableSql} AS c
                          WHERE m.conversation_id = @conversation_id
                            AND m.user_id = @user_id
                            AND m.left_at_ms IS NULL
                            AND c.conversation_id = @conversation_id;
                          """,
                         connection,
                         transaction))
        {
            softDelete.Parameters.AddWithValue("conversation_id", conversationId);
            softDelete.Parameters.AddWithValue("user_id", targetUserId);
            softDelete.Parameters.AddWithValue("occurred_at_ms", occurredAtMs);
            await softDelete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // Membership periods：记录被移除成员离群（与移除操作同事务）。
        await RecordMembershipLeaveInTransactionAsync(
                connection, transaction, conversationId, targetUserId,
                occurredAtMs, leftReason: "removed", ct)
            .ConfigureAwait(false);

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
        await InsertGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                operation: 3,
                fingerprint,
                conversationId,
                succeeded: true,
                errorCode: null,
                ct)
            .ConfigureAwait(false);
        // 审计 Outbox：在业务事务内写入审计，失败则整个事务回滚。
        await RecordAuditInTransactionAsync(
                connection, transaction,
                GroupConversationOperation.RemoveMember, actorUserId, conversationId,
                targetUserId, previousRole: null, newRole: null,
                requestId, actorSessionId, succeeded: true, errorCode: null, occurredAtMs, ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title);
    }

    public async Task<GroupMutatePersistResult> LeaveAsync(
        string requestId,
        long actorUserId,
        string conversationId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        var fingerprint = ComputeGroupFingerprint(4, $"{conversationId}");

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        // P0-2：事务内检查 actor 生命周期。
        var leaveActorError = await CheckActorLifecycleInTxAsync(
            connection, transaction, actorUserId, ct).ConfigureAwait(false);
        if (leaveActorError is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(leaveActorError, "用户已注销，操作被拒绝。");
        }

        var existing = await TryReadGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                ct)
            .ConfigureAwait(false);
        if (existing is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            if (existing.Fingerprint != fingerprint)
                return GroupMutatePersistResult.Fail(
                    "request_conflict",
                    "请求编号已用于不同操作。");
            return GroupMutatePersistResult.Ok(existing.ConversationId!, null, null);
        }

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

        await using (var softDelete = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.ConversationMembersTableSql} AS m
                          SET left_at_ms = @occurred_at_ms,
                              left_sequence = c.last_sequence,
                              left_message_id = c.last_message_id,
                              left_message_preview = c.last_message_preview,
                              left_message_at_ms = c.last_message_at_ms,
                              left_sender_user_id = c.last_sender_user_id,
                              sent_count_at_leave = m.sent_count
                          FROM {_databaseSchema.ConversationsTableSql} AS c
                          WHERE m.conversation_id = @conversation_id
                            AND m.user_id = @user_id
                            AND m.left_at_ms IS NULL
                            AND c.conversation_id = @conversation_id;
                          """,
                         connection,
                         transaction))
        {
            softDelete.Parameters.AddWithValue("conversation_id", conversationId);
            softDelete.Parameters.AddWithValue("user_id", actorUserId);
            softDelete.Parameters.AddWithValue("occurred_at_ms", occurredAtMs);
            await softDelete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // Membership periods：记录主动退群成员离群（与退群操作同事务）。
        await RecordMembershipLeaveInTransactionAsync(
                connection, transaction, conversationId, actorUserId,
                occurredAtMs, leftReason: "leave", ct)
            .ConfigureAwait(false);

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
        await InsertGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                operation: 4,
                fingerprint,
                conversationId,
                succeeded: true,
                errorCode: null,
                ct)
            .ConfigureAwait(false);
        // 审计 Outbox：在业务事务内写入审计，失败则整个事务回滚。
        await RecordAuditInTransactionAsync(
                connection, transaction,
                GroupConversationOperation.Leave, actorUserId, conversationId,
                targetUserId: null, previousRole: null, newRole: null,
                requestId, actorSessionId, succeeded: true, errorCode: null, occurredAtMs, ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title);
    }

    public async Task<GroupMutatePersistResult> ChangeRoleAsync(
        string requestId,
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

        var fingerprint = ComputeGroupFingerprint(
            5,
            $"{conversationId}\n{targetUserId}\n{(short)newRole}");

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        // P0-2：事务内检查 actor 生命周期。
        var changeRoleActorError = await CheckActorLifecycleInTxAsync(
            connection, transaction, actorUserId, ct).ConfigureAwait(false);
        if (changeRoleActorError is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(changeRoleActorError, "用户已注销，操作被拒绝。");
        }

        var existing = await TryReadGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                ct)
            .ConfigureAwait(false);
        if (existing is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            if (existing.Fingerprint != fingerprint)
                return GroupMutatePersistResult.Fail(
                    "request_conflict",
                    "请求编号已用于不同操作。");
            return GroupMutatePersistResult.Ok(existing.ConversationId!, null, null);
        }

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
            await InsertGroupMutationRequestAsync(
                    connection,
                    transaction,
                    actorUserId,
                    requestId,
                    operation: 5,
                    fingerprint,
                    conversationId,
                    succeeded: true,
                    errorCode: null,
                    ct)
                .ConfigureAwait(false);
            // 审计 Outbox：角色未变（幂等 no-op）但仍记录审计，失败则整个事务回滚。
            await RecordAuditInTransactionAsync(
                    connection, transaction,
                    GroupConversationOperation.ChangeRole, actorUserId, conversationId,
                    targetUserId, previousRole, newRole,
                    requestId, actorSessionId, succeeded: true, errorCode: null, occurredAtMs, ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Ok(conversationId, title)
                with { PreviousRole = previousRole, NewRole = newRole };
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
        await InsertGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                operation: 5,
                fingerprint,
                conversationId,
                succeeded: true,
                errorCode: null,
                ct)
            .ConfigureAwait(false);
        // 审计 Outbox：在业务事务内写入审计，失败则整个事务回滚。
        await RecordAuditInTransactionAsync(
                connection, transaction,
                GroupConversationOperation.ChangeRole, actorUserId, conversationId,
                targetUserId, previousRole, newRole,
                requestId, actorSessionId, succeeded: true, errorCode: null, occurredAtMs, ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title)
            with { PreviousRole = previousRole, NewRole = newRole };
    }

    public async Task<GroupMutatePersistResult> DissolveAsync(
        string requestId,
        long actorUserId,
        string conversationId,
        string? actorSessionId,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        var fingerprint = ComputeGroupFingerprint(7, $"{conversationId}");

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        // P0-2：事务内检查 actor 生命周期。
        var dissolveActorError = await CheckActorLifecycleInTxAsync(
            connection, transaction, actorUserId, ct).ConfigureAwait(false);
        if (dissolveActorError is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Fail(dissolveActorError, "用户已注销，操作被拒绝。");
        }

        var existing = await TryReadGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                ct)
            .ConfigureAwait(false);
        if (existing is not null)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            if (existing.Fingerprint != fingerprint)
                return GroupMutatePersistResult.Fail(
                    "request_conflict",
                    "请求编号已用于不同操作。");
            return GroupMutatePersistResult.Ok(existing.ConversationId!, null, null);
        }

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
            return GroupMutatePersistResult.Fail("forbidden", "仅 Owner 可解散群。");
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

        await using (var dissolveMembers = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.ConversationMembersTableSql} AS m
                          SET left_at_ms = @occurred_at_ms,
                              left_sequence = c.last_sequence,
                              left_message_id = c.last_message_id,
                              left_message_preview = c.last_message_preview,
                              left_message_at_ms = c.last_message_at_ms,
                              left_sender_user_id = c.last_sender_user_id,
                              sent_count_at_leave = m.sent_count
                          FROM {_databaseSchema.ConversationsTableSql} AS c
                          WHERE m.conversation_id = @conversation_id
                            AND m.left_at_ms IS NULL
                            AND c.conversation_id = @conversation_id;
                          """,
                         connection,
                         transaction))
        {
            dissolveMembers.Parameters.AddWithValue("conversation_id", conversationId);
            dissolveMembers.Parameters.AddWithValue("occurred_at_ms", occurredAtMs);
            await dissolveMembers.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // Membership periods：记录全员离群（与解散操作同事务）。
        foreach (var memberId in notifyTargets)
        {
            await RecordMembershipLeaveInTransactionAsync(
                    connection, transaction, conversationId, memberId,
                    occurredAtMs, leftReason: "dissolved", ct)
                .ConfigureAwait(false);
        }

        await using (var dissolveConv = new NpgsqlCommand(
                         $"""
                          UPDATE {_databaseSchema.ConversationsTableSql}
                          SET dissolved_at_ms = @occurred_at_ms
                          WHERE conversation_id = @conversation_id AND dissolved_at_ms IS NULL;
                          """,
                         connection,
                         transaction))
        {
            dissolveConv.Parameters.AddWithValue("conversation_id", conversationId);
            dissolveConv.Parameters.AddWithValue("occurred_at_ms", occurredAtMs);
            await dissolveConv.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var evt = CreateConversationCreatedAggregatedEvent(
            conversationId,
            title,
            actorUserId,
            notifyTargets.ToArray(),
            occurredAtMs,
            actorSessionId,
            traceParent,
            traceState,
            causeToken: $"group-dissolved:{occurredAtMs}");

        await InsertOutboxManyAsync(connection, transaction, [evt], ct).ConfigureAwait(false);
        await InsertGroupMutationRequestAsync(
                connection,
                transaction,
                actorUserId,
                requestId,
                operation: 7,
                fingerprint,
                conversationId,
                succeeded: true,
                errorCode: null,
                ct)
            .ConfigureAwait(false);
        // 审计 Outbox：在业务事务内写入审计，失败则整个事务回滚。
        await RecordAuditInTransactionAsync(
                connection, transaction,
                GroupConversationOperation.Dissolve, actorUserId, conversationId,
                targetUserId: null, previousRole: null, newRole: null,
                requestId, actorSessionId, succeeded: true, errorCode: null, occurredAtMs, ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return GroupMutatePersistResult.Ok(conversationId, title);
    }

    public async Task<IReadOnlyList<ConversationMemberItem>> ListMembersAsync(
        long actorUserId,
        string conversationId,
        CancellationToken ct = default)
    {
        // LongTerm-2：合并成员资格校验与成员读取为单条 SQL，避免打开两次连接，
        // 同时消除“校验通过 / 读取之前”的 TOCTOU 窗口。EXISTS 子查询保证 actor 仍是当前成员。
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT members.user_id, members.role, members.joined_at_ms
             FROM {_databaseSchema.ConversationMembersTableSql} AS members
             WHERE members.conversation_id = @conversation_id
               AND members.left_at_ms IS NULL
               AND EXISTS (
                   SELECT 1
                   FROM {_databaseSchema.ConversationMembersTableSql} AS actor
                   WHERE actor.conversation_id = @conversation_id
                     AND actor.user_id = @actor_user_id
                     AND actor.left_at_ms IS NULL
               )
             ORDER BY members.role ASC, members.joined_at_ms ASC, members.user_id ASC;
             """,
            connection);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("actor_user_id", actorUserId);

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
             WHERE conversation_id = @conversation_id
               AND user_id = @user_id
               AND left_at_ms IS NULL
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
                     WHERE x.conversation_id = c.conversation_id AND x.left_at_ms IS NULL) AS member_count
             FROM {_databaseSchema.ConversationsTableSql} AS c
             INNER JOIN {_databaseSchema.ConversationMembersTableSql} AS m
                 ON m.conversation_id = c.conversation_id AND m.user_id = @actor_user_id
                   AND m.left_at_ms IS NULL
             WHERE c.conversation_id = @conversation_id
               AND c.type = @group_type
               AND c.dissolved_at_ms IS NULL
             -- P0-7：同时锁 conversations 与操作者成员行，使所有群写操作在群级别串行化，
             -- 避免不同管理员并发读取相同 member_count 后突破群人数上限。
             FOR UPDATE OF c, m;
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
                 conversation_id, user_id, peer_user_id, joined_at_ms, role, last_message_at_ms,
                 last_read_sequence, sent_count, sent_count_at_read, unread_count
             )
             SELECT @conversation_id, t.user_id, NULL, @joined_at_ms, @role, @joined_at_ms,
                    c.last_sequence, 0, 0, 0
             FROM UNNEST(@user_ids) AS t(user_id)
             CROSS JOIN {_databaseSchema.ConversationsTableSql} AS c
             WHERE c.conversation_id = @conversation_id
             ON CONFLICT (conversation_id, user_id) DO UPDATE
                 SET role = EXCLUDED.role,
                     joined_at_ms = EXCLUDED.joined_at_ms,
                     left_at_ms = NULL,
                     last_message_at_ms = EXCLUDED.last_message_at_ms,
                     last_read_sequence = EXCLUDED.last_read_sequence,
                     sent_count = 0,
                     sent_count_at_read = 0,
                     unread_count = 0,
                     left_sequence = NULL,
                     left_message_id = NULL,
                     left_message_preview = NULL,
                     left_message_at_ms = NULL,
                     left_sender_user_id = NULL,
                     sent_count_at_leave = NULL
             WHERE conversation_members.left_at_ms IS NOT NULL
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
               AND left_at_ms IS NULL
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
               AND user_id <> @exclude_user_id
               AND left_at_ms IS NULL;
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
               AND left_at_ms IS NULL
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
               AND left_at_ms IS NULL
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

    private async Task<GroupMutationRequestRow?> TryReadGroupMutationRequestAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long actorUserId,
        string requestId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT operation, request_fingerprint, conversation_id, succeeded, error_code
             FROM {_databaseSchema.GroupMutationRequestsTableSql}
             WHERE actor_user_id = @actor_user_id
               AND request_id = @request_id
             FOR UPDATE
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("actor_user_id", actorUserId);
        command.Parameters.AddWithValue("request_id", requestId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return new GroupMutationRequestRow(
            reader.GetInt16(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.GetBoolean(3),
            reader.IsDBNull(4) ? null : reader.GetString(4));
    }

    private async Task InsertGroupMutationRequestAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long actorUserId,
        string requestId,
        short operation,
        string fingerprint,
        string? conversationId,
        bool succeeded,
        string? errorCode,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.GroupMutationRequestsTableSql} (
                 actor_user_id, request_id, operation, request_fingerprint,
                 conversation_id, succeeded, error_code, created_at_ms
             ) VALUES (
                 @actor_user_id, @request_id, @operation, @request_fingerprint,
                 @conversation_id, @succeeded, @error_code, @created_at_ms
             )
             ON CONFLICT (actor_user_id, request_id) DO NOTHING
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("actor_user_id", actorUserId);
        command.Parameters.AddWithValue("request_id", requestId);
        command.Parameters.AddWithValue("operation", operation);
        command.Parameters.AddWithValue("request_fingerprint", fingerprint);
        command.Parameters.AddWithValue("conversation_id", (object?)conversationId ?? DBNull.Value);
        command.Parameters.AddWithValue("succeeded", succeeded);
        command.Parameters.AddWithValue("error_code", (object?)errorCode ?? DBNull.Value);
        command.Parameters.AddWithValue("created_at_ms", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static string ComputeGroupFingerprint(short operation, string keyData) =>
        Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes($"{operation}\n{keyData}")));

    /// <summary>
    /// 审计 Outbox：在业务事务内写入群操作审计记录。
    /// <para>
    /// 复用调用方已有的连接与事务，与业务变更 / outbox / 幂等账本同生共死。
    /// 审计失败向上抛出，由 <c>await using</c> 释放事务触发回滚，
    /// 保证“业务变更成功 ⇒ 审计已记录”，审计不会静默丢失。
    /// </para>
    /// <para>
    /// 仅在成功路径调用：失败路径已回滚事务，审计改由 Processor 走 best-effort
    /// <see cref="IGroupOperationAuditStore.RecordAsync"/> 记录失败尝试。
    /// </para>
    /// </summary>
    private async Task RecordAuditInTransactionAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        GroupConversationOperation operation,
        long actorUserId,
        string? conversationId,
        long? targetUserId,
        ConversationMemberRole? previousRole,
        ConversationMemberRole? newRole,
        string requestId,
        string? actorSessionId,
        bool succeeded,
        string? errorCode,
        long occurredAtMs,
        CancellationToken ct)
    {
        if (_auditStore is null)
            return;

        var entry = new GroupOperationAuditEntry
        {
            ActorUserId = actorUserId,
            ConversationId = conversationId,
            Operation = operation,
            TargetUserId = targetUserId,
            PreviousRole = previousRole,
            NewRole = newRole,
            RequestId = requestId,
            ActorSessionId = actorSessionId,
            Succeeded = succeeded,
            ErrorCode = errorCode,
            OccurredAtMs = occurredAtMs
        };

        await _auditStore
            .RecordInTransactionAsync(entry, connection, transaction, ct)
            .ConfigureAwait(false);
    }

    private sealed record GroupMutationRequestRow(
        short Operation,
        string Fingerprint,
        string? ConversationId,
        bool Succeeded,
        string? ErrorCode);
}
