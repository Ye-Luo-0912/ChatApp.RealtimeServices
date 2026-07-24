using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

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
        foreach (var userId in members)
        {
            var role = userId == creatorUserId
                ? ConversationMemberRole.Owner
                : ConversationMemberRole.Member;
            await InsertMemberAsync(
                    connection,
                    transaction,
                    conversationId,
                    userId,
                    role,
                    occurredAtMs,
                    ct)
                .ConfigureAwait(false);
            memberItems.Add(new ConversationMemberItem
            {
                UserId = userId,
                Role = role,
                JoinedAtMs = occurredAtMs
            });
        }

        var events = new List<RealtimeEvent>(memberItems.Count * 2);
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();

        foreach (var target in memberItems)
        {
            events.Add(CreateConversationCreatedEvent(
                conversationId,
                title,
                target.UserId,
                creatorUserId,
                occurredAtMs,
                actorSessionId,
                traceParent,
                traceState));
        }

        foreach (var joined in memberItems.Where(m => m.UserId != creatorUserId))
        {
            foreach (var target in memberItems)
            {
                events.Add(CreateMemberJoinedEvent(
                    conversationId,
                    title,
                    joined.UserId,
                    joined.Role,
                    creatorUserId,
                    target.UserId,
                    occurredAtMs,
                    actorSessionId,
                    traceParent,
                    traceState));
            }
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

        var newlyAdded = new List<ConversationMemberItem>();
        foreach (var userId in toAdd)
        {
            if (userId == actorUserId)
                continue;
            var inserted = await TryInsertMemberAsync(
                    connection,
                    transaction,
                    conversationId,
                    userId,
                    ConversationMemberRole.Member,
                    occurredAtMs,
                    ct)
                .ConfigureAwait(false);
            if (inserted)
            {
                newlyAdded.Add(new ConversationMemberItem
                {
                    UserId = userId,
                    Role = ConversationMemberRole.Member,
                    JoinedAtMs = occurredAtMs
                });
            }
        }

        if (newlyAdded.Count == 0)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            return GroupMutatePersistResult.Ok(conversationId, title);
        }

        var allMembers = await ListMembersInTxAsync(connection, transaction, conversationId, ct)
            .ConfigureAwait(false);
        var events = new List<RealtimeEvent>(newlyAdded.Count * allMembers.Count + allMembers.Count);
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();

        foreach (var joined in newlyAdded)
        {
            foreach (var target in allMembers)
            {
                events.Add(CreateMemberJoinedEvent(
                    conversationId,
                    title,
                    joined.UserId,
                    joined.Role,
                    actorUserId,
                    target.UserId,
                    occurredAtMs,
                    actorSessionId,
                    traceParent,
                    traceState));
            }

            events.Add(CreateConversationCreatedEvent(
                conversationId,
                title,
                joined.UserId,
                actorUserId,
                occurredAtMs,
                actorSessionId,
                traceParent,
                traceState,
                causeToken: $"join:{joined.UserId}:{occurredAtMs}"));
        }

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

        var events = new List<RealtimeEvent>(notifyTargets.Count);
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        foreach (var target in notifyTargets)
        {
            events.Add(CreateMemberRemovedEvent(
                conversationId,
                targetUserId,
                actorUserId,
                target,
                occurredAtMs,
                actorSessionId,
                traceParent,
                traceState));
        }

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
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

        var events = new List<RealtimeEvent>(notifyTargets.Count);
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        foreach (var target in notifyTargets)
        {
            events.Add(CreateMemberLeftEvent(
                conversationId,
                actorUserId,
                target,
                occurredAtMs,
                actorSessionId,
                traceParent,
                traceState));
        }

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
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
        var events = new List<RealtimeEvent>(notifyTargets.Count * (newRole == ConversationMemberRole.Owner ? 2 : 1));
        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();

        foreach (var target in notifyTargets)
        {
            events.Add(CreateRoleChangedEvent(
                conversationId,
                targetUserId,
                previousRole.Value,
                newRole,
                actorUserId,
                target,
                occurredAtMs,
                actorSessionId,
                traceParent,
                traceState));

            if (newRole == ConversationMemberRole.Owner)
            {
                events.Add(CreateRoleChangedEvent(
                    conversationId,
                    actorUserId,
                    ConversationMemberRole.Owner,
                    ConversationMemberRole.Admin,
                    actorUserId,
                    target,
                    occurredAtMs,
                    actorSessionId,
                    traceParent,
                    traceState));
            }
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

    private async Task InsertMemberAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long userId,
        ConversationMemberRole role,
        long joinedAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.ConversationMembersTableSql} (
                 conversation_id, user_id, peer_user_id, joined_at_ms, role, last_message_at_ms
             ) VALUES (
                 @conversation_id, @user_id, NULL, @joined_at_ms, @role, @joined_at_ms
             );
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("joined_at_ms", joinedAtMs);
        command.Parameters.AddWithValue("role", (short)role);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task<bool> TryInsertMemberAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string conversationId,
        long userId,
        ConversationMemberRole role,
        long joinedAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.ConversationMembersTableSql} (
                 conversation_id, user_id, peer_user_id, joined_at_ms, role, last_message_at_ms
             ) VALUES (
                 @conversation_id, @user_id, NULL, @joined_at_ms, @role, @joined_at_ms
             )
             ON CONFLICT (conversation_id, user_id) DO NOTHING
             RETURNING user_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("joined_at_ms", joinedAtMs);
        command.Parameters.AddWithValue("role", (short)role);
        var scalar = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return scalar is not null;
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

    private static RealtimeEvent CreateConversationCreatedEvent(
        string conversationId,
        string title,
        long targetUserId,
        long actorUserId,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState,
        string? causeToken = null)
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
            EventId = RealtimeEventContracts.CreateConversationChangedEventId(
                conversationId,
                lastMessageId: causeToken ?? $"group-created:{occurredAtMs}",
                targetUserId,
                causeToken ?? "group-created"),
            Type = RealtimeEventType.ConversationListChanged,
            TargetUserId = targetUserId,
            ActorUserId = actorUserId,
            SessionId = actorSessionId,
            PayloadJson = JsonSerializer.Serialize(
                payload,
                RealtimeJsonSerializerContext.Default.RealtimeConversationChangedPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };
    }

    private static RealtimeEvent CreateMemberJoinedEvent(
        string conversationId,
        string title,
        long joinedUserId,
        ConversationMemberRole role,
        long actorUserId,
        long targetUserId,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState) =>
        new()
        {
            EventId = RealtimeEventContracts.CreateMemberJoinedEventId(
                conversationId,
                joinedUserId,
                targetUserId,
                occurredAtMs),
            Type = RealtimeEventType.MemberJoined,
            TargetUserId = targetUserId,
            ActorUserId = actorUserId,
            SessionId = actorSessionId,
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeMemberJoinedPayload
                {
                    ConversationId = conversationId,
                    UserId = joinedUserId,
                    Role = role,
                    ActorUserId = actorUserId,
                    Title = title,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeMemberJoinedPayload),
            OccurredAtMs = occurredAtMs,
            TraceParent = traceParent,
            TraceState = traceState
        };

    private static RealtimeEvent CreateMemberLeftEvent(
        string conversationId,
        long leftUserId,
        long targetUserId,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState) =>
        new()
        {
            EventId = RealtimeEventContracts.CreateMemberLeftEventId(
                conversationId,
                leftUserId,
                targetUserId,
                occurredAtMs),
            Type = RealtimeEventType.MemberLeft,
            TargetUserId = targetUserId,
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
            TraceState = traceState
        };

    private static RealtimeEvent CreateMemberRemovedEvent(
        string conversationId,
        long removedUserId,
        long actorUserId,
        long targetUserId,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState) =>
        new()
        {
            EventId = RealtimeEventContracts.CreateMemberRemovedEventId(
                conversationId,
                removedUserId,
                targetUserId,
                occurredAtMs),
            Type = RealtimeEventType.MemberRemoved,
            TargetUserId = targetUserId,
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
            TraceState = traceState
        };

    private static RealtimeEvent CreateRoleChangedEvent(
        string conversationId,
        long userId,
        ConversationMemberRole previousRole,
        ConversationMemberRole newRole,
        long actorUserId,
        long targetUserId,
        long occurredAtMs,
        string? actorSessionId,
        string? traceParent,
        string? traceState) =>
        new()
        {
            EventId = RealtimeEventContracts.CreateRoleChangedEventId(
                conversationId,
                userId,
                newRole,
                targetUserId,
                occurredAtMs),
            Type = RealtimeEventType.RoleChanged,
            TargetUserId = targetUserId,
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
            TraceState = traceState
        };

    private async Task InsertOutboxManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<RealtimeEvent> events,
        CancellationToken ct)
    {
        if (events.Count == 0)
            return;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        const int chunkSize = 50;
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
                values.Add(
                    $"(@event_id_{i}, @payload_json_{i}, @target_user_id_{i}, @event_type_{i}, @status, @created_at_ms, @next_attempt_at_ms, 0)");
                command.Parameters.AddWithValue($"event_id_{i}", evt.EventId);
                command.Parameters.AddWithValue(
                    $"payload_json_{i}",
                    JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent));
                command.Parameters.AddWithValue($"target_user_id_{i}", evt.TargetUserId);
                command.Parameters.AddWithValue($"event_type_{i}", (short)evt.Type);
            }

            command.Parameters.AddWithValue("status", (short)RealtimeOutboxStatus.Pending);
            command.Parameters.AddWithValue("created_at_ms", now);
            command.Parameters.AddWithValue("next_attempt_at_ms", now);
            command.CommandText =
                $"""
                 INSERT INTO {_databaseSchema.OutboxTableSql} (
                     event_id, payload_json, target_user_id, event_type, status,
                     created_at_ms, next_attempt_at_ms, attempt_count
                 ) VALUES {string.Join(", ", values)}
                 ON CONFLICT (event_id) DO NOTHING;
                 """;
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }
}
