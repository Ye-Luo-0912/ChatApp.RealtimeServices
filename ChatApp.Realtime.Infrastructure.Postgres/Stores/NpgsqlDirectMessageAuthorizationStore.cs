using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL 单聊授权聚合查询。把原来的发送方存在、接收方存在、屏蔽、隐私和双向好友
/// 五次串行查询合并为一次数据库往返。
/// </summary>
public sealed class NpgsqlDirectMessageAuthorizationStore : IDirectMessageAuthorizationStore
{
    private readonly RealtimeDatabaseClient _databaseClient;

    public NpgsqlDirectMessageAuthorizationStore(RealtimeDatabaseClient databaseClient)
    {
        _databaseClient = databaseClient;
    }

    public async Task<DirectMessageAuthorizationResult> AuthorizeAsync(
        long senderUserId,
        long receiverUserId,
        CancellationToken cancellationToken = default)
    {
        if (senderUserId <= 0)
        {
            return new DirectMessageAuthorizationResult(
                DirectMessageAuthorizationDecision.SenderNotFound);
        }

        if (receiverUserId <= 0)
        {
            return new DirectMessageAuthorizationResult(
                DirectMessageAuthorizationDecision.ReceiverNotFound);
        }

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            """
            SELECT
                EXISTS (
                    SELECT 1
                    FROM public."AspNetUsers"
                    WHERE "Id" = @sender_id
                ) AS sender_exists,
                EXISTS (
                    SELECT 1
                    FROM public."AspNetUsers"
                    WHERE "Id" = @receiver_id
                ) AS receiver_exists,
                EXISTS (
                    SELECT 1
                    FROM public."T_BlockRecords"
                    WHERE "BlockerId" = @receiver_id
                      AND "BlockedUserId" = @sender_id
                ) AS is_blocked,
                COALESCE((
                    SELECT "FriendRequestPolicy"::int
                    FROM public."AspNetUsers"
                    WHERE "Id" = @receiver_id
                    LIMIT 1
                ), -1) AS privacy_policy,
                (
                    EXISTS (
                        SELECT 1
                        FROM public."T_UserFriendEntry"
                        WHERE "UserId" = @sender_id
                          AND "FriendId" = @receiver_id
                          AND NOT "IsDeleted"
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM public."T_UserFriendEntry"
                        WHERE "UserId" = @receiver_id
                          AND "FriendId" = @sender_id
                          AND NOT "IsDeleted"
                    )
                ) AS are_friends;
            """,
            connection);
        command.Parameters.Add("sender_id", NpgsqlDbType.Bigint).Value = senderUserId;
        command.Parameters.Add("receiver_id", NpgsqlDbType.Bigint).Value = receiverUserId;

        await using var reader = await command
            .ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("单聊授权聚合查询未返回结果。");
        }

        if (!reader.GetBoolean(0))
        {
            return new DirectMessageAuthorizationResult(
                DirectMessageAuthorizationDecision.SenderNotFound);
        }

        if (!reader.GetBoolean(1))
        {
            return new DirectMessageAuthorizationResult(
                DirectMessageAuthorizationDecision.ReceiverNotFound);
        }

        if (reader.GetBoolean(2))
        {
            return new DirectMessageAuthorizationResult(
                DirectMessageAuthorizationDecision.Blocked);
        }

        // 保持现有策略顺序和语义：DisallowAll(2) 先拒绝，其余策略继续要求双向好友。
        if (reader.GetInt32(3) == 2)
        {
            return new DirectMessageAuthorizationResult(
                DirectMessageAuthorizationDecision.PrivacyRejected);
        }

        if (!reader.GetBoolean(4))
        {
            return new DirectMessageAuthorizationResult(
                DirectMessageAuthorizationDecision.NotFriend);
        }

        return DirectMessageAuthorizationResult.Success;
    }
}
