using System.Linq;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Transactions;

/// <summary>
/// P0-2：用户生命周期 advisory lock —— 消除"查 tombstone → 开消息事务 → 提交"之间的 TOCTOU 竞态。
/// <para>
/// 消息/群写入路径在事务内先获取 <c>pg_advisory_xact_lock_shared</c>（共享锁），
/// 再检查 tombstone state，最后执行写入。锁随事务提交/回滚自动释放。
/// </para>
/// <para>
/// 账号删除路径在事务内获取 <c>pg_advisory_xact_lock</c>（排他锁），再写入 tombstone。
/// 排他锁会等待所有共享锁释放，此后新写入的共享锁会等待排他锁释放。
/// 排他锁释放（事务提交）时 tombstone 已持久化，后续写入能读到 state=Deleting 并拒绝。
/// </para>
/// <para>
/// 使用单键版本 <c>pg_advisory_xact_lock_shared(bigint)</c>，键值为 namespace XOR user_id，
/// 避免与 migration（0x5245_414C_5449_4D45）和 retention GC（0x4D53_4752_4554_4E01）冲突。
/// </para>
/// </summary>
/// <summary>
/// 二-6：生命周期检查结果。区分"已注销"和"已冻结"以返回不同错误码。
/// </summary>
internal readonly record struct LifecycleGateResult(bool IsActive, string? ErrorCode)
{
    public static readonly LifecycleGateResult Active = new(true, null);
    public static readonly LifecycleGateResult Deleted = new(false, "user_deleted");
    public static readonly LifecycleGateResult Frozen = new(false, "user_frozen");
}

internal static class UserLifecycleAdvisoryLock
{
    /// <summary>
    /// "USERLIFE" ASCII —— 与 migration（0x5245_414C_5449_4D45）和 retention（0x4D53_4752_4554_4E01）的命名空间隔离。
    /// </summary>
    public const long NamespaceKey = 0x5553_4552_4C49_4645L;

    /// <summary>
    /// 组合键 = namespace XOR user_id。不同用户产生不同键值，namespace 确保不与其他 advisory lock 冲突。
    /// </summary>
    private static long CombineKey(long userId) => NamespaceKey ^ userId;

    /// <summary>
    /// 获取共享事务级 advisory lock。多个写入可并发持有同一用户的共享锁。
    /// 锁在事务提交或回滚时自动释放。
    /// </summary>
    public static async Task AcquireSharedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long userId,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock_shared(@key);",
            connection,
            transaction);
        cmd.Parameters.AddWithValue("key", CombineKey(userId));
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 获取排他事务级 advisory lock。账号删除路径使用，阻塞同一用户的所有共享锁。
    /// 锁在事务提交或回滚时自动释放。
    /// </summary>
    public static async Task AcquireExclusiveAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long userId,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(@key);",
            connection,
            transaction);
        cmd.Parameters.AddWithValue("key", CombineKey(userId));
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 在当前事务内查询用户生命周期状态。调用前应已获取 advisory lock。
    /// </summary>
    public static async Task<UserLifecycleState> GetStateInTxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        long userId,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT state
             FROM {schema.UserDeletionTombstonesTableSql}
             WHERE user_id = @user_id
             LIMIT 1;
             """,
            connection,
            transaction);
        cmd.Parameters.AddWithValue("user_id", userId);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return UserLifecycleState.Active;

        var stateByte = reader.GetByte(0);
        return stateByte switch
        {
            1 => UserLifecycleState.Deleting,
            2 => UserLifecycleState.Deleted,
            3 => UserLifecycleState.Frozen,
            _ => UserLifecycleState.Active
        };
    }

    /// <summary>
    /// 获取共享锁并检查用户是否活跃。返回 IsActive 为 false 时表示用户正在删除、已删除或已冻结，写入应被拒绝。
    /// </summary>
    public static async Task<LifecycleGateResult> AcquireSharedAndCheckActiveAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        long userId,
        CancellationToken ct)
    {
        await AcquireSharedAsync(connection, transaction, userId, ct).ConfigureAwait(false);
        var state = await GetStateInTxAsync(connection, transaction, schema, userId, ct)
            .ConfigureAwait(false);
        return state switch
        {
            UserLifecycleState.Frozen => LifecycleGateResult.Frozen,
            UserLifecycleState.Active => LifecycleGateResult.Active,
            _ => LifecycleGateResult.Deleted
        };
    }

    /// <summary>
    /// P0-2 / P0-4：批量获取多个用户的共享生命周期锁并检查活跃状态。
    /// <para>
    /// 按 userId 升序获取锁以避免死锁（消除 A→B 与 B→A 并发写入之间的死锁环）。
    /// 用一条 SQL（UNNEST）获取全部 advisory locks，一条 SQL 批量查询 tombstone。
    /// </para>
    /// <para>
    /// 返回 IsActive 为 false 时，锁已在事务级别获取（事务回滚会自动释放），调用方应中止操作。
    /// </para>
    /// </summary>
    public static async Task<LifecycleGateResult> AcquireSharedAndCheckActiveManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        IReadOnlyCollection<long> userIds,
        CancellationToken ct)
    {
        if (userIds.Count == 0)
            return LifecycleGateResult.Active;

        var sortedIds = NormalizeSortedDistinct(userIds);
        return await AcquireSharedAndCheckActiveCoreAsync(
            connection,
            transaction,
            schema,
            sortedIds,
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 消息热路径专用双用户入口，避免先构造临时集合再做 LINQ 去重/排序。
    /// receiverUserId &lt;= 0 时只检查发送方。
    /// </summary>
    public static Task<LifecycleGateResult> AcquireSharedAndCheckActiveAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        long senderUserId,
        long receiverUserId,
        CancellationToken ct)
    {
        if (receiverUserId <= 0 || receiverUserId == senderUserId)
            return AcquireSharedAndCheckActiveCoreAsync(
                connection,
                transaction,
                schema,
                [senderUserId],
                ct);

        var sortedIds = senderUserId < receiverUserId
            ? new[] { senderUserId, receiverUserId }
            : new[] { receiverUserId, senderUserId };
        return AcquireSharedAndCheckActiveCoreAsync(
            connection,
            transaction,
            schema,
            sortedIds,
            ct);
    }

    private static async Task<LifecycleGateResult> AcquireSharedAndCheckActiveCoreAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        long[] sortedIds,
        CancellationToken ct)
    {
        // 单条 SQL 同时按 userId 顺序获取全部共享锁并读取 tombstone，减少一次数据库往返。
        // MATERIALIZED CTE 保证 volatile advisory-lock 函数在状态读取前完整执行。
        await using var stateCmd = new NpgsqlCommand(
            $"""
             WITH ordered_users AS MATERIALIZED (
                 SELECT DISTINCT t.user_id
                 FROM UNNEST(@user_ids) AS t(user_id)
                 ORDER BY t.user_id
             ),
             locked_users AS MATERIALIZED (
                 SELECT
                     u.user_id,
                     pg_advisory_xact_lock_shared((@namespace_key::bigint # u.user_id)) AS lock_result
                 FROM ordered_users AS u
             )
             SELECT l.user_id, tombstone.state, l.lock_result
             FROM locked_users AS l
             LEFT JOIN {schema.UserDeletionTombstonesTableSql} AS tombstone
               ON tombstone.user_id = l.user_id
             ORDER BY l.user_id;
             """,
            connection,
            transaction);
        stateCmd.Parameters.AddWithValue("user_ids", sortedIds);
        stateCmd.Parameters.AddWithValue("namespace_key", NamespaceKey);

        await using var reader = await stateCmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (reader.IsDBNull(1))
                continue;
            var stateByte = reader.GetByte(1);
            if (stateByte == (byte)UserLifecycleState.Frozen)
                return LifecycleGateResult.Frozen;
            if (stateByte != (byte)UserLifecycleState.Active)
                return LifecycleGateResult.Deleted;
        }
        return LifecycleGateResult.Active;
    }

    private static long[] NormalizeSortedDistinct(IReadOnlyCollection<long> userIds)
    {
        if (userIds.Count == 1)
        {
            foreach (var userId in userIds)
                return [userId];
        }

        if (userIds.Count == 2)
        {
            using var enumerator = userIds.GetEnumerator();
            enumerator.MoveNext();
            var first = enumerator.Current;
            enumerator.MoveNext();
            var second = enumerator.Current;
            if (first == second)
                return [first];
            return first < second ? [first, second] : [second, first];
        }

        return userIds.Distinct().OrderBy(static id => id).ToArray();
    }
}
