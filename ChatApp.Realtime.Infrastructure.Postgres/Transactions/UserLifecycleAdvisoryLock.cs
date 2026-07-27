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
            _ => UserLifecycleState.Active
        };
    }

    /// <summary>
    /// 获取共享锁并检查用户是否活跃。返回 false 表示用户正在删除或已删除，写入应被拒绝。
    /// </summary>
    public static async Task<bool> AcquireSharedAndCheckActiveAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        long userId,
        CancellationToken ct)
    {
        await AcquireSharedAsync(connection, transaction, userId, ct).ConfigureAwait(false);
        var state = await GetStateInTxAsync(connection, transaction, schema, userId, ct)
            .ConfigureAwait(false);
        return state == UserLifecycleState.Active;
    }
}
