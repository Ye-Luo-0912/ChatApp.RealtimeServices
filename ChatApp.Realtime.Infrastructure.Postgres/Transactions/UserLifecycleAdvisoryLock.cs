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

    /// <summary>
    /// P0-2 / P0-4：批量获取多个用户的共享生命周期锁并检查活跃状态。
    /// <para>
    /// 按 userId 升序获取锁以避免死锁（消除 A→B 与 B→A 并发写入之间的死锁环）。
    /// 用一条 SQL（UNNEST）获取全部 advisory locks，一条 SQL 批量查询 tombstone。
    /// </para>
    /// <para>
    /// 返回 false 时，锁已在事务级别获取（事务回滚会自动释放），调用方应中止操作。
    /// </para>
    /// </summary>
    public static async Task<bool> AcquireSharedAndCheckActiveManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        IReadOnlyCollection<long> userIds,
        CancellationToken ct)
    {
        if (userIds.Count == 0)
            return true;

        // 去重 + 按 userId 升序排序（避免死锁）
        var sortedIds = userIds.Distinct().OrderBy(id => id).ToArray();

        // 在 C# 中预计算 advisory lock 键（namespace XOR user_id），保持与单用户版本
        // CombineKey 一致，避免 SQL 端 XOR 运算符歧义（PostgreSQL 中 # 为位异或，^ 为幂运算）。
        var keys = new long[sortedIds.Length];
        for (var i = 0; i < sortedIds.Length; i++)
            keys[i] = CombineKey(sortedIds[i]);

        // 一条 SQL 获取全部 advisory locks（UNNEST 保留数组顺序，按 userId 升序获取）
        await using (var lockCmd = new NpgsqlCommand(
                         """
                         SELECT pg_advisory_xact_lock_shared(t.key)
                         FROM UNNEST(@keys) AS t(key);
                         """,
                         connection,
                         transaction))
        {
            lockCmd.Parameters.AddWithValue("keys", keys);
            await lockCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // 一条 SQL 批量查询 tombstone。tombstone 表中仅存在 Deleting/Deleted 行（无 Active 行），
        // 因此只要返回任意行即表示对应用户非活跃，应拒绝写入。
        await using var stateCmd = new NpgsqlCommand(
            $"""
             SELECT user_id, state
             FROM {schema.UserDeletionTombstonesTableSql}
             WHERE user_id = ANY(@user_ids);
             """,
            connection,
            transaction);
        stateCmd.Parameters.AddWithValue("user_ids", sortedIds);

        await using var reader = await stateCmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var stateByte = reader.GetByte(1);
            if (stateByte != (byte)UserLifecycleState.Active)
                return false;
        }
        return true;
    }
}
