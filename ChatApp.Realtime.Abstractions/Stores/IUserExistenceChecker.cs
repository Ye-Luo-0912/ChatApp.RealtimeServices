namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 二-1：用户存在性校验接口。
/// <para>
/// 用于校验用户 ID 是否对应真实存在的用户。当前 realtime schema 无独立 users 表，
/// 通过本接口抽象，由外部系统（如 Identity 服务）提供实现。
/// </para>
/// <para>
/// 默认 Noop 实现返回 true（不阻塞），待外部系统接入时替换。
/// </para>
/// </summary>
public interface IUserExistenceChecker
{
    /// <summary>
    /// 校验指定用户是否存在。
    /// </summary>
    /// <param name="userId">用户编号。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>用户存在时返回 true。</returns>
    Task<bool> ExistsAsync(
        long userId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量校验用户是否存在。
    /// </summary>
    /// <param name="userIds">用户编号列表。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>不存在的用户编号列表（空列表表示全部存在）。</returns>
    Task<IReadOnlyList<long>> FilterNonExistentAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default);
}
