using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// 二-1：空实现，始终返回"用户存在"（不阻塞）。待外部系统接入时替换。
/// </summary>
public sealed class NoopUserExistenceChecker : IUserExistenceChecker
{
    public static NoopUserExistenceChecker Instance { get; } = new();

    private NoopUserExistenceChecker() { }

    public Task<bool> ExistsAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(true);
    }

    public Task<IReadOnlyList<long>> FilterNonExistentAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IReadOnlyList<long>>(Array.Empty<long>());
    }
}