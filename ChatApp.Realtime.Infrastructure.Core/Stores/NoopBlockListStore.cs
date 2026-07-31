using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// 二-2：空实现，始终返回"未屏蔽"。
/// </summary>
public sealed class NoopBlockListStore : IBlockListStore
{
    public static NoopBlockListStore Instance { get; } = new();

    private NoopBlockListStore() { }

    public Task<bool> IsBlockedAsync(
        long receiverUserId,
        long senderUserId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(false);
    }
}