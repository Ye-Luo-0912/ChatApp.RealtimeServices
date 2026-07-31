using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// 二-4：空实现，始终允许 DM。
/// </summary>
public sealed class NoopPrivacySettingStore : IPrivacySettingStore
{
    public static NoopPrivacySettingStore Instance { get; } = new();

    private NoopPrivacySettingStore() { }

    public Task<bool> AllowsDirectMessageAsync(
        long userId,
        long targetUserId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(true);
    }
}