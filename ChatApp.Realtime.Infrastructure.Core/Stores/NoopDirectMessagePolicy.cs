using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// 二-3：空实现，始终允许 DM。
/// </summary>
public sealed class NoopDirectMessagePolicy : IDirectMessagePolicy
{
    public static NoopDirectMessagePolicy Instance { get; } = new();

    private NoopDirectMessagePolicy() { }

    public Task<DirectMessagePolicyResult> CheckAsync(
        long senderUserId,
        long receiverUserId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new DirectMessagePolicyResult { Allowed = true });
    }
}