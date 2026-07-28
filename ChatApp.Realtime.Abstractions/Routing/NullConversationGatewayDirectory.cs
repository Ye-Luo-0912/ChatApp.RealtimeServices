namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 空实现：始终返回 <see cref="GatewayLookupResultKind.LookupFailure"/>。
/// <para>
/// 用于未配置会话路由目录时的回退。调用方收到 LookupFailure 后会回退到
/// <see cref="IGatewayDirectory"/> 的 per-user 路由，保证不丢事件。
/// </para>
/// </summary>
public sealed class NullConversationGatewayDirectory : IConversationGatewayDirectory
{
    private static readonly IReadOnlyList<string> Empty = Array.Empty<string>();

    public static NullConversationGatewayDirectory Instance { get; } = new();

    public Task<GatewayLookupResult> GetConversationGatewaysAsync(
        string conversationId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new GatewayLookupResult(
            GatewayLookupResultKind.LookupFailure,
            Empty));
    }
}
