namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// @提及（mentions）业务校验常量。
/// <para>
/// 与 <see cref="Protocol.RealtimeWireLimits"/> 中"群成员事件聚合硬限制"并列：
/// 那些是 wire/event 层的硬上限，这里是单条消息 @提及 的业务上限。
/// 数量超限时由 <c>MentionValidator</c> 截断而非拒绝整条消息。
/// </para>
/// </summary>
public static class MentionOptions
{
    /// <summary>单条消息 @提及用户数上限（去重 + 排除自身后截断）。</summary>
    public const int MaxMentionedUserIds = 50;

    /// <summary>单条消息 @提及角色数上限（去重 + 白名单过滤后截断）。</summary>
    public const int MaxMentionedRoles = 5;

    /// <summary>
    /// 允许的 mention 角色字面量（小写、Ordinal 比较）。
    /// 任何非白名单角色静默移除，不抛异常。
    /// </summary>
    public static readonly HashSet<string> AllowedRoles = new(StringComparer.Ordinal)
    {
        "all",
        "admin"
    };

    /// <summary>
    /// 需要群管理员权限才能使用的 mention 角色字面量（小写、Ordinal 比较）。
    /// 非管理员（普通 Member）发送的此类角色静默移除。
    /// Owner / Admin 角色可以发送。
    /// </summary>
    public static readonly HashSet<string> RolesRequiringManager = new(StringComparer.Ordinal)
    {
        "all",
        "admin"
    };

    /// <summary>
    /// 判断给定角色字面量是否为群管理员可用的 mention 角色。
    /// 输入会先 trim 再 OrdinalIgnoreCase 比较，最终归一化为小写。
    /// </summary>
    public static bool IsManagerRole(string? role)
    {
        if (string.IsNullOrWhiteSpace(role))
            return false;
        return RolesRequiringManager.Contains(role.Trim());
    }
}
