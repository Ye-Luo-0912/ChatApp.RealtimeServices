namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageEditCommand
{
    public required string RequestId { get; init; }
    public required string MessageId { get; init; }
    public required string Content { get; init; }
    public long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public long OccurredAtMs { get; init; }

    /// <summary>
    /// 编辑后替换的 @提及用户 Id 列表。
    /// <para>
    /// <c>null</c>（默认）表示不修改现有 mentions，保留消息原值；
    /// 非空数组（包括空数组）会替换原值。服务端会做去重 / 排除自身 / 截断 / 群成员校验 / @all|@admin 权限校验。
    /// </para>
    /// </summary>
    public IReadOnlyList<long>? MentionedUserIds { get; init; }

    /// <summary>
    /// 编辑后替换的 @提及角色列表。语义同 <see cref="MentionedUserIds"/>：
    /// <c>null</c> 不修改；非空数组（包括空数组）替换。
    /// </summary>
    public IReadOnlyList<string>? MentionedRoles { get; init; }
}
