using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

/// <summary>
/// @提及（mentions）业务校验器：对 <see cref="IncomingMessageCommand.MentionedUserIds"/> /
/// <see cref="IncomingMessageCommand.MentionedRoles"/> 做去重、排除自身、截断与角色白名单过滤。
/// <para>
/// 所有违规一律 <b>静默过滤</b>，不抛异常、不拒绝消息。这与 Realtime 的"消息必达"语义一致：
/// 客户端的 UI 错误不应阻塞消息写入；服务端只做净化。
/// </para>
/// <para>
/// 调用方负责提供 <c>isManager</c> 上下文（发送者是否群 Owner/Admin）与可选的群成员集合。
/// 本类是纯函数，无 I/O。
/// </para>
/// </summary>
internal static class MentionValidator
{
    /// <summary>
    /// 规范化 MentionedUserIds：
    /// <list type="number">
    /// <item>过滤掉非正数（防御性）。</item>
    /// <item>排除 sender 自己（不能 @ 自己）。</item>
    /// <item>HashSet 去重。</item>
    /// <item>若提供 <paramref name="activeMemberUserIds"/>：仅保留群活跃成员。</item>
    /// <item>排序后截断到 <see cref="MentionOptions.MaxMentionedUserIds"/>。</item>
    /// </list>
    /// </summary>
    /// <returns>
    /// 规范化后的数组（按升序）；输入为空或全部被过滤时返回 null（表示无 mention）。
    /// </returns>
    public static long[]? NormalizeUserIds(
        IReadOnlyList<long>? mentionedUserIds,
        long senderUserId,
        IReadOnlyCollection<long>? activeMemberUserIds = null)
    {
        if (mentionedUserIds is null || mentionedUserIds.Count == 0)
            return null;

        var set = new HashSet<long>(mentionedUserIds.Count);
        foreach (var id in mentionedUserIds)
        {
            if (id <= 0)
                continue;
            if (id == senderUserId)
                continue;
            set.Add(id);
        }

        if (set.Count == 0)
            return null;

        long[] sorted;
        if (activeMemberUserIds is { Count: > 0 } members)
        {
            // 仅保留活跃群成员；非成员静默移除。
            // 注意：若 members 集合为空（群不存在或发送方非成员），不做过滤——
            // 由下游 SaveAsync 的权威成员校验拒绝消息；此处保留原列表避免误清。
            var filtered = new List<long>(set.Count);
            foreach (var id in set)
            {
                if (members.Contains(id))
                    filtered.Add(id);
            }

            if (filtered.Count == 0)
                return null;

            sorted = filtered.ToArray();
        }
        else
        {
            sorted = new long[set.Count];
            set.CopyTo(sorted);
        }

        Array.Sort(sorted);

        if (sorted.Length > MentionOptions.MaxMentionedUserIds)
        {
            var truncated = new long[MentionOptions.MaxMentionedUserIds];
            Array.Copy(sorted, truncated, MentionOptions.MaxMentionedUserIds);
            return truncated;
        }

        return sorted;
    }

    /// <summary>
    /// 规范化 MentionedRoles：
    /// <list type="number">
    /// <item>trim + OrdinalIgnoreCase 归一化为小写。</item>
    /// <item>白名单过滤（仅保留 <see cref="MentionOptions.AllowedRoles"/>）。</item>
    /// <item>权限过滤：非管理员发送的 <see cref="MentionOptions.RolesRequiringManager"/> 角色静默移除。</item>
    /// <item>HashSet 去重。</item>
    /// <item>排序后截断到 <see cref="MentionOptions.MaxMentionedRoles"/>。</item>
    /// </list>
    /// </summary>
    /// <returns>
    /// 规范化后的数组（Ordinal 升序）；输入为空或全部被过滤时返回 null。
    /// </returns>
    public static string[]? NormalizeRoles(
        IReadOnlyList<string>? mentionedRoles,
        bool isManager)
    {
        if (mentionedRoles is null || mentionedRoles.Count == 0)
            return null;

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var raw in mentionedRoles)
        {
            if (string.IsNullOrWhiteSpace(raw))
                continue;
            var role = raw.Trim();
            // 白名单：仅允许 all / admin
            if (!MentionOptions.AllowedRoles.Contains(role))
                continue;
            // 权限：非管理员不能 @all / @admin
            if (!isManager && MentionOptions.RolesRequiringManager.Contains(role))
                continue;
            seen.Add(role);
        }

        if (seen.Count == 0)
            return null;

        var sorted = new string[seen.Count];
        seen.CopyTo(sorted);
        Array.Sort(sorted, StringComparer.Ordinal);

        if (sorted.Length > MentionOptions.MaxMentionedRoles)
        {
            var truncated = new string[MentionOptions.MaxMentionedRoles];
            Array.Copy(sorted, truncated, MentionOptions.MaxMentionedRoles);
            return truncated;
        }

        return sorted;
    }

    /// <summary>
    /// 包装为 <see cref="IReadOnlyList{T}"/>：null 输入返回 null，非空数组包装为只读。
    /// 供 <see cref="RealtimeMessageRecord.MentionedUserIds"/> 等只读字段使用。
    /// </summary>
    public static IReadOnlyList<long>? AsReadOnly(long[]? arr) => arr is null || arr.Length == 0 ? null : arr;

    /// <summary>同 <see cref="AsReadOnly(long[])"/> 的字符串重载。</summary>
    public static IReadOnlyList<string>? AsReadOnly(string[]? arr) => arr is null || arr.Length == 0 ? null : arr;
}
