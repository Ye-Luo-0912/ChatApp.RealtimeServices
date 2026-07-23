using System.Security.Cryptography;
using System.Text;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 稳定内容指纹：同 (sender, client_message_id) 下用于区分真重放与内容冲突。
/// v2 起哈希输入包含排序去重后的附件 Id 集合；输出仍为 64 位十六进制（兼容 varchar(64)）。
/// </summary>
public static class RealtimeMessageFingerprint
{
    /// <summary>当前写入语义版本（哈希输入前缀 <c>v2\n</c>）。</summary>
    public const int CurrentVersion = 2;

    /// <summary>
    /// 计算 v2 指纹：receiver + content + 排序唯一附件 Id。
    /// 同内容且同附件集合（任意顺序）→ 相同指纹；附件集合不同 → 不同指纹。
    /// </summary>
    public static string Compute(
        long receiverUserId,
        string content,
        IReadOnlyList<string>? attachmentIds = null)
    {
        ArgumentNullException.ThrowIfNull(content);
        var normalized = NormalizeAttachmentIds(attachmentIds);
        var attachmentsPart = string.Join(',', normalized);
        var input = Encoding.UTF8.GetBytes($"v2\n{receiverUserId}\n{content}\n{attachmentsPart}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 旧版 v1（仅 receiver+content）。仅用于对照/迁移；新写入一律用 <see cref="Compute"/>。
    /// </summary>
    public static string ComputeV1(long receiverUserId, string content)
    {
        ArgumentNullException.ThrowIfNull(content);
        var input = Encoding.UTF8.GetBytes($"{receiverUserId}\n{content}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string[] NormalizeAttachmentIds(IReadOnlyList<string>? attachmentIds)
    {
        if (attachmentIds is null || attachmentIds.Count == 0)
            return [];

        return attachmentIds
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Select(static id => id.Trim())
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static id => id, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>
    /// 与已有行比较：存储指纹可直接相等；NULL/旧 v1 则用已有附件集合重算 v2。
    /// </summary>
    public static bool MatchesExisting(
        string? storedFingerprint,
        long existingReceiverUserId,
        string existingContent,
        IReadOnlyList<string>? existingAttachmentIds,
        string incomingFingerprint)
    {
        if (string.Equals(storedFingerprint, incomingFingerprint, StringComparison.Ordinal))
            return true;

        var recomputed = Compute(existingReceiverUserId, existingContent, existingAttachmentIds);
        return string.Equals(recomputed, incomingFingerprint, StringComparison.Ordinal);
    }
}
