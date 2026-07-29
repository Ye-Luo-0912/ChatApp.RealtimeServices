using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Buffers;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 稳定内容指纹：同 (sender, client_message_id) 下用于区分真重放与内容冲突。
/// <para>
/// v4 起使用长度前缀编码（<c>{byteCount}:{utf8}</c>）替代 <c>\n</c> 分隔符，消除字段内含
/// <c>\n</c> 导致的哈希碰撞；并覆盖 Reply/Forward 的 sender 与 preview，防止仅引用元数据
/// 变化而不触发冲突。输出仍为 64 位十六进制（兼容 varchar(64)）。
/// </para>
/// <para>
/// P0-10：指纹应在 mentions sanitization 之前基于原始请求计算，避免 sanitization 结果
/// 依赖当前成员/角色导致相同请求在不同时刻产生不同指纹（误判冲突）。
/// </para>
/// </summary>
public static class RealtimeMessageFingerprint
{
    /// <summary>当前写入语义版本（哈希输入前缀 <c>v4:</c>）。</summary>
    public const int CurrentVersion = 4;

    /// <summary>
    /// 计算 v4 指纹：receiver + conversation + content + 排序唯一附件 Id +
    /// reply（含 sender/preview）+ forward（含 sender/preview）+ 排序去重 @提及用户与角色。
    /// 每个字段以长度前缀编码，消除分隔符碰撞。同内容且同附件集合（任意顺序）→ 相同指纹；
    /// 任意覆盖字段不同 → 不同指纹。
    /// </summary>
    public static string Compute(
        long receiverUserId,
        string? content,
        IReadOnlyList<string>? attachmentIds = null,
        string? conversationId = null,
        string? replyToMessageId = null,
        string? forwardedFromMessageId = null,
        IReadOnlyList<long>? mentionedUserIds = null,
        IReadOnlyList<string>? mentionedRoles = null,
        long? replyToSenderUserId = null,
        string? replyToPreview = null,
        long? forwardedFromSenderUserId = null,
        string? forwardedFromPreview = null)
    {
        // P0-8：修复 Content=null 的 NRE，attachment-only 消息视为空字符串
        var safeContent = content ?? string.Empty;

        // P0-10：长度前缀编码。每个字段写为 "{utf8字节长度}:{内容}"，
        // 列表写为 "{count}:" 后跟 count 个长度前缀元素。
        // 长度前缀保证字段内含 ':' 或 '\n' 时不产生碰撞。
        var sb = new StringBuilder(256);
        sb.Append("v4:");
        AppendLengthPrefixed(sb, receiverUserId.ToString(CultureInfo.InvariantCulture));
        AppendLengthPrefixed(sb, conversationId ?? string.Empty);
        AppendLengthPrefixed(sb, safeContent);
        AppendLengthPrefixedList(sb, NormalizeAttachmentIds(attachmentIds));
        AppendLengthPrefixed(sb, replyToMessageId ?? string.Empty);
        AppendLengthPrefixed(sb, forwardedFromMessageId ?? string.Empty);
        // P0-10：覆盖 Reply/Forward 的 sender 与 preview
        AppendLengthPrefixed(sb, (replyToSenderUserId ?? 0).ToString(CultureInfo.InvariantCulture));
        AppendLengthPrefixed(sb, replyToPreview ?? string.Empty);
        AppendLengthPrefixed(sb, (forwardedFromSenderUserId ?? 0).ToString(CultureInfo.InvariantCulture));
        AppendLengthPrefixed(sb, forwardedFromPreview ?? string.Empty);
        AppendLengthPrefixedLongList(sb, NormalizeMentionedUserIds(mentionedUserIds));
        AppendLengthPrefixedList(sb, NormalizeMentionedRoles(mentionedRoles));

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        return Convert.ToHexStringLower(SHA256.HashData(bytes));
    }

    /// <summary>
    /// 旧版 v1（仅 receiver+content）。仅用于对照/迁移；新写入一律用 <see cref="Compute"/>。
    /// </summary>
    public static string ComputeV1(long receiverUserId, string content)
    {
        ArgumentNullException.ThrowIfNull(content);

        Span<byte> hash = stackalloc byte[32];
        using (var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
        {
            AppendUserId(hasher, receiverUserId);
            AppendUtf8(hasher, "\n"u8);
            AppendUtf8String(hasher, content);

            if (!hasher.TryGetHashAndReset(hash, out var written) || written != 32)
                throw new CryptographicException("SHA-256 指纹计算失败。");
        }

        return Convert.ToHexStringLower(hash);
    }

    public static string[] NormalizeAttachmentIds(IReadOnlyList<string>? attachmentIds)
    {
        if (attachmentIds is null || attachmentIds.Count == 0)
            return [];

        // 入口规范化：去空白、去重、排序；避免 Compute 内重复 LINQ。
        var buffer = new string[attachmentIds.Count];
        var count = 0;
        for (var i = 0; i < attachmentIds.Count; i++)
        {
            var id = attachmentIds[i];
            if (string.IsNullOrWhiteSpace(id))
                continue;
            var trimmed = id.Trim();
            var exists = false;
            for (var j = 0; j < count; j++)
            {
                if (string.Equals(buffer[j], trimmed, StringComparison.Ordinal))
                {
                    exists = true;
                    break;
                }
            }

            if (!exists)
                buffer[count++] = trimmed;
        }

        if (count == 0)
            return [];

        Array.Sort(buffer, 0, count, StringComparer.Ordinal);
        if (count == buffer.Length)
            return buffer;

        var exact = new string[count];
        Array.Copy(buffer, exact, count);
        return exact;
    }

    /// <summary>
    /// 与已有行比较：存储指纹可直接相等；NULL/旧版本则用已有行数据重算 v4。
    /// 新增覆盖字段以可选参数传入，旧调用方（仅传基础字段）仍可兼容。
    /// </summary>
    public static bool MatchesExisting(
        string? storedFingerprint,
        long existingReceiverUserId,
        string? existingContent,
        IReadOnlyList<string>? existingAttachmentIds,
        string incomingFingerprint,
        string? existingConversationId = null,
        string? existingReplyToMessageId = null,
        string? existingForwardedFromMessageId = null,
        IReadOnlyList<long>? existingMentionedUserIds = null,
        IReadOnlyList<string>? existingMentionedRoles = null,
        long? existingReplyToSenderUserId = null,
        string? existingReplyToPreview = null,
        long? existingForwardedFromSenderUserId = null,
        string? existingForwardedFromPreview = null)
    {
        if (string.Equals(storedFingerprint, incomingFingerprint, StringComparison.Ordinal))
            return true;

        var recomputed = Compute(
            existingReceiverUserId,
            existingContent,
            existingAttachmentIds,
            existingConversationId,
            existingReplyToMessageId,
            existingForwardedFromMessageId,
            existingMentionedUserIds,
            existingMentionedRoles,
            existingReplyToSenderUserId,
            existingReplyToPreview,
            existingForwardedFromSenderUserId,
            existingForwardedFromPreview);
        return string.Equals(recomputed, incomingFingerprint, StringComparison.Ordinal);
    }

    /// <summary>
    /// @提及用户 Id 规范化：去重、排序，保证任意顺序输入产生相同指纹。
    /// </summary>
    public static long[] NormalizeMentionedUserIds(IReadOnlyList<long>? mentionedUserIds)
    {
        if (mentionedUserIds is null || mentionedUserIds.Count == 0)
            return [];

        // 去重后排序：HashSet 去重，再排序保证顺序稳定
        var set = new HashSet<long>(mentionedUserIds);
        if (set.Count == 0)
            return [];

        var result = new long[set.Count];
        set.CopyTo(result);
        Array.Sort(result);
        return result;
    }

    /// <summary>
    /// @提及角色规范化：去空白、去重、排序，保证任意顺序输入产生相同指纹。
    /// </summary>
    public static string[] NormalizeMentionedRoles(IReadOnlyList<string>? mentionedRoles)
    {
        if (mentionedRoles is null || mentionedRoles.Count == 0)
            return [];

        var buffer = new string[mentionedRoles.Count];
        var count = 0;
        for (var i = 0; i < mentionedRoles.Count; i++)
        {
            var role = mentionedRoles[i];
            if (string.IsNullOrWhiteSpace(role))
                continue;
            var trimmed = role.Trim();
            var exists = false;
            for (var j = 0; j < count; j++)
            {
                if (string.Equals(buffer[j], trimmed, StringComparison.Ordinal))
                {
                    exists = true;
                    break;
                }
            }

            if (!exists)
                buffer[count++] = trimmed;
        }

        if (count == 0)
            return [];

        Array.Sort(buffer, 0, count, StringComparer.Ordinal);
        if (count == buffer.Length)
            return buffer;

        var exact = new string[count];
        Array.Copy(buffer, exact, count);
        return exact;
    }

    // P0-10：长度前缀编码辅助方法。每个字段写为 "{utf8字节长度}:{内容}"，
    // 列表写为 "{count}:" 后跟 count 个长度前缀元素。长度前缀消除分隔符碰撞。

    private static void AppendLengthPrefixed(StringBuilder sb, string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        sb.Append(byteCount).Append(':').Append(value);
    }

    private static void AppendLengthPrefixedList(StringBuilder sb, string[] items)
    {
        sb.Append(items.Length).Append(':');
        for (var i = 0; i < items.Length; i++)
            AppendLengthPrefixed(sb, items[i]);
    }

    private static void AppendLengthPrefixedLongList(StringBuilder sb, long[] items)
    {
        sb.Append(items.Length).Append(':');
        for (var i = 0; i < items.Length; i++)
            AppendLengthPrefixed(sb, items[i].ToString(CultureInfo.InvariantCulture));
    }

    // 以下 IncrementalHash 辅助方法仅供 ComputeV1 使用（旧版本兼容，不修改）。

    private static void AppendUserId(IncrementalHash hasher, long userId)
    {
        Span<char> chars = stackalloc char[20];
        if (!userId.TryFormat(chars, out var written))
            throw new InvalidOperationException("用户 Id 格式化失败。");
        AppendUtf8Chars(hasher, chars[..written]);
    }

    private static void AppendUtf8(IncrementalHash hasher, ReadOnlySpan<byte> utf8) =>
        hasher.AppendData(utf8);

    private static void AppendUtf8String(IncrementalHash hasher, string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount <= 256)
        {
            Span<byte> buffer = stackalloc byte[byteCount];
            Encoding.UTF8.GetBytes(value, buffer);
            hasher.AppendData(buffer);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            var written = Encoding.UTF8.GetBytes(value, rented.AsSpan(0, byteCount));
            hasher.AppendData(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void AppendUtf8Chars(IncrementalHash hasher, ReadOnlySpan<char> chars)
    {
        var byteCount = Encoding.UTF8.GetByteCount(chars);
        Span<byte> buffer = stackalloc byte[byteCount];
        Encoding.UTF8.GetBytes(chars, buffer);
        hasher.AppendData(buffer);
    }
}
