using System.Security.Cryptography;
using System.Text;
using System.Buffers;

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

        Span<byte> hash = stackalloc byte[32];
        using (var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
        {
            AppendUtf8(hasher, "v2\n"u8);
            AppendUserId(hasher, receiverUserId);
            AppendUtf8(hasher, "\n"u8);
            AppendUtf8String(hasher, content);
            AppendUtf8(hasher, "\n"u8);
            AppendNormalizedAttachmentIds(hasher, attachmentIds);

            if (!hasher.TryGetHashAndReset(hash, out var written) || written != 32)
                throw new CryptographicException("SHA-256 指纹计算失败。");
        }

        return Convert.ToHexStringLower(hash);
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

    private static void AppendNormalizedAttachmentIds(
        IncrementalHash hasher,
        IReadOnlyList<string>? attachmentIds)
    {
        var normalized = NormalizeAttachmentIds(attachmentIds);
        for (var i = 0; i < normalized.Length; i++)
        {
            if (i > 0)
                AppendUtf8(hasher, ","u8);
            AppendUtf8String(hasher, normalized[i]);
        }
    }

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
