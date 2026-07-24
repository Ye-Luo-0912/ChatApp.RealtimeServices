using System.Globalization;

namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 稳定会话标识。单聊为确定性字符串 <c>dm:{minUserId}:{maxUserId}</c>。
/// </summary>
public static class ConversationId
{
    public const int MaxLength = 64;
    public const int PreviewMaxChars = 256;
    public const int TitleMaxLength = 128;
    private const string DirectPrefix = "dm:";
    private const string GroupPrefix = "grp:";

    /// <summary>
    /// 由双方用户编号生成单聊会话 ID（较小用户编号在前）。
    /// </summary>
    public static string CreateDirect(long userA, long userB)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userA);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userB);
        if (userA == userB)
        {
            throw new ArgumentException("单聊会话双方用户编号不能相同。", nameof(userB));
        }

        var lo = userA < userB ? userA : userB;
        var hi = userA < userB ? userB : userA;
        var loDigits = CountDigits(lo);
        var hiDigits = CountDigits(hi);
        var length = DirectPrefix.Length + loDigits + 1 + hiDigits;

        return string.Create(
            length,
            (lo, hi, loDigits),
            static (span, state) =>
            {
                DirectPrefix.AsSpan().CopyTo(span);
                var offset = DirectPrefix.Length;
                WriteUnsignedDecimal(span.Slice(offset, state.loDigits), state.lo);
                offset += state.loDigits;
                span[offset++] = ':';
                WriteUnsignedDecimal(span.Slice(offset), state.hi);
            });
    }

    public static bool TryParseDirect(
        ReadOnlySpan<char> conversationId,
        out long userLo,
        out long userHi)
    {
        userLo = 0;
        userHi = 0;
        if (!conversationId.StartsWith(DirectPrefix, StringComparison.Ordinal))
            return false;

        var rest = conversationId[DirectPrefix.Length..];
        var separator = rest.IndexOf(':');
        if (separator <= 0 || separator >= rest.Length - 1)
            return false;

        if (!long.TryParse(rest[..separator], NumberStyles.None, CultureInfo.InvariantCulture, out userLo)
            || !long.TryParse(rest[(separator + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out userHi))
        {
            userLo = 0;
            userHi = 0;
            return false;
        }

        if (userLo <= 0 || userHi <= 0 || userLo >= userHi)
        {
            userLo = 0;
            userHi = 0;
            return false;
        }

        return true;
    }

    public static bool IsDirect(ReadOnlySpan<char> conversationId) =>
        TryParseDirect(conversationId, out _, out _);

    /// <summary>
    /// 生成群聊会话 ID：<c>grp:{32 hex}</c>（Guid N 格式）。
    /// </summary>
    public static string CreateGroup()
    {
        var hex = Guid.CreateVersion7().ToString("N");
        return string.Create(
            GroupPrefix.Length + hex.Length,
            hex,
            static (span, state) =>
            {
                GroupPrefix.AsSpan().CopyTo(span);
                state.AsSpan().CopyTo(span[GroupPrefix.Length..]);
            });
    }

    public static bool IsGroup(ReadOnlySpan<char> conversationId)
    {
        if (!conversationId.StartsWith(GroupPrefix, StringComparison.Ordinal))
            return false;
        if (conversationId.Length != GroupPrefix.Length + 32)
            return false;
        var hex = conversationId[GroupPrefix.Length..];
        foreach (var c in hex)
        {
            var isHex = (c is >= '0' and <= '9')
                        || (c is >= 'a' and <= 'f')
                        || (c is >= 'A' and <= 'F');
            if (!isHex)
                return false;
        }

        return true;
    }

    /// <summary>
    /// 截断会话列表摘要，避免大正文进入会话投影与事件 payload。
    /// </summary>
    public static string CreatePreview(ReadOnlySpan<char> content, int maxChars = PreviewMaxChars)
    {
        if (maxChars <= 0)
            return string.Empty;
        if (content.Length <= maxChars)
            return content.Length == 0 ? string.Empty : content.ToString();

        return content[..maxChars].ToString();
    }

    private static int CountDigits(long value)
    {
        // value > 0（调用方已保证）
        var digits = 1;
        while (value >= 10)
        {
            value /= 10;
            digits++;
        }

        return digits;
    }

    private static void WriteUnsignedDecimal(Span<char> destination, long value)
    {
        // Utf8Formatter 走字节；这里直接写十进制到 char span，避免额外编码缓冲。
        var index = destination.Length;
        do
        {
            value = Math.DivRem(value, 10, out var digit);
            destination[--index] = (char)('0' + digit);
        } while (value > 0);
    }
}
