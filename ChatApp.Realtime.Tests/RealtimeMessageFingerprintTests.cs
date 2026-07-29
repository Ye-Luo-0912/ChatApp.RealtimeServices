using ChatApp.Realtime.Abstractions.Stores;
using Xunit;

namespace ChatApp.Realtime.Tests;

public sealed class RealtimeMessageFingerprintTests
{
    [Fact]
    public void NormalizeAttachmentIds_SortsAndDedupes()
    {
        var normalized = RealtimeMessageFingerprint.NormalizeAttachmentIds(["b", "a", "b", ""]);
        Assert.Equal(["a", "b"], normalized);
    }

    [Fact]
    public void Compute_IsOrderIndependent_ForSameAttachmentSet()
    {
        var first = RealtimeMessageFingerprint.Compute(42, "hello", ["z", "a", "m"]);
        var second = RealtimeMessageFingerprint.Compute(42, "hello", ["m", "a", "z"]);
        Assert.Equal(first, second);
        Assert.Equal(64, first.Length);
        Assert.NotEqual(first, RealtimeMessageFingerprint.ComputeV1(42, "hello"));
    }

    [Fact]
    public void Compute_Differs_WhenAttachmentSetDiffers()
    {
        var withA = RealtimeMessageFingerprint.Compute(42, "hello", ["a"]);
        var withB = RealtimeMessageFingerprint.Compute(42, "hello", ["b"]);
        Assert.NotEqual(withA, withB);
    }

    [Fact]
    public void MatchesExisting_RecomputesV1StoredFingerprint_WithEmptyAttachments()
    {
        var v1 = RealtimeMessageFingerprint.ComputeV1(42, "hello");
        var incoming = RealtimeMessageFingerprint.Compute(42, "hello");
        Assert.True(RealtimeMessageFingerprint.MatchesExisting(
            v1,
            42,
            "hello",
            Array.Empty<string>(),
            incoming));
    }

    // P0-10：长度前缀编码消除字段内含 \n 导致的哈希碰撞。
    // v3 的 \n 分隔符下，("a", "b\n") 与 ("a\n", "b") 会产生相同指纹；
    // v4 长度前缀下两者必须不同。
    [Fact]
    public void Compute_LengthPrefixEncoding_PreventsNewlineDelimiterCollision()
    {
        var first = RealtimeMessageFingerprint.Compute(
            receiverUserId: 42,
            content: "b\n",
            conversationId: "a");
        var second = RealtimeMessageFingerprint.Compute(
            receiverUserId: 42,
            content: "b",
            conversationId: "a\n");
        Assert.NotEqual(first, second);
    }

    // P0-10：Reply/Forward 的 sender 与 preview 纳入指纹，
    // 仅引用元数据变化也必须触发冲突。
    [Fact]
    public void Compute_Differs_WhenReplySenderUserIdChanges()
    {
        var baseFp = RealtimeMessageFingerprint.Compute(
            42, "hello", replyToMessageId: "rm1", replyToSenderUserId: 100);
        var changed = RealtimeMessageFingerprint.Compute(
            42, "hello", replyToMessageId: "rm1", replyToSenderUserId: 200);
        Assert.NotEqual(baseFp, changed);
    }

    [Fact]
    public void Compute_Differs_WhenReplyPreviewChanges()
    {
        var baseFp = RealtimeMessageFingerprint.Compute(
            42, "hello", replyToMessageId: "rm1", replyToPreview: "p1");
        var changed = RealtimeMessageFingerprint.Compute(
            42, "hello", replyToMessageId: "rm1", replyToPreview: "p2");
        Assert.NotEqual(baseFp, changed);
    }

    [Fact]
    public void Compute_Differs_WhenForwardedFromSenderUserIdChanges()
    {
        var baseFp = RealtimeMessageFingerprint.Compute(
            42, "hello", forwardedFromMessageId: "fm1", forwardedFromSenderUserId: 100);
        var changed = RealtimeMessageFingerprint.Compute(
            42, "hello", forwardedFromMessageId: "fm1", forwardedFromSenderUserId: 200);
        Assert.NotEqual(baseFp, changed);
    }

    [Fact]
    public void Compute_Differs_WhenForwardedFromPreviewChanges()
    {
        var baseFp = RealtimeMessageFingerprint.Compute(
            42, "hello", forwardedFromMessageId: "fm1", forwardedFromPreview: "p1");
        var changed = RealtimeMessageFingerprint.Compute(
            42, "hello", forwardedFromMessageId: "fm1", forwardedFromPreview: "p2");
        Assert.NotEqual(baseFp, changed);
    }

    // P0-10：MatchesExisting 在覆盖字段不同时必须返回 false（冲突）。
    // existing* 参数表示 DB 行的实际数据；stored 行 replyToSenderUserId=200，
    // incoming replyToSenderUserId=100 → 重算后仍不等 → 冲突。
    [Fact]
    public void MatchesExisting_ReturnsFalse_WhenReplySenderDiffers()
    {
        var incoming = RealtimeMessageFingerprint.Compute(
            42, "hello", replyToMessageId: "rm1", replyToSenderUserId: 100);
        var storedDifferentSender = RealtimeMessageFingerprint.Compute(
            42, "hello", replyToMessageId: "rm1", replyToSenderUserId: 200);
        Assert.False(RealtimeMessageFingerprint.MatchesExisting(
            storedDifferentSender,
            42,
            "hello",
            Array.Empty<string>(),
            incoming,
            existingReplyToMessageId: "rm1",
            existingReplyToSenderUserId: 200));
    }
}
