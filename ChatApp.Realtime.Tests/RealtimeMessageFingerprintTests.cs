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
}
