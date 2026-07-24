using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Tests;

public sealed class ConversationIdTests
{
    [Theory]
    [InlineData(1002, 1001, "dm:1001:1002")]
    [InlineData(1001, 1002, "dm:1001:1002")]
    [InlineData(1, 9, "dm:1:9")]
    public void CreateDirect_NormalizesUserOrder(long a, long b, string expected)
    {
        Assert.Equal(expected, ConversationId.CreateDirect(a, b));
    }

    [Fact]
    public void CreateDirect_RejectsSelfChat()
    {
        Assert.Throws<ArgumentException>(() => ConversationId.CreateDirect(7, 7));
    }

    [Fact]
    public void TryParseDirect_RoundTrips()
    {
        var id = ConversationId.CreateDirect(42, 7);
        Assert.True(ConversationId.TryParseDirect(id, out var lo, out var hi));
        Assert.Equal(7, lo);
        Assert.Equal(42, hi);
    }

    [Theory]
    [InlineData("dm:2:2")]
    [InlineData("dm:10:1")]
    [InlineData("group:1")]
    [InlineData("")]
    public void TryParseDirect_RejectsInvalid(string value)
    {
        Assert.False(ConversationId.TryParseDirect(value, out _, out _));
    }

    [Fact]
    public void CreateGroup_ProducesValidGroupId()
    {
        var id = ConversationId.CreateGroup();
        Assert.True(ConversationId.IsGroup(id));
        Assert.False(ConversationId.IsDirect(id));
    }

    [Fact]
    public void CreatePreview_TruncatesWithoutExtraCopyBeyondLimit()
    {
        var content = new string('x', 300);
        var preview = ConversationId.CreatePreview(content);
        Assert.Equal(256, preview.Length);
        Assert.Equal(content[..256], preview);
    }
}
