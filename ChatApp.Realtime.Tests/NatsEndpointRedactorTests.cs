using ChatApp.Realtime.Abstractions.Diagnostics;
using Xunit;

namespace ChatApp.Realtime.Tests;

public sealed class NatsEndpointRedactorTests
{
    [Theory]
    [InlineData("nats://127.0.0.1:4222", "nats://127.0.0.1:4222")]
    [InlineData("nats://user:s3cret@nats.example:4222", "nats://***@nats.example:4222")]
    [InlineData("nats://token@host", "nats://***@host")]
    [InlineData("", "(unset)")]
    [InlineData("not-a-url", "(invalid)")]
    public void ForLog_RedactsUserInfo(string input, string expected)
    {
        Assert.Equal(expected, NatsEndpointRedactor.ForLog(input));
    }
}
