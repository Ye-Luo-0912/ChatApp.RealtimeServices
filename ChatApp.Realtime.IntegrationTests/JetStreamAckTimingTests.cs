using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.RealtimeServices.Workers.Reliability;

namespace ChatApp.Realtime.IntegrationTests;

public sealed class JetStreamAckTimingTests
{
    [Fact]
    public void EffectiveAckWait_UsesFirstBackoff_WhenBackoffIsConfigured()
    {
        var options = new JetStreamOptions
        {
            Consumer = new JetStreamConsumerOptions
            {
                AckWaitSeconds = 60,
                BackoffSeconds = [1, 5, 30]
            }
        };

        var effectiveAckWait = JetStreamAckTiming.GetEffectiveAckWait(options);

        Assert.Equal(TimeSpan.FromSeconds(1), effectiveAckWait);
    }

    [Fact]
    public void EffectiveAckWait_UsesAckWait_WhenBackoffIsEmpty()
    {
        var options = new JetStreamOptions
        {
            Consumer = new JetStreamConsumerOptions
            {
                AckWaitSeconds = 60,
                BackoffSeconds = []
            }
        };

        var effectiveAckWait = JetStreamAckTiming.GetEffectiveAckWait(options);

        Assert.Equal(TimeSpan.FromSeconds(60), effectiveAckWait);
    }

    [Fact]
    public void ProgressAckInterval_RefreshesOneSecondLeaseBeforeExpiry()
    {
        var effectiveAckWait = TimeSpan.FromSeconds(1);

        var interval = JetStreamAckTiming.GetProgressAckInterval(effectiveAckWait);

        Assert.Equal(TimeSpan.FromMilliseconds(500), interval);
        Assert.True(interval < effectiveAckWait);
    }

    [Fact]
    public void EffectiveAckWait_IsDisabled_WhenJetStreamIsNotConfigured()
    {
        Assert.Equal(
            TimeSpan.Zero,
            JetStreamAckTiming.GetEffectiveAckWait(options: null));
    }
}
