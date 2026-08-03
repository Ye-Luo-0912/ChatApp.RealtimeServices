using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;

namespace ChatApp.Realtime.Tests;

public sealed class ContractCompatibilityTests
{
    [Fact]
    public void Metadata_PackageVersion_Is2_0_0()
    {
        Assert.Equal("2.0.0", RealtimeContractMetadata.PackageVersion);
    }

    [Fact]
    public void Metadata_ProtocolVersion_MatchesCurrent()
    {
        Assert.Equal(RealtimeProtocolVersions.Current, RealtimeContractMetadata.ProtocolVersion);
    }

    [Fact]
    public void Metadata_MinSupportedProtocolVersion_MatchesMinSupported()
    {
        Assert.Equal(RealtimeProtocolVersions.MinSupported, RealtimeContractMetadata.MinSupportedProtocolVersion);
    }

    [Fact]
    public void RealtimeEvent_RoundTripsKeyFields()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        var evt = new RealtimeEvent
        {
            EventId = "evt-1",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 42,
            ProtocolVersion = RealtimeProtocolVersions.Current,
            AudienceVersion = 7,
            MinProtocolVersion = RealtimeProtocolVersions.V1
        };

        var json = JsonSerializer.Serialize(evt, options);
        var restored = JsonSerializer.Deserialize<RealtimeEvent>(json, options);

        Assert.NotNull(restored);
        Assert.Equal(evt.EventId, restored.EventId);
        Assert.Equal(evt.Type, restored.Type);
        Assert.Equal(evt.TargetUserId, restored.TargetUserId);
        Assert.Equal(evt.ProtocolVersion, restored.ProtocolVersion);
        Assert.Equal(evt.AudienceVersion, restored.AudienceVersion);
        Assert.Equal(evt.MinProtocolVersion, restored.MinProtocolVersion);
    }

    [Fact]
    public void RealtimeEvent_HasRequiredContractProperties()
    {
        Assert.NotNull(typeof(RealtimeEvent).GetProperty(nameof(RealtimeEvent.AudienceVersion)));
        Assert.NotNull(typeof(RealtimeEvent).GetProperty(nameof(RealtimeEvent.MinProtocolVersion)));
        Assert.NotNull(typeof(RealtimeEvent).GetProperty(nameof(RealtimeEvent.ProtocolVersion)));
    }
}