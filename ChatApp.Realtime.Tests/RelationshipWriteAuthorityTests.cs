using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.DependencyInjection;
using ChatApp.Realtime.Infrastructure.Core.Relationships;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using Microsoft.Extensions.DependencyInjection;

namespace ChatApp.Realtime.Tests;

public sealed class RelationshipWriteAuthorityTests
{
    [Fact]
    public async Task DefaultRegistration_ResolvesFailClosedServerAuthorityGate()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRealtimeInfrastructureCore();

        await using var provider = services.BuildServiceProvider();
        var processor = provider.GetRequiredService<IRelationshipCommandProcessor>();

        var result = await processor.ProcessAsync(new RelationshipCommand
        {
            RequestId = "authority-test",
            ActorUserId = 10,
            Operation = RelationshipOperation.BlockUser,
            TargetUserId = 20,
        });

        Assert.Same(ServerAuthoritativeRelationshipCommandProcessor.Instance, processor);
        Assert.False(result.Succeeded);
        Assert.Equal("authority-test", result.RequestId);
        Assert.Equal("relationship_write_moved_to_server", result.ErrorCode);
        Assert.Contains("ChatApp.Server HTTP API", result.ErrorMessage, StringComparison.Ordinal);
        Assert.Null(result.RetryAfterMs);
        Assert.Null(result.QueueKind);
    }

    [Fact]
    public async Task Gate_AppliesToEveryRelationshipMutation()
    {
        foreach (var operation in Enum.GetValues<RelationshipOperation>())
        {
            var result = await ServerAuthoritativeRelationshipCommandProcessor.Instance.ProcessAsync(
                new RelationshipCommand
                {
                    RequestId = $"authority-{operation}",
                    ActorUserId = 10,
                    Operation = operation,
                    TargetUserId = 20,
                    RequestIdToRespond = "friend-request",
                });

            Assert.False(result.Succeeded);
            Assert.Equal("relationship_write_moved_to_server", result.ErrorCode);
        }
    }

    [Fact]
    public async Task DefaultRegistration_RejectsLegacyRelationshipListReads()
    {
        var services = new ServiceCollection();
        services.AddRealtimeInfrastructureCore();

        await using var provider = services.BuildServiceProvider();
        var processor = provider.GetRequiredService<IRelationshipListQueryProcessor>();

        var result = await processor.ProcessAsync(new RelationshipListQuery
        {
            RequestId = "relationship-list-authority",
            ActorUserId = 10,
            ListType = RelationshipListType.Friends,
        });

        Assert.Same(ServerAuthoritativeRelationshipListQueryProcessor.Instance, processor);
        Assert.False(result.Succeeded);
        Assert.Equal("relationship_read_projection_unavailable", result.ErrorCode);
        Assert.Contains("ChatApp.Server HTTP API", result.ErrorMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DefaultSyncRegistration_RejectsExplicitRelationshipWatermarks()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRealtimeInfrastructureCore();

        await using var provider = services.BuildServiceProvider();
        var processor = provider.GetRequiredService<ISyncBootstrapQueryProcessor>();

        var result = await processor.ProcessAsync(new SyncBootstrapQuery
        {
            RequestId = "relationship-sync-authority",
            UserId = 10,
            RelationshipWatermarks =
            [
                new RelationshipSyncWatermark
                {
                    ListType = RelationshipListType.Friends,
                    AfterSequence = 1,
                },
            ],
        });

        Assert.False(result.Succeeded);
        Assert.Equal("relationship_sync_projection_unavailable", result.ErrorCode);
        Assert.Contains("ChatApp.Server HTTP API", result.ErrorMessage, StringComparison.Ordinal);
        Assert.Null(result.RelationshipCatchUps);
    }

    [Fact]
    public async Task DefaultRegistration_DoesNotExposeRelationshipProjectionOpsData()
    {
        var services = new ServiceCollection();
        services.AddRealtimeInfrastructureCore();

        await using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IRelationshipProjectionOpsQueryStore>();

        Assert.Same(UnavailableRelationshipProjectionOpsQueryStore.Instance, store);
        Assert.False((await store.GetStatusAsync()).Available);
        Assert.False((await store.ListStreamsAsync(null, null, 100)).Available);
    }
}
