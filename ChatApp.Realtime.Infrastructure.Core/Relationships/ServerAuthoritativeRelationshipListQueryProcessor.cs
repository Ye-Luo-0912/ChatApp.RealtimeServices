using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Infrastructure.Core.Relationships;

/// <summary>
/// Fail-closed read boundary until ChatApp.Server publishes an authoritative
/// relationship projection for Realtime consumers.
/// </summary>
public sealed class ServerAuthoritativeRelationshipListQueryProcessor : IRelationshipListQueryProcessor
{
    public static ServerAuthoritativeRelationshipListQueryProcessor Instance { get; } = new();

    private ServerAuthoritativeRelationshipListQueryProcessor()
    {
    }

    public Task<RelationshipListResult> ProcessAsync(
        RelationshipListQuery query,
        CancellationToken ct = default) =>
        Task.FromResult(RelationshipListResult.Failed(
            query.RequestId,
            "relationship_read_projection_unavailable",
            "关系列表请通过 ChatApp.Server HTTP API 查询；Realtime 权威投影尚未启用。"));
}
