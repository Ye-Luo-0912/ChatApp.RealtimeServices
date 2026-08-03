namespace ChatApp.Realtime.Abstractions.Relationships;

public interface IRelationshipListQueryProcessor
{
    Task<RelationshipListResult> ProcessAsync(
        RelationshipListQuery query,
        CancellationToken ct = default);
}

public interface IRelationshipListQueryConsumer
{
    IAsyncEnumerable<RelationshipListQueryEnvelope> ConsumeAsync(CancellationToken ct = default);
}
