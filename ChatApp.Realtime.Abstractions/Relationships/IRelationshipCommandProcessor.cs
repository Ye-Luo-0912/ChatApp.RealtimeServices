namespace ChatApp.Realtime.Abstractions.Relationships;

public interface IRelationshipCommandProcessor
{
    Task<RelationshipCommandResult> ProcessAsync(
        RelationshipCommand command,
        CancellationToken ct = default);
}

public interface IRelationshipCommandConsumer
{
    IAsyncEnumerable<RelationshipCommandEnvelope> ConsumeAsync(CancellationToken ct = default);
}
