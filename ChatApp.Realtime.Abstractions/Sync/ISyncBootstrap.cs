namespace ChatApp.Realtime.Abstractions.Sync;

public interface ISyncBootstrapQueryConsumer
{
    IAsyncEnumerable<SyncBootstrapQueryEnvelope> ConsumeAsync(CancellationToken ct = default);
}

public interface ISyncBootstrapQueryProcessor
{
    Task<SyncBootstrapPage> ProcessAsync(SyncBootstrapQuery query, CancellationToken ct = default);
}
