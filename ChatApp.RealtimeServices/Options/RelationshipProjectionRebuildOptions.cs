namespace ChatApp.RealtimeServices.Options;

public sealed class RelationshipProjectionRebuildOptions
{
    public const string SectionName = "RelationshipProjectionRebuild";
    public const string ApiKeyHeaderName = "X-Relationship-Projection-Key";

    public bool Enabled { get; init; }
    public string? ServerBaseAddress { get; init; }
    public string? ApiKey { get; init; }
    public int PageSize { get; init; } = 100;
    public int RequestTimeoutSeconds { get; init; } = 30;
    public int LeaseSeconds { get; init; } = 120;
    public int FailureRetrySeconds { get; init; } = 15;
    public int StablePollSeconds { get; init; } = 300;
    public int IdlePollMilliseconds { get; init; } = 1000;
}
