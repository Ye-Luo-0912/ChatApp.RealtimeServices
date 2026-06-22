namespace ChatApp.RealtimeServices.Options;

public sealed class RealtimeConnectionOptions
{
    public required string Garnet { get; init; }
    public string? RealtimeDatabase { get; init; }
}
