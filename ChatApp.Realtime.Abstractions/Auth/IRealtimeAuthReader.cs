namespace ChatApp.Realtime.Abstractions.Auth;

public interface IRealtimeAuthReader
{
    Task<RealtimeAuthResult> ValidateAccessTokenAsync(
        string accessToken,
        string? deviceId = null,
        CancellationToken ct = default);
}
