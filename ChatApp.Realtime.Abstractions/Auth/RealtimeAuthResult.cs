namespace ChatApp.Realtime.Abstractions.Auth;

public sealed class RealtimeAuthResult
{
    public required bool Succeeded { get; init; }
    public long? UserId { get; init; }
    public string? SessionId { get; init; }
    public string? UserName { get; init; }
    public string? DeviceId { get; init; }
    public IReadOnlyList<string> Roles { get; init; } = [];
    public string? ErrorMessage { get; init; }

    public static RealtimeAuthResult Success(
        long userId,
        string? sessionId,
        string? userName,
        string? deviceId,
        IReadOnlyList<string>? roles = null)
    {
        return new RealtimeAuthResult
        {
            Succeeded = true,
            UserId = userId,
            SessionId = sessionId,
            UserName = userName,
            DeviceId = deviceId,
            Roles = roles ?? []
        };
    }

    public static RealtimeAuthResult Fail(string message)
    {
        return new RealtimeAuthResult
        {
            Succeeded = false,
            ErrorMessage = message
        };
    }
}
