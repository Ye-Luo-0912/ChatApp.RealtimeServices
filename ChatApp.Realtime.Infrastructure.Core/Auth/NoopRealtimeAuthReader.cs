using ChatApp.Realtime.Abstractions.Auth;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Auth;

public sealed class NoopRealtimeAuthReader : IRealtimeAuthReader
{
    private readonly ILogger<NoopRealtimeAuthReader> _logger;

    public NoopRealtimeAuthReader(ILogger<NoopRealtimeAuthReader> logger)
    {
        _logger = logger;
    }

    public Task<RealtimeAuthResult> ValidateAccessTokenAsync(
        string accessToken,
        string? deviceId = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        _logger.LogWarning("尚未配置真实实时认证读取器。设备编号={DeviceId}", deviceId);
        return Task.FromResult(RealtimeAuthResult.Fail("尚未配置真实实时认证读取器。"));
    }
}
