using ChatApp.Realtime.Abstractions.Auth;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Auth;

/// <summary>
/// NoopRealtimeAuthReader 类是一个实现 IRealtimeAuthReader 接口的空操作类。
/// 该类主要用于在未配置真实的认证读取器时提供默认行为。它总是返回一个失败的验证结果，
/// 并且会记录一条警告日志来指示当前使用的是空操作认证读取器。
/// </summary>
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
