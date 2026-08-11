using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 未绑定附件过期清理后台服务。周期调用 <see cref="IAttachmentSweeper.SweepAsync"/>，
/// 把超过保留期、未绑定消息的附件标记为 Expired。
/// <para>
/// 启用条件：<see cref="AttachmentSweepOptions.Enabled"/> 为 true 时每个周期执行一轮清理；
/// 单个周期异常不阻断，下个间隔自动重试。对象存储实现未注入时仅标记状态，物理删除由后续兜底。
/// </para>
/// </summary>
public sealed class AttachmentSweepWorker : BackgroundService
{
    private readonly IAttachmentSweeper _sweeper;
    private readonly AttachmentSweepOptions _options;
    private readonly TimeSpan _interval;
    private readonly ILogger<AttachmentSweepWorker> _logger;

    public AttachmentSweepWorker(
        IAttachmentSweeper sweeper,
        IOptions<AttachmentSweepOptions> options,
        ILogger<AttachmentSweepWorker> logger)
    {
        _sweeper = sweeper;
        _options = options.Value;
        _interval = TimeSpan.FromMilliseconds(Math.Max(1_000, _options.IntervalMs));
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_interval);
        do
        {
            try
            {
                if (_options.Enabled)
                {
                    await _sweeper.SweepAsync(stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "附件过期清理周期失败，将在下个间隔重试。");
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
    }
}