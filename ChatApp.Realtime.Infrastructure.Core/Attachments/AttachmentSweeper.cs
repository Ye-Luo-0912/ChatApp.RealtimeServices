using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Stores;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Core.Attachments;

/// <summary>
/// P1-3：未绑定附件过期清理器。把超过保留期、未绑定消息的
/// Ticketed/Uploaded/Scanning 附件标记为 Expired（带 state_version 条件更新），
/// 并在配置对象存储时删除物理对象。
/// </summary>
public sealed class AttachmentSweeper : IAttachmentSweeper
{
    private readonly IRealtimeAttachmentStore _store;
    private readonly IObjectStorage? _objectStorage;
    private readonly ILogger<AttachmentSweeper> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly TimeSpan _retention;

    public AttachmentSweeper(
        IRealtimeAttachmentStore store,
        ILogger<AttachmentSweeper> logger,
        TimeSpan? retention = null,
        IObjectStorage? objectStorage = null,
        TimeProvider? timeProvider = null)
    {
        _store = store;
        _logger = logger;
        _objectStorage = objectStorage;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _retention = retention ?? TimeSpan.FromDays(7);
    }

    public async Task<int> SweepAsync(CancellationToken ct = default)
    {
        var cutoffMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()
            - (long)_retention.TotalMilliseconds;
        var candidates = await _store
            .ListExpiryCandidatesAsync(cutoffMs, take: 200, ct)
            .ConfigureAwait(false);

        if (candidates.Count == 0)
            return 0;

        var expired = 0;
        foreach (var candidate in candidates)
        {
            var marked = await _store
                .MarkExpiredAsync(candidate.AttachmentId, candidate.StateVersion, ct)
                .ConfigureAwait(false);
            if (!marked)
                continue; // 已被并发转换（绑定/扫描），跳过。

            expired++;
            if (_objectStorage is not null)
            {
                try
                {
                    await _objectStorage
                        .DeleteAsync(candidate.ObjectKey, ct)
                        .ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // 对象删除失败不阻断状态标记；由后续 blob purge 兜底重试。
                    _logger.LogWarning(
                        ex,
                        "过期附件对象删除失败。附件={AttachmentId}；对象={ObjectKey}",
                        candidate.AttachmentId,
                        candidate.ObjectKey);
                }
            }
        }

        if (expired > 0)
        {
            _logger.LogInformation(
                "附件过期清理：标记过期={Expired}；候选={Candidate}",
                expired,
                candidates.Count);
        }
        return expired;
    }
}