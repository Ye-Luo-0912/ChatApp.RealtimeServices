using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Workers;

/// <summary>
/// 账号清理可续跑 Saga Worker。
/// <para>
/// LongTerm-2：取代原内联一次性清理。从 <c>account_cleanup_jobs</c> 表轮询 pending 作业，
/// 按 phase（attachments → metadata → completed）分批推进清理。
/// </para>
/// <para>
/// 内存有界：每批最多 <see cref="RealtimeOptions.AccountCleanupBatchSize"/>（默认 200）个对象。
/// 断点续跑：通过 cursor（最后处理的 attachment_id）记录进度，崩溃后从 cursor 续跑。
/// 事务有界：每批一次 purge Outbox + 一次 DELETE，不产生数百个串行小事务。
/// 失败有上限：超过 <see cref="RealtimeOptions.AccountCleanupMaxRetries"/> 标记 failed。
/// </para>
/// <para>
/// 六-1：Job lease。认领时写入 claim_token / locked_until_ms；每批处理后续租；
/// lease 过期后 running 作业可被其他实例重新认领，崩溃不再永久卡死。
/// 六-2：EventId 使用稳定哈希（SHA256），跨进程/重启保持幂等。
/// 六-3：attachments 批次三项操作（Outbox + DELETE + cursor）在同一事务中原子完成。
/// 六-4：metadata 阶段额外清理 reactions / membership_periods。
/// </para>
/// </summary>
public sealed class AccountCleanupWorker : BackgroundService
{
    private const string WorkerName = nameof(AccountCleanupWorker);

    private readonly IAccountCleanupJobStore _jobStore;
    private readonly IRealtimeAttachmentStore _attachmentStore;
    private readonly IRealtimeMessageStore _messageStore;
    private readonly IRealtimeDeviceSyncCursorStore _deviceSyncCursorStore;
    private readonly IRealtimeReactionStore _reactionStore;
    private readonly IMembershipPeriodStore _membershipPeriodStore;
    private readonly IUserDeletionTombstoneStore _tombstoneStore;
    private readonly IRealtimeOutboxSignal _outboxSignal;
    private readonly RealtimeOptions _options;
    private readonly ILogger<AccountCleanupWorker> _logger;

    public AccountCleanupWorker(
        IAccountCleanupJobStore jobStore,
        IRealtimeAttachmentStore attachmentStore,
        IRealtimeMessageStore messageStore,
        IRealtimeDeviceSyncCursorStore deviceSyncCursorStore,
        IRealtimeReactionStore reactionStore,
        IMembershipPeriodStore membershipPeriodStore,
        IUserDeletionTombstoneStore tombstoneStore,
        IRealtimeOutboxSignal outboxSignal,
        IOptions<RealtimeOptions> options,
        ILogger<AccountCleanupWorker> logger)
    {
        _jobStore = jobStore;
        _attachmentStore = attachmentStore;
        _messageStore = messageStore;
        _deviceSyncCursorStore = deviceSyncCursorStore;
        _reactionStore = reactionStore;
        _membershipPeriodStore = membershipPeriodStore;
        _tombstoneStore = tombstoneStore;
        _outboxSignal = outboxSignal;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "账号清理 Saga Worker 已启动。批次大小={BatchSize}；最大重试={MaxRetries}；轮询间隔={PollMs}ms；租约={LeaseMs}ms",
            _options.AccountCleanupBatchSize,
            _options.AccountCleanupMaxRetries,
            _options.AccountCleanupPollIntervalMs,
            _options.AccountCleanupLeaseMs);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await ProcessOneCycleAsync(stoppingToken).ConfigureAwait(false);
                if (processed == 0)
                {
                    await Task.Delay(_options.AccountCleanupPollIntervalMs, stoppingToken)
                        .ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "账号清理 Saga 周期异常，将在间隔后重试。");
                await Task.Delay(_options.AccountCleanupPollIntervalMs, stoppingToken)
                    .ConfigureAwait(false);
            }
        }

        _logger.LogInformation("账号清理 Saga Worker 已停止。");
    }

    private async Task<int> ProcessOneCycleAsync(CancellationToken ct)
    {
        var maxBatches = Math.Max(1, _options.AccountCleanupMaxBatchesPerCycle);
        var leaseDuration = TimeSpan.FromMilliseconds(Math.Max(1_000, _options.AccountCleanupLeaseMs));
        var processed = 0;
        for (var i = 0; i < maxBatches; i++)
        {
            var job = await _jobStore
                .GetNextPendingAsync(_options.InstanceId, leaseDuration, ct)
                .ConfigureAwait(false);
            if (job is null)
                return processed;

            await ProcessJobAsync(job, leaseDuration, ct).ConfigureAwait(false);
            processed++;
        }
        return processed;
    }

    private async Task ProcessJobAsync(AccountCleanupJob job, TimeSpan leaseDuration, CancellationToken ct)
    {
        try
        {
            switch (job.Phase)
            {
                case AccountCleanupJob.PhaseAttachments:
                    await ProcessAttachmentsPhaseAsync(job, leaseDuration, ct).ConfigureAwait(false);
                    break;
                case AccountCleanupJob.PhaseMetadata:
                    await ProcessMetadataPhaseAsync(job, leaseDuration, ct).ConfigureAwait(false);
                    break;
                case AccountCleanupJob.PhaseCompleted:
                    await ProcessCompletedPhaseAsync(job, ct).ConfigureAwait(false);
                    break;
                default:
                    _logger.LogWarning(
                        "账号清理 Saga 遇到未知 phase，标记失败。用户={UserId}；阶段={Phase}",
                        job.UserId,
                        job.Phase);
                    await _jobStore
                        .RecordFailureAsync(
                            job.UserId, job.Phase, job.ClaimToken!, _options.AccountCleanupMaxRetries, ct)
                        .ConfigureAwait(false);
                    break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "账号清理 Saga phase 失败。用户={UserId}；阶段={Phase}",
                job.UserId,
                job.Phase);
            await _jobStore
                .RecordFailureAsync(
                    job.UserId, job.Phase, job.ClaimToken!, _options.AccountCleanupMaxRetries, ct)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// attachments 阶段：分批列出附件元数据 → 写 purge Outbox → 删除本批元数据 → 更新游标。
    /// 六-2：EventId 使用稳定哈希（SHA256）保证跨进程幂等。
    /// 六-3：Outbox 入队 + 附件删除 + cursor 更新在同一事务中原子完成。
    /// 六-1：每批处理后续租 lease，过期则停止处理。
    /// </summary>
    private async Task ProcessAttachmentsPhaseAsync(AccountCleanupJob job, TimeSpan leaseDuration, CancellationToken ct)
    {
        var batchSize = Math.Max(1, _options.AccountCleanupBatchSize);

        // 通过 cursor（attachment_id）分页读取，内存有界。
        var records = await _attachmentStore
            .ListForUserExportAsync(job.UserId, job.Cursor, batchSize, ct)
            .ConfigureAwait(false);

        if (records.Count == 0)
        {
            // 附件全部清理完毕，进入 metadata 阶段。
            _logger.LogInformation(
                "账号清理 attachments 阶段完成。用户={UserId}",
                job.UserId);
            await _jobStore
                .CompletePhaseAsync(job.UserId, AccountCleanupJob.PhaseAttachments, job.ClaimToken!, ct)
                .ConfigureAwait(false);
            return;
        }

        var objectKeys = records.Select(r => r.ObjectKey).ToArray();
        var attachmentIds = records.Select(r => r.AttachmentId).ToArray();
        var lastAttachmentId = records[^1].AttachmentId;

        // 六-2：purge EventId 基于 userId + 起始 cursor，使用稳定哈希（SHA256）保证幂等。
        var cursorForEventId = job.Cursor ?? "start";
        var purgeEventId = AttachmentEventIdFactory.CreateAttachmentBlobsPurgeEventId(
            $"cleanup:{job.UserId}",
            cursorForEventId);

        var purgeEvent = new RealtimeEvent
        {
            EventId = purgeEventId,
            Type = RealtimeEventType.AttachmentBlobsPurge,
            TargetUserId = job.UserId,
            OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            PayloadJson = JsonSerializer.Serialize(
                new AttachmentBlobsPurgePayload
                {
                    UserId = job.UserId,
                    ObjectKeys = objectKeys,
                    // Saga 模式下每批为独立 chunk，ChunkCount=1 表示无总片数信息。
                    ChunkIndex = 0,
                    ChunkCount = 1
                },
                RealtimeJsonSerializerContext.Default.AttachmentBlobsPurgePayload),
        };

        // 六-3：在同一事务中原子完成 Outbox 入队 + 附件删除 + cursor 更新。
        var advanced = await _jobStore
            .ProcessAttachmentsBatchAtomicAsync(
                job.UserId,
                job.ClaimToken!,
                lastAttachmentId,
                attachmentIds,
                purgeEvent,
                ct)
            .ConfigureAwait(false);

        if (!advanced)
        {
            // lease 已失效（被抢占或过期），停止处理本作业。
            _logger.LogWarning(
                "账号清理 attachments 批次 lease 失效，停止处理。用户={UserId}",
                job.UserId);
            return;
        }

        // 通知 Outbox Publisher 尽快发布 purge 事件。
        _outboxSignal.Notify();

        // 六-1：每批处理后续租 lease。
        var renewed = await _jobStore
            .RenewLeaseAsync(job.UserId, job.Phase, job.ClaimToken!, leaseDuration, ct)
            .ConfigureAwait(false);
        if (!renewed)
        {
            _logger.LogWarning(
                "账号清理 attachments 批次续租失败，停止处理。用户={UserId}",
                job.UserId);
            return;
        }

        _logger.LogDebug(
            "账号清理 attachments 批次完成。用户={UserId}；本批={Count}；游标={Cursor}",
            job.UserId,
            records.Count,
            lastAttachmentId);
    }

    /// <summary>
    /// metadata 阶段：清理设备游标 + 消息 + 反应 + membership periods，写 AccountCleanupCompleted Outbox。
    /// 六-4：扩展清理范围至 reactions / membership_periods / mentioned_user_ids / group_mutation_requests。
    /// 消息删除内部已分批（DELETE LIMIT 循环），mentioned_user_ids 与 group_mutation_requests 在其中清理。
    /// </summary>
    private async Task ProcessMetadataPhaseAsync(AccountCleanupJob job, TimeSpan leaseDuration, CancellationToken ct)
    {
        // 清理设备同步游标。
        var cursorDeleted = await _deviceSyncCursorStore
            .DeleteByUserAsync(job.UserId, ct)
            .ConfigureAwait(false);

        // 六-4：清理该用户的全部反应记录。
        var reactionsDeleted = await _reactionStore
            .DeleteByUserAsync(job.UserId, ct)
            .ConfigureAwait(false);

        // 六-4：清理该用户的全部 membership periods。
        var membershipPeriodsDeleted = await _membershipPeriodStore
            .DeleteByUserAsync(job.UserId, ct)
            .ConfigureAwait(false);

        // 清理该用户全部消息（内部已分批，直到无剩余行）。
        // 六-4：内部同时清理 mentioned_user_ids（array_remove）与 group_mutation_requests。
        var deleted = await _messageStore
            .DeleteByUserAsync(job.UserId, batchSize: 1000, ct)
            .ConfigureAwait(false);

        // 写 AccountCleanupCompleted Outbox（幂等 EventId）。
        await _messageStore
            .EnqueueEventAsync(
                new RealtimeEvent
                {
                    EventId = $"cleanup-done:{job.UserId}",
                    Type = RealtimeEventType.AccountCleanupCompleted,
                    TargetUserId = job.UserId,
                    OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    PayloadJson = null,
                },
                ct)
            .ConfigureAwait(false);

        _outboxSignal.Notify();

        _logger.LogInformation(
            "账号清理 metadata 阶段完成。用户={UserId}；删除消息={Deleted}；设备游标={Cursors}；反应={Reactions}；membership={Membership}",
            job.UserId,
            deleted,
            cursorDeleted,
            reactionsDeleted,
            membershipPeriodsDeleted);

        // 六-1：续租 lease 后完成 phase。
        var renewed = await _jobStore
            .RenewLeaseAsync(job.UserId, job.Phase, job.ClaimToken!, leaseDuration, ct)
            .ConfigureAwait(false);
        if (!renewed)
        {
            _logger.LogWarning(
                "账号清理 metadata 阶段续租失败，作业将被其他实例重新认领。用户={UserId}",
                job.UserId);
            return;
        }

        // 进入 completed 阶段。
        await _jobStore
            .CompletePhaseAsync(job.UserId, AccountCleanupJob.PhaseMetadata, job.ClaimToken!, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// completed 阶段：将 tombstone 升级为 Deleted，标记作业完成。
    /// </summary>
    private async Task ProcessCompletedPhaseAsync(AccountCleanupJob job, CancellationToken ct)
    {
        await _tombstoneStore
            .RecordDeletionCompletedAsync(job.UserId, ct)
            .ConfigureAwait(false);

        // completed phase 标记完成（无下一阶段）。
        await _jobStore
            .UpdateProgressAsync(
                job.UserId,
                AccountCleanupJob.PhaseCompleted,
                null,
                AccountCleanupJob.StatusCompleted,
                job.ClaimToken!,
                ct)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "账号清理 Saga 全部完成。用户={UserId}",
            job.UserId);
    }
}
