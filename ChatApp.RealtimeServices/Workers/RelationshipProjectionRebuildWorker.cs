using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Options;
using System.Diagnostics;

namespace ChatApp.RealtimeServices.Workers;

internal sealed class RelationshipProjectionRebuildWorker(
    IRelationshipProjectionRebuildStateStore stateStore,
    IRelationshipProjectionStore projectionStore,
    IRelationshipProjectionSnapshotSource snapshotSource,
    IOptions<RelationshipProjectionRebuildOptions> rebuildOptions,
    IOptions<RealtimeOptions> realtimeOptions,
    RealtimeMetrics metrics,
    ILogger<RelationshipProjectionRebuildWorker> logger) : BackgroundService
{
    private readonly RelationshipProjectionRebuildOptions _options = rebuildOptions.Value;
    private readonly string _leaseOwner = realtimeOptions.Value.InstanceId;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("关系投影快照 Rebuilder 已启用。");
        while (!stoppingToken.IsCancellationRequested)
        {
            RelationshipProjectionRebuildLease? lease = null;
            try
            {
                lease = await stateStore.TryAcquireAsync(
                        _leaseOwner,
                        LeaseDuration,
                        stoppingToken)
                    .ConfigureAwait(false);
                if (lease is null)
                {
                    await Task.Delay(_options.IdlePollMilliseconds, stoppingToken)
                        .ConfigureAwait(false);
                    continue;
                }

                metrics.SetRelationshipProjectionRebuildStablePasses(lease.StablePasses);
                metrics.SetRelationshipProjectionRebuildActive(true);
                try
                {
                    await RunLeaseAsync(lease, stoppingToken).ConfigureAwait(false);
                }
                finally
                {
                    metrics.SetRelationshipProjectionRebuildActive(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (RelationshipProjectionRebuildLeaseLostException ex)
            {
                metrics.RecordRelationshipProjectionRebuildLeaseLost(ex.Stage);
                logger.LogWarning(
                    "关系投影快照 Rebuilder 已失去租约；停止当前扫描。阶段={Stage}",
                    ex.Stage);
            }
            catch (Exception ex)
            {
                var errorCode = ClassifyError(ex);
                metrics.RecordRelationshipProjectionRebuildFailure(errorCode);
                logger.LogWarning(
                    ex,
                    "关系投影快照 Rebuilder 本轮失败；将从已提交 cursor 续跑。错误={ErrorCode}",
                    errorCode);
                if (lease is not null)
                {
                    try
                    {
                        await stateStore.ReleaseFailureAsync(
                                lease,
                                errorCode,
                                FailureRetry,
                                CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                    catch (Exception releaseError)
                    {
                        logger.LogWarning(releaseError, "释放关系投影 Rebuilder 租约失败；等待租约自然过期。");
                    }
                }
            }
        }
    }

    internal async Task<RelationshipProjectionRebuildPassResult> RunLeaseAsync(
        RelationshipProjectionRebuildLease initialLease,
        CancellationToken ct)
    {
        var lease = initialLease;
        var passStartedAt = Stopwatch.GetTimestamp();
        while (true)
        {
            var page = await snapshotSource.ListStreamsAsync(
                    lease.AfterOwnerUserId,
                    lease.AfterListType,
                    _options.PageSize,
                    ct)
                .ConfigureAwait(false);
            ValidatePage(page, lease);

            var pageChanged = false;
            foreach (var stream in page.Items)
            {
                var streamStartedAt = Stopwatch.GetTimestamp();
                try
                {
                    if (!await stateStore.RenewAsync(lease, LeaseDuration, ct).ConfigureAwait(false))
                        throw new RelationshipProjectionRebuildLeaseLostException("renew");

                    var snapshot = await snapshotSource.ReadStreamAsync(
                            stream.OwnerUserId,
                            stream.ListType,
                            ct)
                        .ConfigureAwait(false);
                    ValidateSnapshot(stream, snapshot);
                    var result = await projectionStore.ApplySnapshotAsync(snapshot, ct)
                        .ConfigureAwait(false);
                    pageChanged |= result == RelationshipProjectionSnapshotApplyResult.Applied;
                    metrics.RecordRelationshipProjectionRebuildStream(
                        result == RelationshipProjectionSnapshotApplyResult.Applied
                            ? "applied"
                            : "verified",
                        Stopwatch.GetElapsedTime(streamStartedAt));
                }
                catch
                {
                    metrics.RecordRelationshipProjectionRebuildStream(
                        "failed",
                        Stopwatch.GetElapsedTime(streamStartedAt));
                    throw;
                }
            }

            if (!page.HasMore)
            {
                var completed = await stateStore.CompletePassAsync(
                        lease,
                        pageChanged,
                        StablePollInterval,
                        ct)
                    .ConfigureAwait(false)
                    ?? throw new RelationshipProjectionRebuildLeaseLostException("complete-pass");
                metrics.RecordRelationshipProjectionRebuildPass(
                    completed.StablePasses,
                    Stopwatch.GetElapsedTime(passStartedAt));
                return completed;
            }

            var nextOwner = page.NextOwnerUserId!.Value;
            var nextList = page.NextListType!.Value;
            if (!await stateStore.CommitPageAsync(
                    lease,
                    nextOwner,
                    nextList,
                    pageChanged,
                    LeaseDuration,
                    ct)
                .ConfigureAwait(false))
            {
                throw new RelationshipProjectionRebuildLeaseLostException("commit-page");
            }

            lease = lease with
            {
                AfterOwnerUserId = nextOwner,
                AfterListType = nextList,
                PassChanged = lease.PassChanged || pageChanged
            };
        }
    }

    private static void ValidatePage(
        RelationshipProjectionStreamPage page,
        RelationshipProjectionRebuildLease lease)
    {
        ArgumentNullException.ThrowIfNull(page);
        if (page.Items is null)
            throw new InvalidDataException("Relationship projection stream page has null items.");
        RelationshipProjectionStreamDescriptor? previous = null;
        foreach (var item in page.Items)
        {
            if (item.OwnerUserId <= 0 || !Enum.IsDefined(item.ListType) || item.Version < 0)
                throw new InvalidDataException("Relationship projection stream descriptor is invalid.");
            if (previous is not null && Compare(previous, item) >= 0)
                throw new InvalidDataException("Relationship projection stream page is not strictly ordered.");
            if (lease.AfterOwnerUserId is { } owner
                && lease.AfterListType is { } listType
                && Compare(owner, listType, item) >= 0)
            {
                throw new InvalidDataException("Relationship projection stream page did not advance its cursor.");
            }
            previous = item;
        }

        if (!page.HasMore)
        {
            if (page.NextOwnerUserId is not null || page.NextListType is not null)
                throw new InvalidDataException("Terminal relationship projection page has a continuation cursor.");
            return;
        }

        if (page.Items.Count == 0
            || page.NextOwnerUserId is null
            || page.NextListType is null
            || previous is null
            || previous.OwnerUserId != page.NextOwnerUserId
            || previous.ListType != page.NextListType)
        {
            throw new InvalidDataException("Relationship projection continuation cursor is missing or inconsistent.");
        }
    }

    private static void ValidateSnapshot(
        RelationshipProjectionStreamDescriptor descriptor,
        RelationshipProjectionStreamSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot.OwnerUserId != descriptor.OwnerUserId
            || snapshot.ListType != descriptor.ListType
            || snapshot.Version < descriptor.Version)
        {
            throw new InvalidDataException("Relationship projection snapshot does not match its stream descriptor.");
        }
    }

    private static int Compare(
        RelationshipProjectionStreamDescriptor left,
        RelationshipProjectionStreamDescriptor right) =>
        Compare(left.OwnerUserId, left.ListType, right);

    private static int Compare(
        long ownerUserId,
        RelationshipProjectionListType listType,
        RelationshipProjectionStreamDescriptor right)
    {
        var ownerComparison = ownerUserId.CompareTo(right.OwnerUserId);
        return ownerComparison != 0
            ? ownerComparison
            : ((byte)listType).CompareTo((byte)right.ListType);
    }

    private static string ClassifyError(Exception exception) => exception switch
    {
        RelationshipProjectionSourceException source =>
            $"source_http_{(int)(source.StatusCode ?? System.Net.HttpStatusCode.InternalServerError)}",
        RelationshipProjectionSnapshotVersionMismatchException => "snapshot_version_mismatch",
        InvalidDataException => "source_contract_invalid",
        HttpRequestException => "source_transport_failed",
        TaskCanceledException => "source_timeout",
        TimeoutException => "source_timeout",
        _ => "rebuild_failed"
    };

    private TimeSpan LeaseDuration => TimeSpan.FromSeconds(_options.LeaseSeconds);
    private TimeSpan FailureRetry => TimeSpan.FromSeconds(_options.FailureRetrySeconds);
    private TimeSpan StablePollInterval => TimeSpan.FromSeconds(_options.StablePollSeconds);
}

internal sealed class RelationshipProjectionRebuildLeaseLostException(string stage)
    : Exception($"Relationship projection rebuild lease was lost during {stage}.")
{
    public string Stage { get; } = stage;
}
