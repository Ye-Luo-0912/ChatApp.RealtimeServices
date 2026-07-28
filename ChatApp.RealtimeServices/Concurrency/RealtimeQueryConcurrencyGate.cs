using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Concurrency;

/// <summary>
/// 查询 Worker 数据库并发预算，按三类拆分池子避免重型读饿死低延迟交互：
/// - Read：History / Sync / ConversationList（重读，高吞吐）
/// - Interactive：MarkRead / SetPrefs（低延迟，快速响应）
/// - Mutation：Group / Edit / Recall / Reaction（变更类，中等延迟）
/// </summary>
public sealed class RealtimeQueryConcurrencyGate
{
    private readonly SemaphoreSlim _readPool;
    private readonly SemaphoreSlim _interactivePool;
    private readonly SemaphoreSlim _mutationPool;

    public RealtimeQueryConcurrencyGate(IOptions<RealtimeOptions> options)
    {
        var opts = options.Value;
        var readPermits = Math.Max(1, opts.ReadQueryConcurrency);
        var interactivePermits = Math.Max(1, opts.InteractiveQueryConcurrency);
        var mutationPermits = Math.Max(1, opts.MutationQueryConcurrency);
        _readPool = new SemaphoreSlim(readPermits, readPermits);
        _interactivePool = new SemaphoreSlim(interactivePermits, interactivePermits);
        _mutationPool = new SemaphoreSlim(mutationPermits, mutationPermits);
        ReadPermits = readPermits;
        InteractivePermits = interactivePermits;
        MutationPermits = mutationPermits;
    }

    public int ReadPermits { get; }
    public int InteractivePermits { get; }
    public int MutationPermits { get; }

    /// <summary>旧 API 兼容：默认使用 Read 池。</summary>
    public Task WaitAsync(CancellationToken ct) => _readPool.WaitAsync(ct);

    /// <summary>旧 API 兼容：默认使用 Read 池。</summary>
    public Task<bool> WaitAsync(int timeoutMs, CancellationToken ct) =>
        WaitAsync(QueryPoolKind.Read, timeoutMs, ct);

    /// <summary>旧 API 兼容：默认释放 Read 池。</summary>
    public void Release() => _readPool.Release();

    public async Task<bool> WaitAsync(QueryPoolKind kind, int timeoutMs, CancellationToken ct)
    {
        var semaphore = GetSemaphore(kind);
        if (timeoutMs <= 0)
        {
            await semaphore.WaitAsync(ct).ConfigureAwait(false);
            return true;
        }
        return await semaphore.WaitAsync(timeoutMs, ct).ConfigureAwait(false);
    }

    public void Release(QueryPoolKind kind) => GetSemaphore(kind).Release();

    private SemaphoreSlim GetSemaphore(QueryPoolKind kind) => kind switch
    {
        QueryPoolKind.Interactive => _interactivePool,
        QueryPoolKind.Mutation => _mutationPool,
        _ => _readPool
    };
}

/// <summary>
/// 查询并发池类型。
/// - Read：History / Sync / ConversationList
/// - Interactive：MarkRead / SetPrefs
/// - Mutation：Group / Edit / Recall / Reaction
/// </summary>
public enum QueryPoolKind
{
    Read,
    Interactive,
    Mutation
}
