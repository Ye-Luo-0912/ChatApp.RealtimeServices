namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话临时状态存储。只保存通话控制元数据（状态、参与者、revision、TTL），
/// 以 call id 为键、以 revision 为 CAS 前置条件实现原子迁移。
/// <para>实现不得保存 SDP/ICE 载荷或音频数据。</para>
/// </summary>
public interface ICallStateStore
{
    /// <summary>读取通话快照；不存在或已过期清理后返回 null。</summary>
    Task<CallStateSnapshot?> GetAsync(string callId, CancellationToken ct = default);

    /// <summary>
    /// 原子迁移：仅当当前 revision 等于 <paramref name="expectedRevision"/> 时写入
    /// <paramref name="candidate"/> 并应用 <paramref name="ttl"/>。创建新通话时
    /// <paramref name="expectedRevision"/> 为 null（要求当前不存在）。
    /// </summary>
    /// <returns>成功返回写入后的快照；发生冲突（revision 不匹配）返回 null。</returns>
    Task<CallStateSnapshot?> CompareAndSwapAsync(
        string callId,
        long? expectedRevision,
        CallStateSnapshot candidate,
        TimeSpan ttl,
        CancellationToken ct = default);

    /// <summary>删除通话临时状态。</summary>
    Task RemoveAsync(string callId, CancellationToken ct = default);
}

/// <summary>
/// 通话键。
/// </summary>
public static class CallKeys
{
    public static string StateKey(string callId) => $"call:state:{callId}";
}