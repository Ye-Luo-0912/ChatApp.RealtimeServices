namespace ChatApp.Realtime.Abstractions.State;

/// <summary>
/// 实时状态存储接口，用于在实时应用中存储和检索状态数据。
/// 该接口定义了基本的键值对操作，包括设置、获取和删除键值对。
/// </summary>
public interface IRealtimeStateStore
{
    Task SetAsync(string key, string value, CancellationToken ct = default);
    Task<string?> GetAsync(string key, CancellationToken ct = default);
    Task RemoveAsync(string key, CancellationToken ct = default);
}
