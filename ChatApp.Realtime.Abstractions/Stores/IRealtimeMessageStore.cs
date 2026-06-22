namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 实时消息存储接口，定义了保存实时消息的方法。
/// 该接口的实现类负责将实时消息记录持久化到指定的数据存储中。
/// </summary>
public interface IRealtimeMessageStore
{
    /// <summary>
    /// 将实时消息记录异步保存到数据存储中。
    /// </summary>
    /// <param name="message">要保存的实时消息记录。</param>
    /// <param name="ct">用于取消操作的取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    Task SaveAsync(RealtimeMessageRecord message, CancellationToken ct = default);
}
