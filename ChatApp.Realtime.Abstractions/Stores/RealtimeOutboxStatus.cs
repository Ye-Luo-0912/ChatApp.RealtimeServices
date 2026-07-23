namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// Outbox 行生命周期状态。Pending 可被认领；Published 已成功发布；Dead 超过最大重试进入 DLQ。
/// </summary>
public enum RealtimeOutboxStatus : short
{
    Pending = 0,
    Published = 1,
    Dead = 2
}
