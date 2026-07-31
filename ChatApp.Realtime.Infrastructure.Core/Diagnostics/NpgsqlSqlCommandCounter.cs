using System.Diagnostics.Tracing;

namespace ChatApp.Realtime.Infrastructure.Core.Diagnostics;

/// <summary>
/// 六-1：通过 Npgsql EventSource 计数每操作 SQL 命令数。
/// 用于基准测试和性能门禁，检测 SQL 命令数回退。
/// <para>
/// 非侵入式设计：通过 <see cref="EventListener"/> 监听 Npgsql 的 EventSource，
/// 不需要修改任何现有代码。使用 <see cref="AsyncLocal{T}"/> 隔离并发 scope。
/// </para>
/// <para>
/// 用法：
/// <code>
/// using var scope = NpgsqlSqlCommandCounter.BeginScope();
/// // 执行业务操作...
/// var count = NpgsqlSqlCommandCounter.GetCommandCount();
/// </code>
/// </para>
/// </summary>
public sealed class NpgsqlSqlCommandCounter : EventListener
{
    private static readonly AsyncLocal<int> CurrentCount = new();
    private static readonly object InitLock = new();
    private static NpgsqlSqlCommandCounter? _instance;
    private static bool _initialized;

    /// <summary>
    /// 开始一个 SQL 命令计数 scope。返回的 <see cref="IDisposable"/> 在 Dispose 时恢复之前的计数。
    /// </summary>
    public static IDisposable BeginScope()
    {
        EnsureInitialized();
        var initial = CurrentCount.Value;
        return new CountScope(initial);
    }

    /// <summary>获取当前 scope 内的 SQL 命令数。</summary>
    public static int GetCommandCount() => CurrentCount.Value;

    /// <summary>重置当前 scope 的计数为 0。</summary>
    public static void Reset() => CurrentCount.Value = 0;

    private static void EnsureInitialized()
    {
        if (_initialized)
            return;
        lock (InitLock)
        {
            if (_initialized)
                return;
            _instance = new NpgsqlSqlCommandCounter();
            _initialized = true;
        }
    }

    private NpgsqlSqlCommandCounter()
    {
        // EventListener 构造后，OnEventSourceCreated 会被自动回调已存在的 EventSource。
        // 新的 EventSource 创建时也会回调。
    }

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        // Npgsql 的 EventSource 名称在不同版本中可能不同：
        // Npgsql 4-7: "Npgsql"
        // Npgsql 8+: 可能使用 ETW 或 OpenTelemetry
        if (eventSource.Name == "Npgsql" || eventSource.Name == "Npgsql-NET")
        {
            try
            {
                EnableEvents(eventSource, EventLevel.Informational);
            }
            catch
            {
                // 忽略启用失败，某些 EventSource 不支持动态启用
            }
        }
        base.OnEventSourceCreated(eventSource);
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        // Npgsql EventSource 的命令执行事件名称：
        // CommandStart / ExecuteStart / CommandStop / ExecuteStop
        var name = eventData.EventName;
        if (name is "CommandStart" or "ExecuteStart")
        {
            CurrentCount.Value++;
        }
    }

    private sealed class CountScope : IDisposable
    {
        private readonly int _initial;
        private int _disposed;

        public CountScope(int initial)
        {
            _initial = initial;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                CurrentCount.Value = _initial;
            }
        }
    }
}
