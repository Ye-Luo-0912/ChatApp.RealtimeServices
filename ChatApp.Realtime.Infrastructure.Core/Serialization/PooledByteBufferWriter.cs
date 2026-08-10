using System.Buffers;

namespace ChatApp.Realtime.Infrastructure.Core.Serialization;

/// <summary>
/// 线程内共享的 UTF-8 临时缓冲。序列化是同步区段，按线程复用可避免全局池竞争；
/// 每线程最多保留两个 Writer，超大缓冲不缓存，避免偶发大消息长期抬高常驻内存；
/// 归还或扩容前清除已写区域，防止后续租用者观察到上一条消息内容。
/// </summary>
internal sealed class PooledByteBufferWriter : IBufferWriter<byte>
{
    private const int DefaultCapacity = 512;
    private const int MaximumRetainedBufferBytes = 64 * 1024;
    [ThreadStatic]
    private static PooledByteBufferWriter? _cachedWriter1;

    [ThreadStatic]
    private static PooledByteBufferWriter? _cachedWriter2;

    private byte[] _buffer = [];
    private int _written;
    private long _version;
    private long _activeLeaseVersion;

    private PooledByteBufferWriter()
    {
    }

    public static PooledByteBufferLease Rent(int minimumCapacity = DefaultCapacity)
    {
        var writer = _cachedWriter1;
        if (writer is not null)
        {
            _cachedWriter1 = _cachedWriter2;
            _cachedWriter2 = null;
        }
        else
        {
            writer = new PooledByteBufferWriter();
        }

        var version = writer.Initialize(Math.Max(1, minimumCapacity));
        return new PooledByteBufferLease(writer, version);
    }

    public void Advance(int count)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _activeLeaseVersion) == 0, this);
        if (count < 0 || _written > _buffer.Length - count)
            throw new ArgumentOutOfRangeException(nameof(count));
        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    private long Initialize(int minimumCapacity)
    {
        var version = Interlocked.Increment(ref _version);
        if (Interlocked.CompareExchange(ref _activeLeaseVersion, version, 0) != 0)
            throw new InvalidOperationException("共享字节缓冲被重复租用。");

        _written = 0;
        if (_buffer.Length < minimumCapacity)
        {
            if (_buffer.Length > 0)
                ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = ArrayPool<byte>.Shared.Rent(minimumCapacity);
        }
        return version;
    }

    private void EnsureCapacity(int sizeHint)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _activeLeaseVersion) == 0, this);
        sizeHint = Math.Max(1, sizeHint);
        if (sizeHint <= _buffer.Length - _written)
            return;

        var required = checked(_written + sizeHint);
        var newSize = Math.Max(required, Math.Max(DefaultCapacity, _buffer.Length * 2));
        var replacement = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer.AsSpan(0, _written).CopyTo(replacement);
        if (_buffer.Length > 0)
        {
            _buffer.AsSpan(0, _written).Clear();
            ArrayPool<byte>.Shared.Return(_buffer);
        }
        _buffer = replacement;
    }

    internal int GetWrittenCount(long version)
    {
        EnsureCurrentLease(version);
        return _written;
    }

    internal ReadOnlySpan<byte> GetWrittenSpan(long version)
    {
        EnsureCurrentLease(version);
        return _buffer.AsSpan(0, _written);
    }

    internal byte[] ToArray(long version) => GetWrittenSpan(version).ToArray();

    internal void Return(long version)
    {
        if (Interlocked.CompareExchange(ref _activeLeaseVersion, 0, version) != version)
            return;

        _buffer.AsSpan(0, _written).Clear();
        _written = 0;
        if (_buffer.Length > MaximumRetainedBufferBytes)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = [];
        }

        if (_cachedWriter1 is null)
        {
            _cachedWriter1 = this;
            return;
        }

        if (_cachedWriter2 is null)
        {
            _cachedWriter2 = this;
            return;
        }

        if (_buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = [];
        }
    }

    private void EnsureCurrentLease(long version)
    {
        if (Volatile.Read(ref _activeLeaseVersion) != version)
            throw new ObjectDisposedException(nameof(PooledByteBufferLease));
    }
}

/// <summary>
/// 带版本的缓冲租约。版本令牌阻止已归还的旧句柄二次释放后来复用的同一 Writer。
/// </summary>
internal readonly struct PooledByteBufferLease : IDisposable
{
    private readonly PooledByteBufferWriter? _writer;
    private readonly long _version;

    internal PooledByteBufferLease(PooledByteBufferWriter writer, long version)
    {
        _writer = writer;
        _version = version;
    }

    internal IBufferWriter<byte> BufferWriter =>
        _writer ?? throw new ObjectDisposedException(nameof(PooledByteBufferLease));

    internal int WrittenCount => _writer?.GetWrittenCount(_version) ?? 0;

    internal ReadOnlySpan<byte> WrittenSpan =>
        _writer is null ? [] : _writer.GetWrittenSpan(_version);

    internal byte[] ToArray() =>
        _writer?.ToArray(_version) ?? [];

    public void Dispose() => _writer?.Return(_version);
}
