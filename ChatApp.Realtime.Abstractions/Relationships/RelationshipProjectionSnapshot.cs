using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace ChatApp.Realtime.Abstractions.Relationships;

/// <summary>
/// One complete Server-authoritative relationship list captured at a contiguous stream version.
/// </summary>
public sealed class RelationshipProjectionStreamSnapshot
{
    public const int CurrentSchemaVersion = 1;

    public int SchemaVersion { get; init; } = CurrentSchemaVersion;
    public required string SnapshotId { get; init; }
    public required long OwnerUserId { get; init; }
    public required RelationshipProjectionListType ListType { get; init; }
    public required long Version { get; init; }
    public required long CapturedAtMs { get; init; }
    public required int ItemCount { get; init; }
    public required string ResourceHash { get; init; }
    public required IReadOnlyList<RelationshipProjectionSnapshotItem> Items { get; init; }
}

public sealed class RelationshipProjectionSnapshotItem
{
    public required string ResourceId { get; init; }
    public required long SubjectUserId { get; init; }
    public required long ActorUserId { get; init; }
    public string? State { get; init; }
    public string? Message { get; init; }
    public required long OccurredAtMs { get; init; }
}

public sealed record RelationshipProjectionStreamDescriptor(
    long OwnerUserId,
    RelationshipProjectionListType ListType,
    long Version);

public sealed record RelationshipProjectionStreamPage(
    IReadOnlyList<RelationshipProjectionStreamDescriptor> Items,
    bool HasMore,
    long? NextOwnerUserId,
    RelationshipProjectionListType? NextListType);

/// <summary>
/// Privacy-minimized digest used to reconcile one authoritative relationship stream.
/// It intentionally excludes relationship resource ids, messages, and actors.
/// </summary>
public sealed record RelationshipProjectionStreamDigest(
    long OwnerUserId,
    RelationshipProjectionListType ListType,
    long Version,
    int ItemCount,
    string ResourceHash,
    long CapturedAtMs);

public static class RelationshipProjectionSnapshotHash
{
    /// <summary>Hashes the sorted resource-key set; count and version travel separately.</summary>
    public static string Compute(IEnumerable<string> resourceIds)
    {
        ArgumentNullException.ThrowIfNull(resourceIds);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> length = stackalloc byte[sizeof(int)];

        foreach (var resourceId in resourceIds.Order(StringComparer.Ordinal))
        {
            if (string.IsNullOrWhiteSpace(resourceId) || resourceId.Length > 128)
                throw new ArgumentException("Snapshot resource id is invalid.", nameof(resourceIds));

            var bytes = Encoding.UTF8.GetBytes(resourceId);
            BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
            hash.AppendData(length);
            hash.AppendData(bytes);
        }

        return Convert.ToHexString(hash.GetHashAndReset());
    }
}
