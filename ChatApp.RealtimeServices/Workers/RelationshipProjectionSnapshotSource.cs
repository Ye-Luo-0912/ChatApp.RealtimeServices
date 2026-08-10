using System.Globalization;
using System.Net;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.RealtimeServices.Options;

namespace ChatApp.RealtimeServices.Workers;

internal interface IRelationshipProjectionSnapshotSource
{
    Task<RelationshipProjectionStreamPage> ListStreamsAsync(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int limit,
        CancellationToken ct);

    Task<RelationshipProjectionStreamSnapshot> ReadStreamAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct);

    Task<RelationshipProjectionStreamDigest> ReadDigestAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct);
}

internal sealed class UnavailableRelationshipProjectionSnapshotSource
    : IRelationshipProjectionSnapshotSource
{
    public static UnavailableRelationshipProjectionSnapshotSource Instance { get; } = new();

    private UnavailableRelationshipProjectionSnapshotSource()
    {
    }

    public Task<RelationshipProjectionStreamPage> ListStreamsAsync(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int limit,
        CancellationToken ct) => throw new RelationshipProjectionSourceUnavailableException();

    public Task<RelationshipProjectionStreamSnapshot> ReadStreamAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct) => throw new RelationshipProjectionSourceUnavailableException();

    public Task<RelationshipProjectionStreamDigest> ReadDigestAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct) => throw new RelationshipProjectionSourceUnavailableException();
}

internal sealed class ServerRelationshipProjectionSnapshotSource(
    HttpClient httpClient) : IRelationshipProjectionSnapshotSource
{
    public Task<RelationshipProjectionStreamPage> ListStreamsAsync(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int limit,
        CancellationToken ct)
    {
        var path = $"api/ops/relationship-projection/streams?limit={limit.ToString(CultureInfo.InvariantCulture)}";
        if (afterOwnerUserId is { } owner && afterListType is { } listType)
        {
            path += $"&afterOwnerUserId={owner.ToString(CultureInfo.InvariantCulture)}";
            path += $"&afterListType={((byte)listType).ToString(CultureInfo.InvariantCulture)}";
        }

        return ReadAsync(
            path,
            RealtimeJsonSerializerContext.Default.RelationshipProjectionStreamPage,
            ct);
    }

    public Task<RelationshipProjectionStreamSnapshot> ReadStreamAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct) =>
        ReadAsync(
            $"api/ops/relationship-projection/streams/" +
            $"{ownerUserId.ToString(CultureInfo.InvariantCulture)}/" +
            ((byte)listType).ToString(CultureInfo.InvariantCulture),
            RealtimeJsonSerializerContext.Default.RelationshipProjectionStreamSnapshot,
            ct);

    public Task<RelationshipProjectionStreamDigest> ReadDigestAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct) =>
        ReadAsync(
            $"api/ops/relationship-projection/streams/" +
            $"{ownerUserId.ToString(CultureInfo.InvariantCulture)}/" +
            $"{((byte)listType).ToString(CultureInfo.InvariantCulture)}/digest",
            RealtimeJsonSerializerContext.Default.RelationshipProjectionStreamDigest,
            ct);

    private async Task<T> ReadAsync<T>(
        string path,
        System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> typeInfo,
        CancellationToken ct)
        where T : class
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, path);
        using var response = await httpClient.SendAsync(
                request,
                HttpCompletionOption.ResponseHeadersRead,
                ct)
            .ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new RelationshipProjectionSourceException(response.StatusCode);

        await using var stream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        return await JsonSerializer.DeserializeAsync(stream, typeInfo, ct).ConfigureAwait(false)
            ?? throw new InvalidDataException("Relationship projection source returned a null payload.");
    }
}

internal sealed class RelationshipProjectionSourceException(HttpStatusCode statusCode)
    : HttpRequestException(
        $"Relationship projection source returned HTTP {(int)statusCode}.",
        null,
        statusCode);

internal sealed class RelationshipProjectionSourceUnavailableException()
    : InvalidOperationException("Relationship projection snapshot source is not configured.");
