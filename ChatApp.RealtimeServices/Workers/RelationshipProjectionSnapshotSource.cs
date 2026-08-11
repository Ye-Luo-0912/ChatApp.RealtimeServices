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
    // Server 的导出端点以 camelCase 输出（AppJsonContext + ASP.NET Core 默认），
    // 此处用 camelCase + 大小写不敏感反序列化以与之匹配。
    private static readonly JsonSerializerOptions ServerJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        TypeInfoResolver = RealtimeJsonSerializerContext.Default,
    };

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

        return ReadAsync<RelationshipProjectionStreamPage>(path, ct);
    }

    public Task<RelationshipProjectionStreamSnapshot> ReadStreamAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct) =>
        ReadAsync<RelationshipProjectionStreamSnapshot>(
            $"api/ops/relationship-projection/streams/" +
            $"{ownerUserId.ToString(CultureInfo.InvariantCulture)}/" +
            ((byte)listType).ToString(CultureInfo.InvariantCulture),
            ct);

    public Task<RelationshipProjectionStreamDigest> ReadDigestAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct) =>
        ReadAsync<RelationshipProjectionStreamDigest>(
            $"api/ops/relationship-projection/streams/" +
            $"{ownerUserId.ToString(CultureInfo.InvariantCulture)}/" +
            $"{((byte)listType).ToString(CultureInfo.InvariantCulture)}/digest",
            ct);

    private async Task<T> ReadAsync<T>(string path, CancellationToken ct)
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
        return await JsonSerializer.DeserializeAsync<T>(stream, ServerJsonOptions, ct)
                .ConfigureAwait(false)
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
