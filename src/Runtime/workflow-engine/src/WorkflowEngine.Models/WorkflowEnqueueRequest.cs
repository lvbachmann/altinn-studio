using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace WorkflowEngine.Models;

/// <summary>
/// A request to enqueue one or more workflows in a single batch.
/// </summary>
public sealed record WorkflowEnqueueRequest
{
    /// <summary>
    /// Indexed key-value pairs for filtering, grouping, and dashboard queries.
    /// Applied to all workflows in this batch.
    /// </summary>
    [JsonPropertyName("labels")]
    public Dictionary<string, string>? Labels { get; init; }

    /// <summary>
    /// Opaque context passed to command handlers at execution time.
    /// Applied to all workflows in this batch. The engine never inspects this.
    /// </summary>
    [JsonPropertyName("context")]
    public JsonElement? Context { get; init; }

    /// <summary>
    /// Optional collection key. When set, the workflows in this request join the named collection
    /// (created on first use within the namespace). Subsequent requests with the same collection key
    /// can build upon the collection's current head workflows.
    /// </summary>
    [JsonPropertyName("collectionKey")]
    public string? CollectionKey { get; init; }

    /// <summary>
    /// The workflows to enqueue.
    /// </summary>
    [JsonPropertyName("workflows")]
    public required IReadOnlyList<WorkflowRequest> Workflows { get; init; }

    internal byte[] ComputeHash()
    {
        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(this);
        return SHA256.HashData(jsonBytes);
    }
}
