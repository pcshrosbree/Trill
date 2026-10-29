using System.Text.Json;
using System.Text.Json.Serialization;

namespace TrillDocs;

/// <summary>Input payload for every diagram: a key (drawn as colour) and a value (drawn as text).</summary>
public sealed class Ev
{
    public string Key;
    public int Value;
}

/// <summary>Output payload for diagrams whose result is not an <see cref="Ev"/>: a key and a display label.</summary>
public sealed record Out(string Key, string Label);

/// <summary>
/// One event as a diagram sees it. <c>Kind</c> is Trill's <c>StreamEventKind</c>
/// (<c>Interval</c>, <c>Start</c> or <c>End</c>). <c>End</c> is null for a start
/// edge, whose end is not yet known. For an end edge, <c>Start</c> is the start
/// of the event it closes.
/// </summary>
public sealed record EventDto(string Kind, long Start, long? End, string Key, string Label);

/// <summary>One diagram: its inputs, the query text a page shows, and the query that runs.</summary>
public sealed record OperatorEntry(
    string Id,
    string Title,
    string Category,
    string Code,
    string[] InputNames,
    EventDto[][] DefaultInputs,
    Func<Microsoft.StreamProcessing.IStreamable<Microsoft.StreamProcessing.Empty, Ev>[], List<EventDto>> Query);

/// <summary>An entry as written to <c>catalog.json</c>: metadata, inputs, and the engine's output for those inputs.</summary>
public sealed record CatalogEntryDto(
    string Id, string Title, string Category, string Code, string[] InputNames,
    EventDto[][] DefaultInputs, List<EventDto> Output);

/// <summary>Source-generated JSON: the trimmed browser build disables reflection-based serialisation.</summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web, WriteIndented = true)]
[JsonSerializable(typeof(CatalogEntryDto[]))]
[JsonSerializable(typeof(CatalogEntryDto))]
[JsonSerializable(typeof(EventDto[][]))]
[JsonSerializable(typeof(List<EventDto>))]
public sealed partial class CatalogJson : JsonSerializerContext
{
    /// <summary>The full catalogue, each entry run once on its default inputs, as committed to <c>catalog.json</c>.</summary>
    public static string Build() => JsonSerializer.Serialize(
        Operators.All.Select(o => new CatalogEntryDto(
            o.Id, o.Title, o.Category, o.Code, o.InputNames, o.DefaultInputs, Operators.Run(o, o.DefaultInputs))).ToArray(),
        Default.CatalogEntryDtoArray) + "\n";
}
