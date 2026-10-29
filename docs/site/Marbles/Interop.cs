using System.Runtime.InteropServices.JavaScript;
using System.Text.Json;
using TrillDocs;

/// <summary>The JavaScript-facing surface: run one catalogue entry over (possibly dragged) inputs.</summary>
public static partial class Interop
{
    /// <summary>Runs entry <paramref name="id"/> over <paramref name="inputsJson"/> (a JSON array of event arrays).</summary>
    [JSExport]
    public static string Run(string id, string inputsJson)
    {
        var entry = Operators.All.Single(o => o.Id == id);
        var inputs = JsonSerializer.Deserialize(inputsJson, CatalogJson.Default.EventDtoArrayArray);
        return JsonSerializer.Serialize(Operators.Run(entry, inputs), CatalogJson.Default.ListEventDto);
    }

    /// <summary>Entry point required by the WASM host; the widget calls <see cref="Run"/> directly.</summary>
    public static void Main() { }
}
