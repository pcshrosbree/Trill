using System.Runtime.CompilerServices;
using System.Text.Json;
using TrillDocs;

namespace TrillDocs.Tests;

[TestClass]
public sealed class CatalogTests
{
    /// <summary>Set to 1 to rewrite catalog.json from the engine. The run then fails, so an update is never a silent green.</summary>
    const string UpdateVariable = "TRILL_DOCS_UPDATE";

    static string CatalogPath([CallerFilePath] string here = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(here)!, "..", "src", "marbles", "catalog.json"));

    [TestMethod]
    public void CatalogFileMatchesTheEngine()
    {
        var actual = CatalogJson.Build();
        if (Environment.GetEnvironmentVariable(UpdateVariable) == "1")
        {
            File.WriteAllText(CatalogPath(), actual);
            Assert.Fail($"catalog.json rewritten from the engine. Review the diff, then rerun without {UpdateVariable}.");
        }

        var difference = FirstDifference(File.ReadAllText(CatalogPath()), actual);
        Assert.IsNull(difference, $"{difference}\nRegenerate with {UpdateVariable}=1 if the engine change is intended.");
    }

    // The comparator must be able to fail: plant one wrong value per field
    // class the pages draw, and require it to name the entry.
    [TestMethod]
    [DataRow("output-end")]
    [DataRow("output-kind")]
    [DataRow("code")]
    [DataRow("input")]
    public void PlantedWrongExpectationIsCaught(string field)
    {
        var good = CatalogJson.Build();
        var entries = Parse(good);
        var join = entries.Single(e => e.Id == "join");
        var planted = field switch
        {
            "output-end" => join with { Output = [join.Output[0] with { End = join.Output[0].End + 1 }, .. join.Output.Skip(1)] },
            "output-kind" => join with { Output = [join.Output[0] with { Kind = "Start", End = null }, .. join.Output.Skip(1)] },
            "code" => join with { Code = join.Code.Replace("r.Key", "r.Value") },
            "input" => join with { DefaultInputs = [[join.DefaultInputs[0][0] with { Start = 0 }, .. join.DefaultInputs[0].Skip(1)], join.DefaultInputs[1]] },
            _ => throw new ArgumentException(field),
        };
        var wrong = JsonSerializer.Serialize(entries.Select(e => e.Id == "join" ? planted : e).ToArray(), CatalogJson.Default.CatalogEntryDtoArray);

        var difference = FirstDifference(wrong, good);
        Assert.IsNotNull(difference);
        StringAssert.Contains(difference, "'join'");
    }

    [TestMethod]
    public void EveryDiagramHasOutput()
    {
        foreach (var entry in Operators.All)
            Assert.AreNotEqual(0, Operators.Run(entry, entry.DefaultInputs).Count, $"'{entry.Id}' draws an empty output lane");
    }

    [TestMethod]
    public void IdsAreUnique() =>
        Assert.AreEqual(Operators.All.Length, Operators.All.Select(e => e.Id).Distinct().Count());

    [TestMethod]
    public void PageCodeIsTheQueryLambdaBody()
    {
        Assert.AreEqual("input.Where(e => e.Value > 1)", Operators.All.Single(e => e.Id == "where").Code);
        Assert.AreEqual(
            "left.Join(right, l => l.Key, r => r.Key,\n    (l, r) => new Out(l.Key, l.Value + \"+\" + r.Value))",
            Operators.All.Single(e => e.Id == "join").Code);
    }

    [TestMethod]
    public void PagesAndCatalogueNameTheSameDiagrams()
    {
        var src = Path.GetDirectoryName(Path.GetDirectoryName(CatalogPath()))!;
        var onPages = Directory.EnumerateFiles(src, "*.md", SearchOption.AllDirectories)
            .SelectMany(f => System.Text.RegularExpressions.Regex.Matches(File.ReadAllText(f), "data-op=\"([^\"]+)\"").Select(m => m.Groups[1].Value))
            .ToHashSet();
        CollectionAssert.AreEquivalent(Operators.All.Select(e => e.Id).ToList(), onPages.ToList());
    }

    static CatalogEntryDto[] Parse(string json) => JsonSerializer.Deserialize(json, CatalogJson.Default.CatalogEntryDtoArray);

    /// <summary>Null when equal; otherwise names the first entry (by id) that differs, or a missing or extra entry.</summary>
    static string FirstDifference(string expectedJson, string actualJson)
    {
        var expected = Parse(expectedJson).ToDictionary(e => e.Id);
        var actual = Parse(actualJson).ToDictionary(e => e.Id);
        foreach (var id in expected.Keys.Union(actual.Keys))
        {
            if (!expected.TryGetValue(id, out var e)) return $"'{id}' is missing from catalog.json";
            if (!actual.TryGetValue(id, out var a)) return $"'{id}' is in catalog.json but not in the catalogue";
            var es = JsonSerializer.Serialize(e, CatalogJson.Default.CatalogEntryDto);
            var @as = JsonSerializer.Serialize(a, CatalogJson.Default.CatalogEntryDto);
            if (es != @as) return $"'{id}' differs.\nexpected (catalog.json): {es}\nactual (engine): {@as}";
        }
        return null;
    }
}
