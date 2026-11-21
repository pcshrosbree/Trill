using Microsoft.StreamProcessing;
using TrillDocs;

namespace TrillDocs.Tests;

/// <summary>
/// Runs every diagram's query in row mode and in columnar mode and compares the results.
/// The browser can run only row mode (see <see cref="Operators.Run(OperatorEntry, EventDto[][])"/>),
/// so this is the one place a diagram's columnar result is checked.
/// </summary>
[TestClass]
public sealed class BothModesTests
{
    /// <summary>Entries whose two modes differ because of an open issue: entry id to issue number.</summary>
    static readonly Dictionary<string, int> KnownDifferences = new()
    {
    };

    /// <summary>
    /// Entries whose columnar run falls back to row mode for at least one operator, with
    /// what Trill reports. Listed so that a diagram cannot stop running columnar unnoticed.
    /// </summary>
    static readonly Dictionary<string, string> KnownRowModeFallbacks = new()
    {
    };

    [TestMethod]
    [DoNotParallelize]
    public void EveryDiagramGivesTheSameResultInBothModes()
    {
        var results = Operators.All.Select(Compare).ToList();
        var failures = Failures(results, KnownDifferences, KnownRowModeFallbacks);
        Assert.AreEqual(0, failures.Count, string.Join("\n", failures));
    }

    /// <summary>One entry's two runs: the first difference between them, and the columnar run's fallbacks.</summary>
    internal sealed record ModeComparison(string Id, string Difference, string Fallbacks);

    /// <summary>The outcome of one run: its events, or the exception it threw.</summary>
    sealed record Outcome(List<EventDto> Events, string Error);

    static ModeComparison Compare(OperatorEntry entry)
    {
        var row = RunIn(entry, rowMode: true, new List<string>());
        var reports = new List<string>();
        var columnar = RunIn(entry, rowMode: false, reports);
        var fallbacks = reports.Count == 0 ? null : string.Join("; ", reports.Distinct());
        return new ModeComparison(entry.Id, FirstDifference(row, columnar), fallbacks);
    }

    static Outcome RunIn(OperatorEntry entry, bool rowMode, List<string> reports)
    {
        var savedMode = Config.ForceRowBasedExecution;
        var savedReporter = Config.RowModeReporter;
        try
        {
            Config.RowModeReporter = r => reports.Add($"{r.Reason} {r.NonColumnarType?.Name}: {r.Detail}");
            return new Outcome(Operators.Run(entry, entry.DefaultInputs, rowMode), null);
        }
        catch (Exception e)
        {
            return new Outcome(null, $"{e.GetType().Name}: {e.Message}");
        }
        finally
        {
            Config.ForceRowBasedExecution = savedMode;
            Config.RowModeReporter = savedReporter;
        }
    }

    /// <summary>
    /// Null when both runs give the same events; otherwise a description of the first difference.
    /// An exception in either run is a difference. Events with the same start time are compared
    /// in a fixed order, because the pages make no claim about the order of simultaneous events.
    /// </summary>
    static string FirstDifference(Outcome row, Outcome columnar)
    {
        if (row.Error is not null || columnar.Error is not null)
            return $"row mode: {row.Error ?? "no error"}; columnar mode: {columnar.Error ?? "no error"}";
        var r = Normalise(row.Events);
        var c = Normalise(columnar.Events);
        for (var i = 0; i < Math.Min(r.Count, c.Count); i++)
            if (r[i] != c[i])
                return $"event {i}: row mode {r[i]}, columnar mode {c[i]}";
        return r.Count == c.Count ? null : $"row mode gives {r.Count} events, columnar mode {c.Count}";
    }

    static List<EventDto> Normalise(List<EventDto> events) =>
        events.OrderBy(e => e.Start).ThenBy(e => e.End).ThenBy(e => e.Kind, StringComparer.Ordinal)
              .ThenBy(e => e.Key, StringComparer.Ordinal).ThenBy(e => e.Label, StringComparer.Ordinal).ToList();

    /// <summary>What the test reports, given each entry's comparison and the two tables.</summary>
    internal static List<string> Failures(
        IReadOnlyList<ModeComparison> results,
        IReadOnlyDictionary<string, int> knownDifferences,
        IReadOnlyDictionary<string, string> knownFallbacks)
    {
        var failures = new List<string>();
        foreach (var r in results)
        {
            var listed = knownDifferences.TryGetValue(r.Id, out var issue);
            if (r.Difference is not null && !listed)
                failures.Add($"'{r.Id}': the modes differ and no issue is listed for it: {r.Difference}");
            else if (r.Difference is null && listed)
                failures.Add($"'{r.Id}': the modes now agree; remove issue #{issue} from {nameof(KnownDifferences)}.");

            knownFallbacks.TryGetValue(r.Id, out var expected);
            if (r.Fallbacks != expected)
                failures.Add($"'{r.Id}': the columnar run falls back to row mode as [{r.Fallbacks ?? "nothing"}], expected [{expected ?? "nothing"}].");
        }

        var ids = results.Select(r => r.Id).ToHashSet();
        foreach (var id in knownDifferences.Keys.Concat(knownFallbacks.Keys).Distinct().Where(id => !ids.Contains(id)))
            failures.Add($"'{id}' is listed but is not in the catalogue.");
        return failures;
    }

    // The check must be able to fail. Each control plants one fault and requires the
    // report to name the entry.

    static readonly Dictionary<string, int> NoDifferences = new();
    static readonly Dictionary<string, string> NoFallbacks = new();

    [TestMethod]
    public void UnlistedDifferenceIsReported()
    {
        var failures = Failures([new("where", "event 0: planted", null)], NoDifferences, NoFallbacks);
        Assert.AreEqual(1, failures.Count, string.Join("\n", failures));
        StringAssert.Contains(failures[0], "'where': the modes differ");
    }

    [TestMethod]
    public void ListedDifferenceThatNoLongerDiffersIsReported()
    {
        var failures = Failures([new("where", null, null)], new Dictionary<string, int> { ["where"] = 1 }, NoFallbacks);
        Assert.AreEqual(1, failures.Count, string.Join("\n", failures));
        StringAssert.Contains(failures[0], "'where': the modes now agree");
    }

    [TestMethod]
    public void UnexpectedFallbackIsReported()
    {
        var failures = Failures([new("where", null, "TypeNotColumnar Out: planted")], NoDifferences, NoFallbacks);
        Assert.AreEqual(1, failures.Count, string.Join("\n", failures));
        StringAssert.Contains(failures[0], "'where': the columnar run falls back");
    }

    [TestMethod]
    public void ListedEntryMissingFromTheCatalogueIsReported()
    {
        var failures = Failures([], new Dictionary<string, int> { ["no-such-entry"] = 1 }, NoFallbacks);
        Assert.AreEqual(1, failures.Count, string.Join("\n", failures));
        StringAssert.Contains(failures[0], "'no-such-entry' is listed");
    }

    [TestMethod]
    [DataRow("end")]
    [DataRow("label")]
    [DataRow("missing")]
    [DataRow("error")]
    public void PlantedChangeToOneRunIsADifference(string change)
    {
        var events = new List<EventDto> { new("Interval", 1, 5, "a", "1"), new("Interval", 3, 9, "b", "2") };
        var planted = change switch
        {
            "end" => new Outcome([events[0] with { End = 6 }, events[1]], null),
            "label" => new Outcome([events[0], events[1] with { Label = "3" }], null),
            "missing" => new Outcome([events[0]], null),
            "error" => new Outcome(null, "InvalidOperationException: planted"),
            _ => throw new ArgumentException(change),
        };
        Assert.IsNull(FirstDifference(new Outcome(events, null), new Outcome(events.ToList(), null)));
        Assert.IsNotNull(FirstDifference(new Outcome(events, null), planted));
    }
}
