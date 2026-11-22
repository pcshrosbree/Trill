using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
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
        // Columnar mode throws while the query is built: Ev is a class with default equality.
        ["clip-event-duration"] = 159,
        // Chop (Beat) refuses Ev in columnar mode; RepetitiveHoppingWindowLifetime ends with a Chop.
        ["repetitive-hopping-window"] = 164,
        ["chop"] = 164,
        ["stitch"] = 164,
        // Columnar mode throws NullReferenceException while the query runs.
        ["quantize-lifetime"] = 163,
        ["progressive-quantize-lifetime"] = 163,
    };

    /// <summary>
    /// Entries whose columnar run falls back to row mode for at least one operator, with
    /// what Trill reports. Listed so that a diagram cannot stop running columnar unnoticed.
    /// </summary>
    static readonly Dictionary<string, string> KnownRowModeFallbacks = new()
    {
        // Ev uses reference equality, which columnar code cannot compile; the pages say so.
        ["join"] = NoEquijoinComparer,
        ["left-outer-join"] = NoAntisemijoinComparer + "; " + NoEquijoinComparer,
        ["where-not-exists"] = NoAntisemijoinComparer,
        // Clip falls back when its left input is start-edge-only (#41), as both operators make it.
        ["consecutive-pairs"] = ClipNeedsComparer,
        ["sessionize"] = ClipNeedsComparer,
        // Payloads that columnar mode cannot hold in columns.
        ["distinct"] = "TypeNotColumnar String: field _stringLength is not public",
        ["pivot"] = "TypeNotColumnar Int32[]: the type has no public parameterless constructor",
        ["top-k"] = "TypeNotColumnar List`1: field _items is not public",
        // Generated code that doesn't compile: #160 (the selector uses the key, reads a payload field in a
        // nested lambda, and names a parameter i) and #138 (the EndPointHeap field, CS0052).
        ["select-many-by-key"] = GeneratedCodeDoesNotCompile,
        ["point-at-end"] = GeneratedCodeDoesNotCompile,
        ["extend-lifetime"] = GeneratedCodeDoesNotCompile,
    };

    const string ClipNeedsComparer = "NotSupported : Type of payload, 'TrillDocs.Ev', to Clip does not have a valid equality operator for columnar mode. "
        + "Its equality and hash expressions may use the payload only to read its fields or properties; set such a comparer on the left input with SetProperty().PayloadEqualityComparer.";

    const string GeneratedCodeDoesNotCompile = "CodeGenerationFailed : code generation threw NullReferenceException: Object reference not set to an instance of an object.";

    const string ComparerNeeded = " does not implement the interface IEqualityComparerExpression. This interface is needed for code generation of this operator for columnar mode. "
        + "Furthermore, the equality expression in the interface can only refer to input variables if used in field or property references.";
    const string NoEquijoinComparer = "NotSupported : The left input payload type, 'TrillDocs.Ev', to Equijoin" + ComparerNeeded;
    const string NoAntisemijoinComparer = "NotSupported : The payload type, 'TrillDocs.Ev', to Left Antisemijoin" + ComparerNeeded;

    [TestMethod]
    [DoNotParallelize]
    public void EveryDiagramGivesTheSameResultInBothModes()
    {
        var results = Operators.All.Select(Compare).ToList();
        var failures = Failures(results, KnownDifferences, KnownRowModeFallbacks);
        Assert.AreEqual(0, failures.Count, string.Join("\n", failures));
    }

    /// <summary>
    /// One entry's two runs: the first difference between them, and the columnar run's fallbacks.
    /// <paramref name="Diagnostics"/> names the operator and the throwing frame of any fallback that
    /// came from an exception; it is printed with an unexpected fallback and is not compared.
    /// </summary>
    internal sealed record ModeComparison(string Id, string Difference, string Fallbacks, string Diagnostics = null);

    /// <summary>The outcome of one run: its events, or the exception it threw.</summary>
    sealed record Outcome(List<EventDto> Events, string Error);

    static ModeComparison Compare(OperatorEntry entry)
    {
        var row = RunIn(entry, rowMode: true, new List<string>(), new List<string>());
        var reports = new List<string>();
        var diagnostics = new List<string>();
        var columnar = RunIn(entry, rowMode: false, reports, diagnostics);
        var fallbacks = reports.Count == 0 ? null : string.Join("; ", reports.Distinct());
        var thrown = diagnostics.Count == 0 ? null : string.Join("; ", diagnostics.Distinct());
        return new ModeComparison(entry.Id, FirstDifference(row, columnar), fallbacks, thrown);
    }

    static Outcome RunIn(OperatorEntry entry, bool rowMode, List<string> reports, List<string> diagnostics)
    {
        var savedMode = Config.ForceRowBasedExecution;
        var savedReporter = Config.RowModeReporter;
        try
        {
            Config.RowModeReporter = r =>
            {
                reports.Add($"{r.Reason} {r.NonColumnarType?.Name}: {r.Detail}");
                if (r.Exception is not null)
                    diagnostics.Add($"{r.Operator?.GetType().Name} threw {r.Exception.GetType().Name} {FirstFrames(r.Exception)}"
                        + (FirstCompilerError(r.Diagnostics) is { } error ? $"; first compiler error: {error}" : ""));
            };
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

    /// <summary>
    /// Puts events in a fixed order, after joining each end edge to its start edge. Trill may send one
    /// event as an interval or as a start edge and a later end edge, and both give the same lifetime:
    /// session-timeout-window's page says its two modes differ in just this way. A start edge with
    /// no end edge stays a start edge, so it still differs from an interval.
    /// </summary>
    static List<EventDto> Normalise(List<EventDto> events)
    {
        var joined = new List<EventDto>();
        foreach (var e in events)
        {
            var open = e.Kind == "End"
                ? joined.FindIndex(s => s.Kind == "Start" && s.Start == e.Start && s.Key == e.Key && s.Label == e.Label)
                : -1;
            if (open >= 0) joined[open] = new EventDto("Interval", e.Start, e.End, e.Key, e.Label);
            else joined.Add(e);
        }
        return joined.OrderBy(e => e.Start).ThenBy(e => e.End).ThenBy(e => e.Kind, StringComparer.Ordinal)
              .ThenBy(e => e.Key, StringComparer.Ordinal).ThenBy(e => e.Label, StringComparer.Ordinal).ToList();
    }

    /// <summary>The first line of code generation's diagnostics that reports an error, or null.</summary>
    static string FirstCompilerError(string diagnostics) =>
        diagnostics?.Split('\n').Select(l => l.Trim()).FirstOrDefault(l => l.Contains("error", StringComparison.OrdinalIgnoreCase));

    /// <summary>The exception's first two stack frames, enough to find the throw in Trill's source.</summary>
    static string FirstFrames(Exception e) =>
        string.Join(" < ", (e.StackTrace ?? "").Split('\n').Select(l => l.Trim()).Where(l => l.Length > 0).Take(2));

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
                failures.Add($"'{r.Id}': the columnar run falls back to row mode as [{r.Fallbacks ?? "nothing"}], expected [{expected ?? "nothing"}]."
                    + (r.Diagnostics is null ? "" : $" Thrown: {r.Diagnostics}"));
        }

        var ids = results.Select(r => r.Id).ToHashSet();
        foreach (var id in knownDifferences.Keys.Concat(knownFallbacks.Keys).Distinct().Where(id => !ids.Contains(id)))
            failures.Add($"'{id}' is listed but is not in the catalogue.");
        return failures;
    }

    /// <summary>
    /// The page note for a known difference: a line under the diagram that starts with this
    /// and ends with the issue number, such as <c>(#159)</c>.
    /// </summary>
    internal const string NotePrefix = "> **Columnar mode differs:**";

    /// <summary>One docs page: its file, the diagrams it shows, and the issue numbers its notes cite (0 for a note without one).</summary>
    internal sealed record Page(string File, IReadOnlyList<string> Diagrams, IReadOnlyList<int> NoteIssues);

    [TestMethod]
    public void PageNotesMatchTheKnownDifferences()
    {
        var failures = NoteFailures(Pages(), KnownDifferences);
        Assert.AreEqual(0, failures.Count, string.Join("\n", failures));
    }

    static List<Page> Pages([CallerFilePath] string here = "")
    {
        var src = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(here)!, "..", "src"));
        return Directory.EnumerateFiles(src, "*.md", SearchOption.AllDirectories)
            .Select(f => ReadPage(Path.GetRelativePath(src, f), File.ReadAllText(f))).ToList();
    }

    internal static Page ReadPage(string file, string text) => new(
        file,
        Regex.Matches(text, "data-op=\"([^\"]+)\"").Select(m => m.Groups[1].Value).ToList(),
        text.Split('\n').Select(l => l.TrimEnd('\r')).Where(l => l.StartsWith(NotePrefix, StringComparison.Ordinal))
            .Select(l => Regex.Match(l, @"\(#(\d+)\)\s*$") is { Success: true } m ? int.Parse(m.Groups[1].Value) : 0).ToList());

    /// <summary>
    /// What the page-note test reports: a known difference whose diagram's page has no note citing
    /// its issue, a note without an issue number, and a note whose issue is not a known difference
    /// of a diagram on that page.
    /// </summary>
    internal static List<string> NoteFailures(IReadOnlyList<Page> pages, IReadOnlyDictionary<string, int> knownDifferences)
    {
        var failures = new List<string>();
        foreach (var (id, issue) in knownDifferences)
        {
            var page = pages.FirstOrDefault(p => p.Diagrams.Contains(id));
            if (page is not null && !page.NoteIssues.Contains(issue))
                failures.Add($"'{id}': {page.File} has no \"{NotePrefix} ... (#{issue})\" note.");
        }
        foreach (var page in pages)
            foreach (var issue in page.NoteIssues)
            {
                if (issue == 0)
                    failures.Add($"{page.File}: a \"{NotePrefix}\" note does not end with an issue number such as (#1).");
                else if (!page.Diagrams.Any(id => knownDifferences.TryGetValue(id, out var known) && known == issue))
                    failures.Add($"{page.File}: its note cites #{issue}, which {nameof(KnownDifferences)} does not list for a diagram on this page.");
            }
        return failures;
    }

    // The check must be able to fail. Each control plants one fault and requires the
    // report to name the entry.

    [TestMethod]
    [DataRow("matching", null)]
    [DataRow("no note", "has no")]
    [DataRow("no number", "does not end with an issue number")]
    [DataRow("wrong number", "cites #8")]
    [DataRow("note on another page", "cites #7")]
    public void PlantedPageNoteFaultIsReported(string change, string expected)
    {
        const string note = NotePrefix + " planted (#7)";
        var diagram = "<div class=\"trill-marbles\" data-op=\"clip\"></div>\n";
        Page[] pages = change switch
        {
            "matching" => [ReadPage("clip.md", diagram + note)],
            "no note" => [ReadPage("clip.md", diagram)],
            "no number" => [ReadPage("clip.md", diagram + note + "\n" + NotePrefix + " planted")],
            "wrong number" => [ReadPage("clip.md", diagram + NotePrefix + " planted (#8)\n" + note)],
            "note on another page" => [ReadPage("clip.md", diagram + note), ReadPage("other.md", "<div data-op=\"other\"></div>\n" + note)],
            _ => throw new ArgumentException(change),
        };
        var failures = NoteFailures(pages, new Dictionary<string, int> { ["clip"] = 7 });
        if (expected is null)
            Assert.AreEqual(0, failures.Count, string.Join("\n", failures));
        else
        {
            Assert.AreEqual(1, failures.Count, string.Join("\n", failures));
            StringAssert.Contains(failures[0], expected);
        }
    }

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

    // The cases in Normalise's summary.
    [TestMethod]
    [DataRow("edges", false)]
    [DataRow("unclosed", true)]
    [DataRow("later end", true)]
    public void EdgesAreComparedByLifetime(string change, bool differs)
    {
        var intervals = new List<EventDto> { new("Interval", 1, 5, "a", "1"), new("Interval", 3, 9, "b", "2") };
        List<EventDto> other = change switch
        {
            "edges" => [new("Start", 1, null, "a", "1"), new("Start", 3, null, "b", "2"), new("End", 1, 5, "a", "1"), new("End", 3, 9, "b", "2")],
            "unclosed" => [new("Start", 1, null, "a", "1"), intervals[1]],
            "later end" => [new("Start", 1, null, "a", "1"), new("End", 1, 6, "a", "1"), intervals[1]],
            _ => throw new ArgumentException(change),
        };
        Assert.AreEqual(differs, FirstDifference(new Outcome(intervals, null), new Outcome(other, null)) is not null);
    }
}
