// The operator catalogue: one entry per diagram. The query text a page shows is
// captured from the query lambda itself by [CallerArgumentExpression], so the
// text and the query that runs cannot drift apart.
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using Microsoft.StreamProcessing;

namespace TrillDocs;

/// <summary>Every diagram in the reference, in index order.</summary>
public static class Operators
{
    /// <summary>Category names, in index order.</summary>
    public static readonly string[] Categories =
        ["Filtering and projection", "Lifetimes and windows", "Combining streams", "Joins", "Aggregates", "Patterns"];

    // Shared inputs. Two keyed interval streams for the binary operators, and
    // one stream of one-tick events for windows, aggregates and patterns.
    static readonly EventDto[] Left = [I(1, 5, "a", 1), I(3, 9, "b", 2), I(6, 8, "a", 3)];
    static readonly EventDto[] Right = [I(2, 4, "a", 10), I(4, 7, "b", 20), I(10, 12, "a", 30)];
    static readonly EventDto[] Ticks = [P(1, "a", 1), P(2, "b", 2), P(4, "a", 3), P(7, "b", 4), P(9, "a", 5)];

    /// <summary>The catalogue.</summary>
    public static readonly OperatorEntry[] All =
    [
        Unary("where", "Where", 0, Left,
            input => input.Where(e => e.Value > 1)),
        Unary("select", "Select", 0, Left,
            input => input.Select(e => new Ev { Key = e.Key, Value = e.Value * 10 })),

        Unary("alter-event-duration", "AlterEventDuration", 1, Ticks,
            input => input.AlterEventDuration(3)),
        Binary("clip-event-duration", "ClipEventDuration", 1, Left, Right,
            (left, right) => left.ClipEventDuration(right, l => l.Key, r => r.Key)),
        Unary("tumbling-window", "TumblingWindowLifetime", 1, Ticks,
            input => input.TumblingWindowLifetime(4)),
        Unary("hopping-window", "HoppingWindowLifetime", 1, Ticks,
            input => input.HoppingWindowLifetime(4, 2)),

        Binary("union", "Union", 2, Left, Right,
            (left, right) => left.Union(right)),
        Unary("multicast", "Multicast", 2, Left,
            input => input.Multicast(s => s.Where(e => e.Key == "a").Union(s.Where(e => e.Value > 2)))),

        Binary("join", "Join", 3, Left, Right,
            (left, right) => left.Join(right, l => l.Key, r => r.Key,
                (l, r) => new Out(l.Key, l.Value + "+" + r.Value))),
        Binary("left-outer-join", "LeftOuterJoin", 3, Left, Right,
            (left, right) => left.LeftOuterJoin(right, l => l.Key, r => r.Key,
                l => new Out(l.Key, l.Value + "+none"),
                (l, r) => new Out(l.Key, l.Value + "+" + r.Value))),
        Binary("where-not-exists", "WhereNotExists", 3, Left, Right,
            (left, right) => left.WhereNotExists(right, l => l.Key, r => r.Key)),

        Unary("count", "Count (tumbling window)", 4, Ticks,
            input => input.TumblingWindowLifetime(4).Count()),
        Unary("sum", "Sum (hopping window)", 4, Ticks,
            input => input.HoppingWindowLifetime(4, 2).Sum(e => e.Value)),
        Unary("group-apply", "GroupApply", 4, Ticks,
            input => input.TumblingWindowLifetime(5).GroupApply(e => e.Key,
                g => g.Average(e => e.Value),
                (g, avg) => new Out(g.Key, "avg " + avg))),

        Unary("detect", "Detect", 5, Ticks,
            input => input.Detect(p => p
                    .SingleElement(e => e.Key == "a")
                    .SingleElement(e => e.Key == "b"),
                maxDuration: 4)),
    ];

    /// <summary>Runs <paramref name="entry"/> over <paramref name="inputs"/> in row mode.</summary>
    public static List<EventDto> Run(OperatorEntry entry, EventDto[][] inputs)
    {
        // Columnar mode compiles C# with Roslyn from file-backed assembly
        // references, which browser WASM cannot supply. Row mode only compiles
        // expression trees, never with Roslyn, and the native tests use it too
        // so that both hosts run the same code.
        Config.ForceRowBasedExecution = true;
        return entry.Query(inputs.Select(Ingress).ToArray());
    }

    /// <summary>Turns diagram input events (intervals only) into a Trill stream.</summary>
    public static IStreamable<Empty, Ev> Ingress(IEnumerable<EventDto> events) =>
        events.OrderBy(e => e.Start)
              .Select(e => StreamEvent.CreateInterval(e.Start, e.End ?? (e.Start + 1),
                  new Ev { Key = e.Key, Value = int.TryParse(e.Label, out var v) ? v : 0 }))
              .ToObservable()
              .ToStreamable();

    static OperatorEntry Unary<T>(string id, string title, int category, EventDto[] input,
        Func<IStreamable<Empty, Ev>, IStreamable<Empty, T>> query,
        [CallerArgumentExpression(nameof(query))] string code = "") =>
        new(id, title, Categories[category], Body(code), ["input"], [input], s => Collect(query(s[0])));

    static OperatorEntry Binary<T>(string id, string title, int category, EventDto[] left, EventDto[] right,
        Func<IStreamable<Empty, Ev>, IStreamable<Empty, Ev>, IStreamable<Empty, T>> query,
        [CallerArgumentExpression(nameof(query))] string code = "") =>
        new(id, title, Categories[category], Body(code), ["left", "right"], [left, right], s => Collect(query(s[0], s[1])));

    /// <summary>Strips the lambda's parameter list and the source indentation of continuation lines.</summary>
    static string Body(string lambda)
    {
        var body = lambda[(lambda.IndexOf("=>", StringComparison.Ordinal) + 2)..].Trim();
        var lines = body.Split('\n').Select(l => l.TrimEnd('\r')).ToArray();
        var indent = lines.Skip(1).Where(l => l.Trim().Length > 0).Select(l => l.Length - l.TrimStart().Length).DefaultIfEmpty(0).Min();
        return string.Join("\n", lines.Take(1).Concat(lines.Skip(1).Select(l => "    " + l[Math.Min(indent, l.Length)..])));
    }

    /// <summary>Runs <paramref name="query"/> and returns its data events as diagram events.</summary>
    // Subscribe, not ToEnumerable: single-threaded browser WASM throws on a
    // blocking wait, and array ingress completes synchronously.
    public static List<EventDto> Collect<T>(IStreamable<Empty, T> query)
    {
        var output = new List<EventDto>();
        query.ToStreamEventObservable().Where(e => e.IsData).Subscribe(e =>
        {
            var (key, label) = Describe(e.Payload);
            long? end = e.EndTime == StreamEvent.InfinitySyncTime ? null : e.EndTime;
            output.Add(new EventDto(e.Kind.ToString(), e.StartTime, end, key, label));
        });
        return output;
    }

    static (string Key, string Label) Describe(object payload) => payload switch
    {
        Ev ev => (ev.Key, ev.Value.ToString()),
        Out o => (o.Key, o.Label),
        Empty => (null, "match"),
        _ => (null, payload?.ToString()),
    };

    static EventDto I(long s, long e, string k, int v) => new("Interval", s, e, k, v.ToString());

    static EventDto P(long t, string k, int v) => new("Interval", t, t + 1, k, v.ToString());
}
