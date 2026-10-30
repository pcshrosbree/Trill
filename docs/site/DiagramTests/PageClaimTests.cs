using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using TrillDocs;

namespace TrillDocs.Tests;

/// <summary>The new type in select.md's overloads: Key and Value are copied from <see cref="Ev"/>, Twice is computed.</summary>
public sealed class EvTwice
{
    public string Key;
    public int Value;
    public int Twice;
}

/// <summary>row-mode.md's property-order claim: First breaks the init-only rule, Second the no-setter rule.</summary>
public sealed class InitThenGetOnly
{
    public int First { get; init; }
    public int Second { get; }
}

/// <summary>row-mode.md's example payload: its read-only field keeps it out of columnar mode.</summary>
public sealed class Reading
{
    public readonly string Sensor;
    public int Value;
}

/// <summary>
/// Pages say things beyond their own diagram: what a drag will do, how other
/// parameters behave, what a substitute does. Each such claim is pinned here,
/// comparing the complete output events (kind, lifetime, key and label).
/// </summary>
[TestClass]
public sealed class PageClaimTests
{
    static EventDto I(long s, long e, string k = "a", int v = 1) => new("Interval", s, e, k, v.ToString());

    static string Show(List<EventDto> events) =>
        string.Join(" ", events.Select(e => $"{e.Kind}[{e.Start},{e.End?.ToString() ?? "inf"}){(e.Key is null ? "" : e.Key + ":")}{e.Label}"));

    static List<EventDto> Run(Func<IStreamable<Empty, Ev>, IStreamable<Empty, Ev>> query, params EventDto[] input)
    {
        Config.ForceRowBasedExecution = true;
        return Operators.Collect(query(Operators.Ingress(input)));
    }

    static List<EventDto> RunEntry(string id, Func<EventDto, EventDto> moveInput)
    {
        var entry = Operators.All.Single(e => e.Id == id);
        return Operators.Run(entry, entry.DefaultInputs.Select(lane => lane.Select(moveInput).ToArray()).ToArray());
    }

    // select.md: "Move an input event and its output moves with it."
    [TestMethod]
    public void SelectOutputMovesWithItsInput() =>
        Assert.AreEqual("Interval[1,5)a:10 Interval[5,11)b:20 Interval[6,8)a:30",
            Show(RunEntry("select", e => e.Label == "2" ? e with { Start = 5, End = 11 } : e)));

    // join.md: "Drag a:30 left until it overlaps a:3 to see a third pair appear."
    [TestMethod]
    public void JoinDragForAThirdPair() =>
        Assert.AreEqual("Interval[2,4)a:1+10 Interval[4,7)b:2+20 Interval[6,8)a:3+30",
            Show(RunEntry("join", e => e.Label == "30" ? e with { Start = 6, End = 8 } : e)));

    // detect.md: "Drag the b at 7 to 9 and the second match disappears ... Drag it to
    // 10, 11 or 12 and a new match forms instead, with the a at 9. From 13 on, that
    // match has expired too, and only the first match remains."
    [TestMethod]
    [DataRow(9, "Interval[2,5)match")]
    [DataRow(10, "Interval[2,5)match Interval[10,13)match")]
    [DataRow(12, "Interval[2,5)match Interval[12,13)match")]
    [DataRow(13, "Interval[2,5)match")]
    public void DetectDragOfTheBAtSeven(int to, string expected) =>
        Assert.AreEqual(expected, Show(RunEntry("detect", e => e.Label == "4" ? e with { Start = to, End = to + 1 } : e)));

    // hopping-window.md: with size a multiple of period, start snaps up and the lifetime is size.
    [TestMethod]
    public void HoppingDivisibleSizeKeepsTheSize() =>
        Assert.AreEqual("Interval[2,6)a:1 Interval[2,6)a:1 Interval[4,8)a:1",
            Show(Run(s => s.HoppingWindowLifetime(4, 2), I(1, 2), I(2, 3), I(4, 5))));

    // hopping-window.md: size larger than period but not a multiple: start and start + size snap separately.
    [TestMethod]
    public void HoppingNonDivisibleSizeSnapsBothEnds() =>
        Assert.AreEqual("Interval[2,6)a:1 Interval[2,8)a:1",
            Show(Run(s => s.HoppingWindowLifetime(5, 2), I(1, 2), I(2, 3))));

    // hopping-window.md: period larger than size: gap events are dropped, kept events last size.
    [TestMethod]
    public void HoppingPeriodLargerThanSizeDropsGapEvents() =>
        Assert.AreEqual("Interval[4,6)a:1",
            Show(Run(s => s.HoppingWindowLifetime(2, 4), I(1, 2), I(3, 4))));

    // multicast.md: "Every branch the function builds from s shares one subscription to the source."
    // The same two branches without Multicast subscribe twice, which shows the count can tell them apart.
    [TestMethod]
    public void MulticastBranchesShareOneSourceSubscription()
    {
        Config.ForceRowBasedExecution = true;
        int subscriptions = 0;
        var source = Observable.Defer(() =>
        {
            subscriptions++;
            return new[] { StreamEvent.CreateInterval(1, 5, new Ev { Key = "a", Value = 3 }) }.ToObservable();
        }).ToStreamable();

        Operators.Collect(source.Multicast(s => s.Where(e => e.Key == "a").Union(s.Where(e => e.Value > 2))));
        Assert.AreEqual(1, subscriptions, "with Multicast");

        subscriptions = 0;
        Operators.Collect(source.Where(e => e.Key == "a").Union(source.Where(e => e.Value > 2)));
        Assert.AreEqual(2, subscriptions, "without Multicast");
    }

    // select-start-time.md: "Drag an input event and its label follows its new start time."
    [TestMethod]
    public void SelectStartTimeLabelFollowsADrag() =>
        Assert.AreEqual("Interval[1,5)a:1 at 1 Interval[5,11)b:2 at 5 Interval[6,8)a:3 at 6",
            Show(RunEntry("select-start-time", e => e.Label == "2" ? e with { Start = 5, End = 11 } : e)));

    // select-many.md: "An empty sequence drops the event." The a:0 event yields no payloads.
    [TestMethod]
    public void SelectManyEmptySequenceDropsTheEvent()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 5, "a", 0), I(2, 6, "b", 2)]);
        Assert.AreEqual("Interval[2,6)b:2.1 Interval[2,6)b:2.2",
            Show(Operators.Collect(input.SelectMany(e => Enumerable.Range(1, e.Value).Select(i => new Out(e.Key, e.Value + "." + i))))));
    }

    // select-start-time.md, select-many.md and select-by-key.md: the start-time and by-key forms
    // "(start, payload)", "(start, key, payload)" and SelectManyByKey's "(key, payload)".
    [TestMethod]
    public void StartTimeAndByKeyForms()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 5, "a", 1), I(3, 9, "b", 2)]);
        Assert.AreEqual("Interval[1,5)a:1@1 Interval[3,9)b:2@3",
            Show(Operators.Collect(input.SelectMany((start, e) => new[] { new Out(e.Key, e.Value + "@" + start) }))));
        Assert.AreEqual("Interval[1,5)a:1@1 in a Interval[3,9)b:2@3 in b",
            Show(Operators.Collect(input.GroupApply(e => e.Key,
                g => g.SelectByKey((start, k, e) => new Out(k.InnerGroup, e.Value + "@" + start + " in " + k.InnerGroup)),
                (g, r) => r))));
        Assert.AreEqual("Interval[1,5)a:1 in a Interval[3,9)b:2 in b",
            Show(Operators.Collect(input.GroupApply(e => e.Key,
                g => g.SelectManyByKey((k, e) => new[] { new Out(k.InnerGroup, e.Value + " in " + k.InnerGroup) }),
                (g, r) => r))));
    }

    // select-by-key.md: "On a stream from ToStreamable() the key type is Empty, so the key says nothing."
    [TestMethod]
    public void SelectByKeyOnAnUngroupedStreamGetsEmpty()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 5, "a", 1)]);
        Assert.AreEqual("Interval[1,5)a:Empty",
            Show(Operators.Collect(input.SelectByKey((Empty k, Ev e) => new Out(e.Key, k.GetType().Name)))));
    }

    // distinct.md: "Without a selector, Distinct compares whole payloads." On string payloads it
    // gives the same output as the diagram's Distinct(e => e.Key).
    [TestMethod]
    public void DistinctWithoutASelectorComparesWholePayloads()
    {
        var entry = Operators.All.Single(e => e.Id == "distinct");
        Config.ForceRowBasedExecution = true;
        Assert.AreEqual(Show(Operators.Run(entry, entry.DefaultInputs)),
            Show(Operators.Collect(Operators.Ingress(entry.DefaultInputs[0]).Select(e => e.Key).Distinct())));
    }

    // select.md: the overloads that build a new type copy the fields the two types share by
    // name and compute the named ones, from a field pair or from a dictionary.
    [TestMethod]
    public void SelectIntoANewTypeCopiesSharedFields()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 5, "a", 3)]);
        Assert.AreEqual("Interval[1,5)a:3/6",
            Show(Operators.Collect(input.Select(() => new EvTwice(), n => n.Twice, o => o.Value * 2)
                .Select(t => new Out(t.Key, t.Value + "/" + t.Twice)))));
        var formulas = new Dictionary<string, System.Linq.Expressions.Expression<Func<Ev, object>>> { ["Twice"] = o => o.Value * 2 };
        Assert.AreEqual("Interval[1,5)a:3/6",
            Show(Operators.Collect(input.Select(() => new EvTwice(), formulas)
                .Select(t => new Out(t.Key, t.Value + "/" + t.Twice)))));
    }

    // row-mode.md: a payload type with a read-only field reports TypeNotColumnar once, from the
    // ingress, with "field Sensor is read-only"; operators after it add no report, except a
    // Where, Select or constant AlterEventDuration fused into the ingress, which reports again
    // (the function overloads are not fused); property rules apply per property; ToString
    // reads "Row mode: <operator> (TypeNotColumnar): field Sensor is read-only"; and with
    // ForceRowBasedExecution there are no reports.
    [TestMethod]
    [DoNotParallelize]
    public void RowModeReporterPageExample()
    {
        var reports = new List<RowModeReport>();
        var source = new[] { StreamEvent.CreateInterval(1, 5, new Reading()) }.ToObservable();
        try
        {
            Config.RowModeReporter = reports.Add;
            Config.ForceRowBasedExecution = false;
            var ingress = source.ToStreamable();
            Assert.AreEqual(1, reports.Count, string.Join("\n", reports));
            Assert.AreSame(ingress, reports[0].Operator);
            Assert.AreEqual(RowModeReason.TypeNotColumnar, reports[0].Reason);
            Assert.AreEqual(typeof(Reading), reports[0].NonColumnarType);
            Assert.AreEqual("field Sensor is read-only", reports[0].Detail);
            StringAssert.StartsWith(reports[0].ToString(), "Row mode: ");
            StringAssert.EndsWith(reports[0].ToString(), " (TypeNotColumnar): field Sensor is read-only");

            // An operator after it that is not fused into it adds no report of its own.
            reports.Clear();
            ingress.ShiftEventLifetime(1).Count();
            Assert.AreEqual(0, reports.Count, string.Join("\n", reports));

            // A Where, Select or AlterEventDuration directly after it is fused into a new
            // ingress, which reports the same reason again.
            foreach (var fused in new Func<IStreamable<Empty, Reading>, object>[]
                { s => s.Where(r => r.Value > 0), s => s.Select(r => r.Value), s => s.AlterEventDuration(2) })
            {
                reports.Clear();
                fused(ingress);
                Assert.AreEqual(1, reports.Count, string.Join("\n", reports));
                Assert.AreNotSame(ingress, reports[0].Operator);
                Assert.AreEqual("field Sensor is read-only", reports[0].Detail);
            }

            // The AlterEventDuration overloads that take a function are not fused.
            reports.Clear();
            ingress.AlterEventDuration(start => 2);
            ingress.AlterEventDuration((start, end) => end - start);
            Assert.AreEqual(0, reports.Count, string.Join("\n", reports));

            // The three property rules are checked per property: the first property, init-only,
            // is reported, although "has no setter" (the second property) is earlier in the table.
            reports.Clear();
            new[] { StreamEvent.CreateInterval(1, 5, new InitThenGetOnly()) }.ToObservable().ToStreamable();
            Assert.AreEqual(1, reports.Count, string.Join("\n", reports));
            Assert.AreEqual("property First has an init-only setter", reports[0].Detail);

            reports.Clear();
            Config.ForceRowBasedExecution = true;
            source.ToStreamable().AlterEventDuration(2);
            Assert.AreEqual(0, reports.Count, string.Join("\n", reports));
        }
        finally
        {
            Config.RowModeReporter = null;
            Config.ForceRowBasedExecution = true;
        }
    }

    // where-not-exists.md: a Join that projects the left payload emits one copy per overlapping right match.
    [TestMethod]
    public void JoinProjectingLeftDuplicatesPerMatch()
    {
        Config.ForceRowBasedExecution = true;
        var left = Operators.Ingress([I(1, 5)]);
        var right = Operators.Ingress([I(2, 4, v: 10), I(2, 4, v: 20)]);
        Assert.AreEqual("Interval[2,4)a:1 Interval[2,4)a:1", Show(Operators.Collect(left.Join(right, l => l.Key, r => r.Key, (l, r) => l))));
    }
}
