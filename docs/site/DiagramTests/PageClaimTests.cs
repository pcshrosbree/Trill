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

/// <summary>Pivot with Average: the fields hold the aggregate's double result.</summary>
public sealed class AverageTotals
{
    public string Key;
    public double Odd;
    public double Even;
}

/// <summary>pivot.md: an int key that the int Sum overwrites.</summary>
public sealed class IntKeyed
{
    public int Key;
    public int Value;
}

/// <summary>pivot.md: the output for <see cref="IntKeyed"/>.</summary>
public sealed class IntTotals
{
    public int Key;
    public int Odd;
}

/// <summary>pivot.md: an input whose key and value are properties.</summary>
public sealed class PropEv
{
    public string Key { get; set; }
    public int Value { get; set; }
}

/// <summary>pivot.md: an output whose key and attribute are properties.</summary>
public sealed class PropTotals
{
    public string Key { get; set; }
    public int Odd { get; set; }
}

/// <summary>pivot.md: an object key, which can hold an int Sum although its type differs.</summary>
public sealed class ObjectKeyed
{
    public object Key;
    public int Value;
}

/// <summary>pivot.md: the output for <see cref="ObjectKeyed"/>.</summary>
public sealed class ObjectKeyTotals
{
    public object Key;
    public int Odd;
}

/// <summary>unpivot.md: a value field of type object, which an int input field cannot be unpivoted into.</summary>
public sealed class ObjectTriple
{
    public string Key;
    public string Attribute;
    public object Value;
}

/// <summary>unpivot.md: int fields, which an int value field unpivots.</summary>
public sealed class IntPair
{
    public string Key;
    public int Low;
    public int High;
}

/// <summary>unpivot.md: int? properties, each unpivoted unless it is null.</summary>
public sealed class PropPair
{
    public string Key { get; set; }
    public int? Low { get; set; }
    public int? High { get; set; }
}

/// <summary>unpivot.md: a string field does not fit an int value field and is not unpivoted.</summary>
public sealed class PairWithNote
{
    public string Key;
    public int? Low;
    public string Note;
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

    // select-many-by-key.md: the "(start, key, payload)" overload.
    [TestMethod]
    public void SelectManyByKeyStartTimeForm()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 5, "a", 1), I(3, 9, "b", 2)]);
        Assert.AreEqual("Interval[1,5)a:1@1 in a Interval[3,9)b:2@3 in b",
            Show(Operators.Collect(input.GroupApply(e => e.Key,
                g => g.SelectManyByKey((start, k, e) => new[] { new Out(k.InnerGroup, e.Value + "@" + start + " in " + k.InnerGroup) }),
                (g, r) => r))));
    }

    static IStreamable<Empty, Totals> PivotParity(IStreamable<Empty, Ev> s) =>
        s.Pivot(() => new Totals(), e => e.Key, e => e.Value % 2 == 0 ? "Even" : "Odd", e => e.Value, w => w.Sum(v => v));

    static string ShowEdges(List<EventDto> events) =>
        string.Join(" ", events.Select(e => $"{e.Kind[0]}[{e.Start},{e.End?.ToString() ?? "inf"}){e.Key}:{e.Label}"));

    // pivot.md: "the output changes whenever the set of live events in a group changes". When a:1 [1,6)
    // ends while a:3 [3,8) is alive, the group's sum drops to 3, as GroupApply with a Where per
    // attribute also reports. Until fork #19 was fixed, Pivot reported odd 0 here.
    [TestMethod]
    public void PivotUpdatesAGroupWhenOneEventEnds()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 6, "a", 1), I(3, 8, "a", 3)]);
        Assert.AreEqual("S[1,inf)a:odd 1, even 0 E[1,3)a:odd 1, even 0 S[3,inf)a:odd 4, even 0 E[3,6)a:odd 4, even 0 S[6,inf)a:odd 3, even 0 E[6,8)a:odd 3, even 0",
            ShowEdges(Operators.Collect(PivotParity(input))));
        Assert.AreEqual("S[1,inf)a:odd 1 E[1,3)a:odd 1 S[3,inf)a:odd 4 E[3,6)a:odd 4 S[6,inf)a:odd 3 E[6,8)a:odd 3",
            ShowEdges(Operators.Collect(input.GroupApply(e => e.Key,
                g => g.Where(e => e.Value % 2 == 1).Sum(e => e.Value), (g, s) => new Out(g.Key, "odd " + s)))));
    }

    // pivot.md: the same holds when the events arrive as separate start and end edges (the engine takes
    // a different path for an end edge than for an interval's expiry), and for Average, which
    // reported NaN before the fix.
    [TestMethod]
    public void PivotUpdatesAGroupForEdgesAndForAverage()
    {
        Config.ForceRowBasedExecution = true;
        var edges = new[]
        {
            StreamEvent.CreateStart(1, new Ev { Key = "a", Value = 1 }),
            StreamEvent.CreateStart(3, new Ev { Key = "a", Value = 3 }),
            StreamEvent.CreateEnd(6, 1, new Ev { Key = "a", Value = 1 }),
            StreamEvent.CreateEnd(8, 3, new Ev { Key = "a", Value = 3 }),
        }.ToObservable().ToStreamable();
        StringAssert.Contains(ShowEdges(Operators.Collect(PivotParity(edges))), "S[6,inf)a:odd 3, even 0");

        var intervals = Operators.Ingress([I(1, 6, "a", 1), I(3, 8, "a", 3)]);
        var averages = new List<StreamEvent<AverageTotals>>();
        intervals.Pivot(() => new AverageTotals(), e => e.Key, e => e.Value % 2 == 0 ? "Even" : "Odd", e => e.Value,
                w => w.Average(v => v))
            .ToStreamEventObservable().Where(e => e.IsData).Subscribe(averages.Add);
        Assert.AreEqual(2.0, averages.First(e => e.StartTime == 3 && e.IsStart).Payload.Odd);
        Assert.AreEqual(3.0, averages.First(e => e.StartTime == 6 && e.IsStart).Payload.Odd);
    }

    // pivot.md: when a:5 starts at 6, the moment a:1 ends, one output covers both changes: odd 3 + 5 = 8
    // (before the fix, 5). With a:0 alive instead, the value is even, so odd drops to 0.
    [TestMethod]
    public void PivotUpdatesAGroupWhenOneEventEndsAsAnotherStarts()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 6, "a", 1), I(3, 8, "a", 3), I(6, 8, "a", 5)]);
        StringAssert.Contains(ShowEdges(Operators.Collect(PivotParity(input))), "S[6,inf)a:odd 8, even 0");

        var zero = Operators.Ingress([I(1, 6, "a", 1), I(3, 8, "a", 0)]);
        StringAssert.Contains(ShowEdges(Operators.Collect(PivotParity(zero))), "S[6,inf)a:odd 0, even 0");
    }

    // pivot.md: "If the key's type can also hold the aggregate's result, Pivot fills the key as an
    // attribute field too, which overwrites it: an int key with Sum comes out as 0 when no event's
    // attribute is the key's name."
    [TestMethod]
    public void PivotOverwritesAKeyOfTheAggregateType()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 6, "a", 3)]).Select(e => new IntKeyed { Key = 7, Value = e.Value });
        var output = new List<IntTotals>();
        input.Pivot(() => new IntTotals(), e => e.Key, e => "Odd", e => e.Value, w => w.Sum(v => v))
            .ToStreamEventObservable().Where(e => e.IsData).Subscribe(e => output.Add(e.Payload));
        Assert.AreEqual(0, output[0].Key);
        Assert.AreEqual(3, output[0].Odd);
    }

    // pivot.md: "Give the key a type that cannot hold the aggregate's result". A different type is not
    // enough: an object key can hold an int Sum, so Pivot overwrites it too and the group's key is lost.
    [TestMethod]
    public void PivotOverwritesAnObjectKey()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 6, "a", 3)]).Select(e => new ObjectKeyed { Key = e.Key, Value = e.Value });
        var output = new List<ObjectKeyTotals>();
        input.Pivot(() => new ObjectKeyTotals(), e => e.Key, e => "Odd", e => e.Value, w => w.Sum(v => v))
            .ToStreamEventObservable().Where(e => e.IsData).Subscribe(e => output.Add(e.Payload));
        Assert.AreNotEqual("a", output[0].Key);
        Assert.AreEqual(3, output[0].Odd);
    }

    // pivot.md: "The key selector must name fields or properties of the input, ... and the output type
    // needs fields or properties with the same names."
    [TestMethod]
    public void PivotAcceptsPropertyKeys()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 6, "a", 1), I(3, 8, "a", 3)]).Select(e => new PropEv { Key = e.Key, Value = e.Value });
        var output = new List<string>();
        input.Pivot(() => new PropTotals(), e => e.Key, e => "Odd", e => e.Value, w => w.Sum(v => v))
            .ToStreamEventObservable().Where(e => e.IsStart).Subscribe(e => output.Add(e.StartTime + ":" + e.Payload.Key + ":" + e.Payload.Odd));
        CollectionAssert.AreEqual(new[] { "1:a:1", "3:a:4", "6:a:3" }, output);
    }

    // unpivot.md: other kinds of value field follow other rules; "an int input field with an object
    // value field throws when the query is built".
    [TestMethod]
    public void UnpivotIntFieldToObjectValueThrowsAtBuild()
    {
        var input = Operators.Ingress([I(1, 5, "a", 1)]).Select(e => new IntPair { Key = e.Key, Low = e.Value, High = e.Value });
        Assert.ThrowsExactly<InvalidOperationException>(() =>
            input.Unpivot(() => new ObjectTriple(), p => p.Key, t => t.Attribute, t => t.Value));
    }

    // pivot.md: "The key selector must name fields or properties of the input". A computed key is refused.
    [TestMethod]
    public void PivotRefusesAComputedKey()
    {
        var input = Operators.Ingress([I(1, 5, "a", 1)]);
        Assert.ThrowsExactly<NotImplementedException>(() =>
            input.Pivot(() => new Totals(), e => e.Key + "x", e => "Odd", e => e.Value, w => w.Sum(v => v)));
    }

    // unpivot.md: with an int value field, an int? field "becomes one output event", "a nullable member
    // that is null is skipped", and "a member of another type, such as string, is ignored".
    [TestMethod]
    public void UnpivotSkipsNullAndNonFittingFields()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 5, "a", 1)]);
        Assert.AreEqual("Interval[1,5)a:High 10",
            Show(Operators.Collect(input.Select(e => new Pair { Key = e.Key, Low = null, High = e.Value * 10 })
                .Unpivot(() => new Triple(), p => p.Key, t => t.Attribute, t => t.Value))));
        Assert.AreEqual("Interval[1,5)a:Low 1",
            Show(Operators.Collect(input.Select(e => new PairWithNote { Key = e.Key, Low = e.Value, Note = "n" })
                .Unpivot(() => new Triple(), p => p.Key, t => t.Attribute, t => t.Value))));
    }

    // unpivot.md: with an int value field, "each other input field or property of that type (int) or of
    // its nullable type (int?) becomes one output event", and "a nullable member that is null is skipped".
    [TestMethod]
    public void UnpivotIntFieldsAndNullableProperties()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 5, "a", 1)]);
        Assert.AreEqual("Interval[1,5)a:High 10 Interval[1,5)a:Low 1",
            Show(Operators.Collect(input.Select(e => new IntPair { Key = e.Key, Low = e.Value, High = e.Value * 10 })
                .Unpivot(() => new Triple(), p => p.Key, t => t.Attribute, t => t.Value))));
        Assert.AreEqual("Interval[1,5)a:High 10 Interval[1,5)a:Low 1",
            Show(Operators.Collect(input.Select(e => new PropPair { Key = e.Key, Low = e.Value, High = e.Value * 10 })
                .Unpivot(() => new Triple(), p => p.Key, t => t.Attribute, t => t.Value))));
        Assert.AreEqual("Interval[1,5)a:High 10",
            Show(Operators.Collect(input.Select(e => new PropPair { Key = e.Key, Low = null, High = e.Value * 10 })
                .Unpivot(() => new Triple(), p => p.Key, t => t.Attribute, t => t.Value))));
        Assert.AreEqual("",
            Show(Operators.Collect(input.Select(e => new PropPair { Key = e.Key, Low = null, High = null })
                .Unpivot(() => new Triple(), p => p.Key, t => t.Attribute, t => t.Value))));
    }

    // reduce.md: "GroupApply(keySelector, apply, resultSelector) is ... Map(keySelector) followed by
    // Reduce(apply, resultSelector), with a mapper that passes the input through".
    [TestMethod]
    public void GroupApplyIsMapThenReduce()
    {
        var entry = Operators.All.Single(e => e.Id == "reduce");
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress(entry.DefaultInputs[0]);
        Assert.AreEqual(Show(Operators.Run(entry, entry.DefaultInputs)),
            Show(Operators.Collect(input.GroupApply(e => e.Key, g => g.Count(), (k, c) => new Out(k.Key, "count " + c)))));
    }

    // reduce.md: "Reduce() with no arguments passes the mapper's output through and merges it back into one
    // stream", and "after a Map with a key selector it does the same".
    [TestMethod]
    public void ReduceWithNoArgumentsPassesAKeyedMapThrough()
    {
        Config.ForceRowBasedExecution = true;
        var input = Operators.Ingress([I(1, 5, "a", 1), I(3, 8, "b", 2)]);
        Assert.AreEqual("Interval[1,5)a:1 Interval[3,8)b:2", Show(Operators.Collect(input.Map(s => s, e => e.Key).Reduce())));
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

    static EventDto Tick(long t, string k, int v) => I(t, t + 1, k, v);

    /// <summary>A row-mode stream of <paramref name="events"/>.</summary>
    static IStreamable<Empty, Ev> In(params EventDto[] events)
    {
        Config.ForceRowBasedExecution = true;
        return Operators.Ingress(events);
    }

    static IStreamable<Empty, Ev> Ticks() => In(Operators.All.Single(e => e.Id == "average").DefaultInputs[0]);

    /// <summary>The distinct numbers of window aggregates that the public overloads of <paramref name="name"/> take.</summary>
    static string AggregateArities(string name) => string.Join(",", typeof(Streamable)
        .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
        .Where(m => m.Name == name)
        .Select(m => m.GetParameters().Count(p => p.ParameterType.IsGenericType
            && p.ParameterType.GetGenericTypeDefinition() == typeof(Func<,>)
            && p.ParameterType.GetGenericArguments()[0].IsGenericType
            && p.ParameterType.GetGenericArguments()[0].GetGenericTypeDefinition() == typeof(Window<,>)))
        .Where(n => n > 0).Distinct().OrderBy(n => n));

    static string ShowRanks<T>(IStreamable<Empty, List<RankedEvent<T>>> query)
    {
        var output = new List<string>();
        query.ToStreamEventObservable().Where(e => e.IsData).Subscribe(e =>
            output.Add($"[{e.StartTime},{e.EndTime})" + string.Join(",", e.Payload.Select(r => r.Rank + ":" + r.Payload))));
        return string.Join(" ", output);
    }

    // group-aggregate.md: GroupAggregate gives the same output as GroupApply running Aggregate on each group.
    [TestMethod]
    public void GroupAggregateIsGroupApplyWithAggregate()
    {
        var entry = Operators.All.Single(e => e.Id == "group-aggregate");
        Assert.AreEqual(Show(Operators.Run(entry, entry.DefaultInputs)),
            Show(Operators.Collect(In(entry.DefaultInputs[0]).TumblingWindowLifetime(5).GroupApply(e => e.Key,
                g => g.Aggregate(w => w.Count(), w => w.Sum(e => e.Value), (count, sum) => new Out(null, "count " + count + ", sum " + sum)),
                (g, r) => new Out(g.Key, r.Label)))));
    }

    // group-aggregate.md: "from one to fifteen aggregates"; aggregate.md: "up to fifteen aggregates".
    [TestMethod]
    public void GroupAggregateAndAggregateTakeOneToFifteenAggregates()
    {
        var oneToFifteen = string.Join(",", Enumerable.Range(1, 15));
        Assert.AreEqual(oneToFifteen, AggregateArities("GroupAggregate"));
        Assert.AreEqual(oneToFifteen, AggregateArities("Aggregate"));
    }

    // group-aggregate.md no longer lists this as a known issue: on a stream whose events never end, a merger that
    // returns a string gives the same events in columnar mode as in row mode.
    [TestMethod]
    [DoNotParallelize]
    public void GroupAggregateStringResultOnStartEdgesInBothModes()
    {
        const string expected = "Start[1,inf)a 1: Start[2,inf)b 1: End[1,4)a 1: Start[4,inf)a 2: End[2,7)b 1: Start[7,inf)b 2: End[4,9)a 2: Start[9,inf)a 3:";
        var input = Operators.All.Single(e => e.Id == "average").DefaultInputs[0];
        try
        {
            foreach (var rowMode in new[] { true, false })
            {
                Config.ForceRowBasedExecution = rowMode;
                Assert.AreEqual(expected,
                    Show(Operators.Collect(Operators.Ingress(input).AlterEventDuration(StreamEvent.InfinitySyncTime)
                        .GroupAggregate(e => e.Key, w => w.Count(), (g, n) => g.Key + " " + n))),
                    rowMode ? "row mode" : "columnar mode");
            }
        }
        finally
        {
            Config.ForceRowBasedExecution = true;
        }
    }

    // group-aggregate.md: "The output is the same as GroupApply with Aggregate on each group", also in columnar
    // mode on a stream whose events never end, where later results of a group start when they change (4, 7 and 9).
    // (The GroupApply form used to repeat the group's first start time there; that known issue is fixed.)
    [TestMethod]
    [DoNotParallelize]
    public void GroupAggregateAndGroupApplyAgreeOnStartTimesInColumnarMode()
    {
        const string expected = "Start[1,inf)a:1 Start[2,inf)b:1 End[1,4)a:1 Start[4,inf)a:2 End[2,7)b:1 Start[7,inf)b:2 End[4,9)a:2 Start[9,inf)a:3";
        var input = Operators.All.Single(e => e.Id == "average").DefaultInputs[0];
        try
        {
            Config.ForceRowBasedExecution = false;
            var columnar = Operators.Ingress(input).AlterEventDuration(StreamEvent.InfinitySyncTime);
            Assert.AreEqual(expected,
                Show(Operators.Collect(columnar.GroupAggregate(e => e.Key, w => w.Count(), (g, n) => new Ev { Key = g.Key, Value = (int)n }))));
            Assert.AreEqual(expected,
                Show(Operators.Collect(columnar.GroupApply(e => e.Key, g => g.Aggregate(w => w.Count()), (g, n) => new Ev { Key = g.Key, Value = (int)n }))));
        }
        finally
        {
            Config.ForceRowBasedExecution = true;
        }
    }

    // aggregate.md: "Average(e => e.Value) is Aggregate(w => w.Average(e => e.Value))", and the same for Sum.
    [TestMethod]
    public void SingleAggregatesAreAggregateWithOneWindowAggregate()
    {
        var input = Ticks().TumblingWindowLifetime(5);
        Assert.AreEqual("Interval[5,10)2 Interval[10,15)4.5", Show(Operators.Collect(input.Aggregate(w => w.Average(e => e.Value)))));
        Assert.AreEqual(Show(Operators.Collect(input.Average(e => e.Value))), Show(Operators.Collect(input.Aggregate(w => w.Average(e => e.Value)))));
        Assert.AreEqual(Show(Operators.Collect(input.Sum(e => e.Value))), Show(Operators.Collect(input.Aggregate(w => w.Sum(e => e.Value)))));
    }

    // average.md: with a nullable selector, nulls are left out, and a window whose values are all null gives null.
    [TestMethod]
    public void AverageLeavesOutNulls()
    {
        var input = In(Tick(1, "a", 1), Tick(2, "b", 2), Tick(3, "a", 4), Tick(6, "b", 5)).TumblingWindowLifetime(5);
        Assert.AreEqual("Interval[5,10)2.5 Interval[10,15)null",
            Show(Operators.Collect(input.Average(e => e.Key == "b" ? (int?)null : e.Value)
                .Select(a => new Out(null, a == null ? "null" : a.Value.ToString())))));
    }

    // average.md: "A window with no events in it gives no output."
    [TestMethod]
    public void AverageGivesNoOutputForAnEmptyWindow() =>
        Assert.AreEqual("Interval[2,4)1 Interval[8,10)3",
            Show(Operators.Collect(In(Tick(1, "a", 1), Tick(7, "a", 3)).TumblingWindowLifetime(2).Average(e => e.Value))));

    // min.md: a comparison reverses the order, so Min with (x, y) => y.CompareTo(x) gives the largest value;
    // nulls are left out, and a window whose values are all null gives null.
    [TestMethod]
    public void MinWithAComparisonAndWithNulls()
    {
        var hopping = Ticks().HoppingWindowLifetime(4, 2);
        Assert.AreEqual("Start[2,inf)2 End[2,4)2 Start[4,inf)3 End[4,6)3 Start[6,inf)3 End[6,8)3 Start[8,inf)4 End[8,10)4 Start[10,inf)5 End[10,12)5 Start[12,inf)5 End[12,14)5",
            Show(Operators.Collect(hopping.Min(e => e.Value, (x, y) => y.CompareTo(x)))));
        var tumbling = In(Tick(1, "a", 1), Tick(2, "b", 2), Tick(6, "a", 5)).TumblingWindowLifetime(5);
        Assert.AreEqual("Interval[5,10)2 Interval[10,15)null",
            Show(Operators.Collect(tumbling.Min(e => e.Key == "a" ? (int?)null : e.Value)
                .Select(m => new Out(null, m == null ? "null" : m.Value.ToString())))));
    }

    // max.md: after TumblingWindowLifetime(4), one maximum per window; a comparison reverses the order;
    // nulls are left out, and a window whose values are all null gives null.
    [TestMethod]
    public void MaxPerWindowWithAComparisonAndWithNulls()
    {
        Assert.AreEqual("Interval[4,8)3 Interval[8,12)4 Interval[12,16)5",
            Show(Operators.Collect(Ticks().TumblingWindowLifetime(4).Max(e => e.Value))));
        var entry = Operators.All.Single(e => e.Id == "max");
        Assert.AreEqual("Start[1,inf)1 End[1,3)1 Start[3,inf)1 End[3,4)1 Start[4,inf)1 End[4,6)1 Start[6,inf)2 End[6,7)2 Start[7,inf)2 End[7,8)2",
            Show(Operators.Collect(In(entry.DefaultInputs[0]).Max(e => e.Value, (x, y) => y.CompareTo(x)))));
        var tumbling = In(Tick(1, "a", 1), Tick(2, "b", 2), Tick(6, "a", 5)).TumblingWindowLifetime(5);
        Assert.AreEqual("Interval[5,10)2 Interval[10,15)null",
            Show(Operators.Collect(tumbling.Max(e => e.Key == "a" ? (int?)null : e.Value)
                .Select(m => new Out(null, m == null ? "null" : m.Value.ToString())))));
    }

    // top-k.md: with fewer than k events alive the list is shorter; a comparison reverses the order.
    [TestMethod]
    public void TopKShortListAndComparison()
    {
        var input = Ticks().TumblingWindowLifetime(5);
        Assert.AreEqual("Interval[5,10)1. a:3, 2. b:2, 3. a:1 Interval[10,15)1. a:5, 2. b:4",
            Show(Operators.Collect(input.TopK(e => e.Value, 3))));
        Assert.AreEqual("Interval[5,10)1. a:1, 2. b:2 Interval[10,15)1. b:4, 2. a:5",
            Show(Operators.Collect(input.TopK(e => e.Value, (x, y) => y.CompareTo(x), 2))));
    }

    // top-k.md: on a payload that can be compared, such as int, tied values share a rank, the next rank
    // skips, and all the values tied for the last place are kept, so the list can be longer than k.
    [TestMethod]
    public void TopKTiesShareARank()
    {
        var values = In(Tick(1, "a", 5), Tick(2, "a", 3), Tick(3, "a", 3), Tick(4, "a", 1)).Select(e => e.Value).TumblingWindowLifetime(5);
        Assert.AreEqual("[5,10)1:5,2:3,2:3", ShowRanks(values.TopK(2)));
        Assert.AreEqual("[5,10)1:5,2:3,2:3,4:1", ShowRanks(values.TopK(4)));
    }

    // top-k.md, known issue: when two live events tie on the ordering value and Trill has no comparison for the
    // payload type, as for Ev, TopK throws ArgumentException; int (TopKTiesShareARank), string, anonymous-type
    // and Tuple payloads work. This pins the engine defect as it is today: when it is fixed, this test fails,
    // and top-k.md's known-issue paragraph (and this test) must be updated.
    [TestMethod]
    public void TopKKnownIssueTiesOnAPayloadThatCannotBeCompared()
    {
        var tie = In(Tick(1, "a", 5), Tick(2, "b", 5)).TumblingWindowLifetime(5);
        var thrown = Assert.ThrowsExactly<ArgumentException>(() => Operators.Collect(tie.TopK(e => e.Value, 2)));
        StringAssert.Contains(thrown.Message, "At least one object must implement IComparable");
        Assert.AreEqual("[5,10)1:a5,1:b5", ShowRanks(tie.Select(e => e.Key + e.Value).TopK(s => s.Length, 2)));
        Assert.AreEqual("[5,10)1:{ Key = a, Value = 5 },1:{ Key = b, Value = 5 }",
            ShowRanks(tie.Select(e => new { e.Key, e.Value }).TopK(p => p.Value, 2)));
        Assert.AreEqual("[5,10)1:(a, 5),1:(b, 5)", ShowRanks(tie.Select(e => Tuple.Create(e.Key, e.Value)).TopK(p => p.Item2, 2)));
        var noTie = In(Tick(1, "a", 5), Tick(2, "b", 4)).TumblingWindowLifetime(5);
        Assert.AreEqual("Interval[5,10)1. a:5, 2. b:4", Show(Operators.Collect(noTie.TopK(e => e.Value, 2))));
    }

    // percentile-continuous.md: the two definitions at p = 0.25 over 1, 2, 3 and over 4, 5.
    [TestMethod]
    public void PercentileDefinitions() =>
        Assert.AreEqual("Interval[5,10)1.5 1 Interval[10,15)4.25 4",
            Show(Operators.Collect(Ticks().TumblingWindowLifetime(5).Aggregate(
                w => w.PercentileContinuous(0.25, e => e.Value), w => w.PercentileDiscrete(0.25, e => e.Value),
                (c, d) => new Out(null, c + " " + d)))));

    // percentile-continuous.md: a comparison that reverses the order makes the percentile count from the
    // largest value, so 0.25 under it is 0.75 without it; a percentile outside 0 to 1 throws when the query is built.
    [TestMethod]
    public void PercentileComparisonAndRange()
    {
        var input = Ticks().TumblingWindowLifetime(5);
        var reversed = Show(Operators.Collect(input.Aggregate(w => w.PercentileContinuous((x, y) => y.CompareTo(x), 0.25, e => e.Value))));
        Assert.AreEqual("Interval[5,10)2.5 Interval[10,15)4.75", reversed);
        Assert.AreEqual(reversed, Show(Operators.Collect(input.Aggregate(w => w.PercentileContinuous(0.75, e => e.Value)))));
        Assert.ThrowsExactly<ArgumentException>(() => input.Aggregate(w => w.PercentileContinuous(1.5, e => e.Value)));
        Assert.ThrowsExactly<ArgumentException>(() => input.Aggregate(w => w.PercentileDiscrete(-0.1, e => e.Value)));
    }

    // percentile-continuous.md, known issue: PercentileDiscrete(0.0) gives 0, not the smallest value, while
    // PercentileContinuous(0.0) and Min give the smallest value. This pins the engine defect as it is today: when it
    // is fixed, this test fails, and percentile-continuous.md's known-issue paragraph (and this test) must be updated.
    [TestMethod]
    public void PercentileKnownIssueDiscreteAtZero()
    {
        var input = Ticks().TumblingWindowLifetime(5);
        Assert.AreEqual("Interval[5,10)0 1 1 Interval[10,15)0 4 4",
            Show(Operators.Collect(input.Aggregate(
                w => w.PercentileDiscrete(0.0, e => e.Value), w => w.PercentileContinuous(0.0, e => e.Value), w => w.Min(e => e.Value),
                (d, c, m) => new Out(null, d + " " + c + " " + m)))));
    }

    // session-timeout-window.md: an event's own end is ignored: the long a:1 is cut, the short b:2 extended.
    [TestMethod]
    public void SessionIgnoresTheEventsOwnEnd() =>
        Assert.AreEqual("Interval[1,5)a:1 Interval[2,5)b:2",
            Show(Operators.Collect(In(I(1, 10, "a", 1), I(2, 3, "b", 2)).SessionTimeoutWindow(3))));

    // session-timeout-window.md: inside GroupApply, each key has its own sessions.
    [TestMethod]
    public void SessionPerKeyInsideGroupApply() =>
        Assert.AreEqual("Start[1,inf)a:1 Start[2,inf)b:2 End[1,4)a:1 Start[4,inf)a:3 End[2,5)b:2 End[4,7)a:3 Start[7,inf)b:4 Start[9,inf)a:5 End[7,10)b:4 End[9,12)a:5",
            Show(Operators.Collect(Ticks().GroupApply(e => e.Key, g => g.SessionTimeoutWindow(3), (g, e) => e))));

    // session-timeout-window.md: the first session's cut point is the first multiple of maxDuration at least
    // maxDuration after its first event; a session that continues a cut one takes the first multiple of
    // maxDuration after its second event; a session with one event ends at its timeout.
    [TestMethod]
    public void SessionMaxDurationCutsAtAMultiple()
    {
        var continued = In(new long[] { 1, 2, 3, 4, 5, 6, 7, 9, 10, 11 }.Select(t => Tick(t, "a", (int)t)).ToArray());
        Assert.AreEqual("Interval[1,8)a:1 Interval[2,8)a:2 Interval[3,8)a:3 Interval[4,8)a:4 Interval[5,8)a:5 Interval[6,8)a:6 Interval[7,8)a:7 Interval[9,12)a:9 Interval[10,12)a:10 Interval[11,12)a:11",
            Show(Operators.Collect(continued.SessionTimeoutWindow(3, 4))));
        var fromOne = In(Enumerable.Range(1, 9).Select(t => Tick(t, "a", t)).ToArray());
        Assert.AreEqual("Interval[1,8)a:1 Interval[2,8)a:2 Interval[3,8)a:3 Interval[4,8)a:4 Interval[5,8)a:5 Interval[6,8)a:6 Interval[7,8)a:7 Interval[8,12)a:8 Interval[9,12)a:9",
            Show(Operators.Collect(fromOne.SessionTimeoutWindow(3, 4))));
        var fromZero = In(Enumerable.Range(0, 9).Select(t => Tick(t, "a", t)).ToArray());
        Assert.AreEqual("Interval[0,4)a:0 Interval[1,4)a:1 Interval[2,4)a:2 Interval[3,4)a:3 Interval[4,8)a:4 Interval[5,8)a:5 Interval[6,8)a:6 Interval[7,8)a:7 Interval[8,11)a:8",
            Show(Operators.Collect(fromZero.SessionTimeoutWindow(3, 4))));
    }

    // session-timeout-window.md, known issue: a session that begins after a timeout takes its cut point from
    // its second event, so the session that starts at 7 with its second event at 9 is cut at 16 and lasts 9
    // ticks, though its timeout would end it at 18; with its second event at 8 it is cut at 12. This pins the engine defect as it is today: when it
    // is fixed, this test fails, and session-timeout-window.md's known-issue paragraph (and this test) must be updated.
    [TestMethod]
    public void SessionKnownIssueCutPointFromTheSecondEvent()
    {
        var secondAtNine = In(new long[] { 1, 2, 4, 7, 9, 10, 11, 12, 13, 14, 15 }.Select(t => Tick(t, "a", (int)t)).ToArray());
        Assert.AreEqual("Interval[1,7)a:1 Interval[2,7)a:2 Interval[4,7)a:4 Interval[7,16)a:7 Interval[9,16)a:9 Interval[10,16)a:10 Interval[11,16)a:11 Interval[12,16)a:12 Interval[13,16)a:13 Interval[14,16)a:14 Interval[15,16)a:15",
            Show(Operators.Collect(secondAtNine.SessionTimeoutWindow(3, 4))));
        var secondAtEight = In(new long[] { 1, 2, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15 }.Select(t => Tick(t, "a", (int)t)).ToArray());
        Assert.AreEqual("Interval[1,7)a:1 Interval[2,7)a:2 Interval[4,7)a:4 Interval[7,12)a:7 Interval[8,12)a:8 Interval[9,12)a:9 Interval[10,12)a:10 Interval[11,12)a:11 Interval[12,16)a:12 Interval[13,16)a:13 Interval[14,16)a:14 Interval[15,16)a:15",
            Show(Operators.Collect(secondAtEight.SessionTimeoutWindow(3, 4))));
    }

    /// <summary>One-tick events at 1, 2, 4, 7 and 9 to 15, with a punctuation at 7 just before the event at 7 if asked.</summary>
    static IStreamable<Empty, Ev> SessionInput(bool punctuationAtSeven)
    {
        var events = new List<StreamEvent<Ev>>();
        foreach (var t in new long[] { 1, 2, 4, 7, 9, 10, 11, 12, 13, 14, 15 })
        {
            if (t == 7 && punctuationAtSeven) events.Add(StreamEvent.CreatePunctuation<Ev>(7));
            events.Add(StreamEvent.CreateInterval(t, t + 1, new Ev { Key = "a", Value = (int)t }));
        }
        return events.ToObservable().ToStreamable();
    }

    /// <summary>Each event's start and end as "start-end", from intervals and end edges.</summary>
    static string Lifetimes(List<EventDto> events) =>
        string.Join(" ", events.Where(e => e.Kind != "Start").Select(e => e.Start + "-" + e.End));

    // session-timeout-window.md, known issue: with a punctuation at 7 before the event at 7, the session that starts
    // at 7 takes its cut point from 7 and ends at 12, inside GroupApply and in columnar mode; in row mode on a
    // stream with no grouping key it still ends at 16. This pins the engine behaviour as it is today: when it
    // changes, this test fails, and session-timeout-window.md (and this test) must be updated.
    [TestMethod]
    [DoNotParallelize]
    public void SessionKnownIssueCutPointAfterAPunctuation()
    {
        const string fromFirst = "1-7 2-7 4-7 7-12 9-12 10-12 11-12 12-16 13-16 14-16 15-16";
        try
        {
            Config.ForceRowBasedExecution = true;
            Assert.AreEqual(fromFirst, Lifetimes(Operators.Collect(
                SessionInput(punctuationAtSeven: true).GroupApply(e => 0, g => g.SessionTimeoutWindow(3, 4), (g, e) => e))));
            Assert.AreEqual("1-7 2-7 4-7 7-16 9-16 10-16 11-16 12-16 13-16 14-16 15-16",
                Lifetimes(Operators.Collect(SessionInput(punctuationAtSeven: true).SessionTimeoutWindow(3, 4))));

            Config.ForceRowBasedExecution = false;
            Assert.AreEqual(fromFirst, Lifetimes(Operators.Collect(SessionInput(punctuationAtSeven: true).SessionTimeoutWindow(3, 4))));
        }
        finally
        {
            Config.ForceRowBasedExecution = true;
        }
    }

    // session-timeout-window.md, known issue: a timeout of 0 is not refused. In row mode with no grouping key
    // the output is empty; inside GroupApply, and in columnar mode, every event comes out and ends as it
    // starts. This pins the engine defect as it is
    // today: when it is fixed, this test fails, and session-timeout-window.md's known-issue paragraph (and
    // this test) must be updated.
    [TestMethod]
    [DoNotParallelize]
    public void SessionKnownIssueZeroTimeout()
    {
        // Row mode with no grouping key, as the diagrams run: no output at all.
        Assert.AreEqual("", Show(Operators.Collect(Ticks().SessionTimeoutWindow(0))));

        // Row mode inside GroupApply: every event comes out, and each one ends as it starts.
        var grouped = Operators.Collect(Ticks().GroupApply(e => e.Key, g => g.SessionTimeoutWindow(0), (g, e) => e));
        var groupedEnds = grouped.Where(e => e.Kind == "End").ToList();
        Assert.AreEqual(5, groupedEnds.Count, string.Join(" ", grouped));
        Assert.IsTrue(groupedEnds.All(e => e.End == e.Start), string.Join(" ", grouped));

        // Columnar mode: every event comes out, and each one ends as it starts.
        var input = Operators.All.Single(e => e.Id == "average").DefaultInputs[0];
        try
        {
            Config.ForceRowBasedExecution = false;
            var columnar = Operators.Collect(Operators.Ingress(input).SessionTimeoutWindow(0));
            var ends = columnar.Where(e => e.Kind == "End").ToList();
            Assert.AreEqual(5, ends.Count, string.Join(" ", columnar));
            Assert.IsTrue(ends.All(e => e.End == e.Start), string.Join(" ", columnar));
        }
        finally
        {
            Config.ForceRowBasedExecution = true;
        }
    }

    // point-at-end.md: an event that never ends gives nothing; one sent as a start edge and an end edge gives
    // its point when the end edge arrives.
    [TestMethod]
    public void PointAtEndNeedsAnEnd()
    {
        Assert.AreEqual("", Show(Operators.Collect(Ticks().AlterEventDuration(StreamEvent.InfinitySyncTime).PointAtEnd())));
        Config.ForceRowBasedExecution = true;
        var edges = new[]
        {
            StreamEvent.CreateStart(1, new Ev { Key = "a", Value = 1 }),
            StreamEvent.CreateEnd(5, 1, new Ev { Key = "a", Value = 1 }),
        }.ToObservable().ToStreamable();
        Assert.AreEqual("Interval[5,6)a:1", Show(Operators.Collect(edges.PointAtEnd())));
    }

    // point-at-end.md: after a tumbling window and an aggregate, one point per window, at the window's end.
    [TestMethod]
    public void PointAtEndAfterAWindowedAggregate() =>
        Assert.AreEqual("Interval[8,9)3 Interval[12,13)1 Interval[16,17)1",
            Show(Operators.Collect(Ticks().TumblingWindowLifetime(4).Count().PointAtEnd())));

    static IStreamable<Empty, Out> Pairs(IStreamable<Empty, Ev> s) =>
        s.ConsecutivePairs((first, second) => new Out(second.Key, first.Value + "," + second.Value));

    // consecutive-pairs.md: only start times count; the events' ends are ignored.
    [TestMethod]
    public void ConsecutivePairsUseStartTimesOnly() =>
        Assert.AreEqual("Start[3,inf)b:1,2 End[3,4)b:1,2 Start[6,inf)a:2,3 End[6,7)a:2,3",
            Show(Operators.Collect(Pairs(In(I(1, 5, "a", 1), I(3, 9, "b", 2), I(6, 8, "a", 3))))));

    // consecutive-pairs.md: events that start together are not paired with each other; each of them is paired
    // with each event of the previous start time.
    [TestMethod]
    public void ConsecutivePairsAtTheSameStartTime() =>
        Assert.AreEqual("Start[3,inf)a:2,3 Start[3,inf)a:1,3 Start[3,inf)b:2,4 Start[3,inf)b:1,4 End[3,4)a:2,3 End[3,4)a:1,3 End[3,4)b:2,4 End[3,4)b:1,4 Start[5,inf)a:4,5 Start[5,inf)a:3,5 End[5,6)a:4,5 End[5,6)a:3,5",
            Show(Operators.Collect(Pairs(In(Tick(1, "a", 1), Tick(1, "b", 2), Tick(3, "a", 3), Tick(3, "b", 4), Tick(5, "a", 5))))));

    // consecutive-pairs.md: inside GroupApply, pairs are formed within each key.
    [TestMethod]
    public void ConsecutivePairsPerKeyInsideGroupApply() =>
        Assert.AreEqual("Start[4,inf)a:1,3 End[4,5)a:1,3 Start[7,inf)b:2,4 End[7,8)b:2,4 Start[9,inf)a:3,5 End[9,10)a:3,5",
            Show(Operators.Collect(Ticks().GroupApply(e => e.Key,
                g => g.ConsecutivePairs((first, second) => new Out(second.Key, first.Value + "," + second.Value)),
                (g, r) => r))));

    // consecutive-pairs.md: "In columnar mode, when Trill's columnar code cannot use the payload type's equality,
    // as for the diagram's Ev, that clip runs in row mode, and the pairs are the same as in row mode." The pairs are the diagram's,
    // and the row-mode report names the clip.
    [TestMethod]
    [DoNotParallelize]
    public void ConsecutivePairsInColumnarModeOnAClassPayload()
    {
        var input = Operators.All.Single(e => e.Id == "consecutive-pairs").DefaultInputs[0];
        var reports = new System.Collections.Concurrent.ConcurrentQueue<RowModeReport>();
        try
        {
            Config.ForceRowBasedExecution = false;
            Config.RowModeReporter = reports.Enqueue;
            Assert.AreEqual("Start[2,inf)b:1,2 End[2,3)b:1,2 Start[4,inf)a:2,3 End[4,5)a:2,3 Start[7,inf)b:3,4 End[7,8)b:3,4 Start[9,inf)a:4,5 End[9,10)a:4,5",
                Show(Operators.Collect(Pairs(Operators.Ingress(input)))));
        }
        finally
        {
            Config.RowModeReporter = null;
            Config.ForceRowBasedExecution = true;
        }

        var clips = reports.Where(r => r.Operator.GetType().Name.StartsWith("ClipJoinStreamable", StringComparison.Ordinal)).ToList();
        Assert.IsTrue(clips.Count > 0, string.Join("; ", reports));
        Assert.IsTrue(clips.All(r => r.Reason == RowModeReason.NotSupported), string.Join("; ", clips));
    }
}
