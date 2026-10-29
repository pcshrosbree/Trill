using Microsoft.StreamProcessing;
using TrillDocs;

namespace TrillDocs.Tests;

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
