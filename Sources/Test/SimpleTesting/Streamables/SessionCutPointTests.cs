// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class SessionCutPointInput
    {
        public string Key;
        public int Value;
    }

    public abstract class SessionCutPointTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // One-tick events at 1, 2, 4, 7 and 9 to 15, with a timeout of 3 and a maxDuration of 4. The events at 1,
        // 2 and 4 form a session that ends at its timeout, 7. The event at 7 starts the next session, whose cut
        // point is the first multiple of 4 at least 4 after 7, which is 12. The event at 12 continues the
        // session in a new one, cut at 16.
        private static readonly long[] AtTheTimeout = { 1, 2, 4, 7, 9, 10, 11, 12, 13, 14, 15 };
        private static readonly string[] CutAtTwelve = { "1-7", "2-7", "4-7", "7-12", "9-12", "10-12", "11-12", "12-16", "13-16", "14-16", "15-16" };

        // A session that starts long after the previous one timed out: the event at 20 starts it, so its cut
        // point is 24, before its timeout at 26.
        private static readonly long[] AfterAGap = { 1, 2, 4, 20, 22, 23 };
        private static readonly string[] CutAtTwentyFour = { "1-7", "2-7", "4-7", "20-24", "22-24", "23-24" };

        // One-tick events at 1, 5, 9, 11, 15 and 19, with a timeout of 5 and a maxDuration of 6. The first session
        // is cut at 12, the first multiple of 6 at least 6 after 1. The event at 15 continues it in a new session,
        // whose cut point is 18, the first multiple of 6 after 15. The event at 19 continues that one in turn, cut
        // at 24.
        private static readonly long[] AfterACut = { 1, 5, 9, 11, 15, 19 };
        private static readonly string[] CutAtEighteen = { "1-12", "5-12", "9-12", "11-12", "15-18", "19-24" };

        // One-tick events at 1, 5, 9, 11, 20 and 24, with a timeout of 5 and a maxDuration of 6, and a punctuation
        // at 12, after the first session's cut point but within the timeout of its last event. The punctuation
        // closes that session at 12. The event at 20 comes after the timeout, so it starts a new session, whose cut
        // point is 30, the first multiple of 6 at least 6 after 20; the events at 20 and 24 end at their timeout, 29.
        private static readonly long[] AfterAPunctuationAtACut = { 1, 5, 9, 11, 20, 24 };
        private static readonly string[] TimeoutAtTwentyNine = { "1-12", "5-12", "9-12", "11-12", "20-29", "24-29" };

        internal SessionCutPointTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestMethod, TestCategory("Gated")]
        public void SessionStartsAfterATimeoutAfterAPunctuationAtACut()
            => Same(TimeoutAtTwentyNine, Lifetimes(Input(AfterAPunctuationAtACut, 20, 12).SessionTimeoutWindow(5, 6)));

        [TestMethod, TestCategory("Gated")]
        public void SessionStartsAfterATimeoutAfterAPunctuationAtACutInsideGroupApply()
            => Same(TimeoutAtTwentyNine, Lifetimes(Grouped(Input(AfterAPunctuationAtACut, 20, 12), 5, 6)));

        [TestMethod, TestCategory("Gated")]
        public void SessionStartsAfterATimeoutAfterAPunctuationAtACutOnAPartitionedStream()
            => Same(TimeoutAtTwentyNine, Partitioned(AfterAPunctuationAtACut, 5, 6, 20, 12));

        // As above, but the next event comes exactly at the first session's timeout, 16: it still starts a new
        // session, cut at 24, so the events at 16 and 18 end at their timeout, 23.
        [TestMethod, TestCategory("Gated")]
        public void SessionStartsAtTheTimeoutAfterAPunctuationAtACutInsideGroupApply()
        {
            var expected = new[] { "1-12", "5-12", "9-12", "11-12", "16-23", "18-23" };
            Same(expected, Lifetimes(Grouped(Input(new long[] { 1, 5, 9, 11, 16, 18 }, 16, 12), 5, 6)));
        }

        // As above with no punctuation, but with key b's event at 13: it closes a's first session at 12 in the same
        // way. b's own session ends at its timeout, 18.
        [TestMethod, TestCategory("Gated")]
        public void SessionStartsAfterATimeoutAfterAnotherKeyClosesItAtACutInsideGroupApply()
        {
            var events = AfterAPunctuationAtACut.Select(t => Interval(t, "a"))
                .Concat(new[] { Interval(13, "b") })
                .OrderBy(e => e.StartTime)
                .ToArray();
            var result = Lifetimes(Grouped(events.ToObservable().ToStreamable(), 5, 6), withKey: true);
            var expected = TimeoutAtTwentyNine.Select(s => "a:" + s).Concat(new[] { "b:13-18" }).OrderBy(s => s, StringComparer.Ordinal).ToArray();
            Same(expected, result.OrderBy(s => s, StringComparer.Ordinal).ToArray());
        }

        // The same input with no punctuation, as a control: the event at 20 closes the first session itself.
        [TestMethod, TestCategory("Gated")]
        public void SessionStartsAfterATimeoutAfterACutWithoutAPunctuation()
        {
            Same(TimeoutAtTwentyNine, Lifetimes(Input(AfterAPunctuationAtACut, null).SessionTimeoutWindow(5, 6)));
            Same(TimeoutAtTwentyNine, Lifetimes(Grouped(Input(AfterAPunctuationAtACut, null), 5, 6)));
            Same(TimeoutAtTwentyNine, Partitioned(AfterAPunctuationAtACut, 5, 6));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventAfterACut()
            => Same(CutAtEighteen, Lifetimes(Input(AfterACut, null).SessionTimeoutWindow(5, 6)));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventAfterACutAndAPunctuation()
            => Same(CutAtEighteen, Lifetimes(Input(AfterACut, 15).SessionTimeoutWindow(5, 6)));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventInsideGroupApplyAfterACut()
            => Same(CutAtEighteen, Lifetimes(Input(AfterACut, null).GroupApply(e => e.Key, g => g.SessionTimeoutWindow(5, 6), (g, e) => e)));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventOnAPartitionedStreamAfterACut()
            => Same(CutAtEighteen, Partitioned(AfterACut, 5, 6));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventAtTheTimeout()
            => Same(CutAtTwelve, Lifetimes(Input(AtTheTimeout, null).SessionTimeoutWindow(3, 4)));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventAtTheTimeoutAfterAPunctuation()
            => Same(CutAtTwelve, Lifetimes(Input(AtTheTimeout, 7).SessionTimeoutWindow(3, 4)));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventAfterAGap()
            => Same(CutAtTwentyFour, Lifetimes(Input(AfterAGap, null).SessionTimeoutWindow(3, 4)));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventAfterAGapAndAPunctuation()
            => Same(CutAtTwentyFour, Lifetimes(Input(AfterAGap, 20).SessionTimeoutWindow(3, 4)));

        // With a grouping key: GroupApply on the payload's key.
        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventInsideGroupApply()
            => Same(CutAtTwelve, Lifetimes(Grouped(Input(AtTheTimeout, null))));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventInsideGroupApplyAfterAPunctuation()
            => Same(CutAtTwelve, Lifetimes(Grouped(Input(AtTheTimeout, 7))));

        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventInsideGroupApplyAfterAGap()
            => Same(CutAtTwentyFour, Lifetimes(Grouped(Input(AfterAGap, null))));

        // Two keys: b's events do not move a's cut point, and each key's session takes its cut point from its
        // own first event.
        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventOfEachKey()
        {
            var events = AtTheTimeout.Select(t => Interval(t, "a"))
                .Concat(new long[] { 3, 5, 8, 9, 11 }.Select(t => Interval(t, "b")))
                .OrderBy(e => e.StartTime)
                .ToArray();
            var result = Lifetimes(Grouped(events.ToObservable().ToStreamable()), withKey: true);

            // b: 3 and 5 form a session that ends at 8, its timeout and its cut point. b's event at 8 is the first
            // event at or after 8, so it ends that session and starts the next one, whose cut point is 12, before
            // its timeout at 14.
            var b = new[] { "b:3-8", "b:5-8", "b:8-12", "b:9-12", "b:11-12" };
            var expected = CutAtTwelve.Select(s => "a:" + s).Concat(b).OrderBy(s => s, StringComparer.Ordinal).ToArray();
            Same(expected, result.OrderBy(s => s, StringComparer.Ordinal).ToArray());
        }

        // A partitioned stream uses its own pipe.
        [TestMethod, TestCategory("Gated")]
        public void SessionCutPointFromTheFirstEventOnAPartitionedStream()
            => Same(CutAtTwelve, Partitioned(AtTheTimeout, 3, 4));

        // One-tick events in partition 0, through the partitioned session pipe, with a punctuation at
        // punctuationTime just before the event at punctuationAt if one is given.
        private static string[] Partitioned(long[] times, long timeout, long maxDuration, long? punctuationAt = null, long? punctuationTime = null)
        {
            var events = new List<PartitionedStreamEvent<int, SessionCutPointInput>>();
            foreach (var t in times)
            {
                if (t == punctuationAt) events.Add(PartitionedStreamEvent.CreatePunctuation<int, SessionCutPointInput>(0, punctuationTime ?? t));
                events.Add(PartitionedStreamEvent.CreateInterval(0, t, t + 1, new SessionCutPointInput { Key = "a", Value = (int)t }));
            }

            return events
                .ToObservable()
                .ToStreamable()
                .SessionTimeoutWindow(timeout, maxDuration)
                .ToStreamEventObservable()
                .Where(e => e.IsInterval || e.IsEnd)
                .Select(e => new { e.StartTime, e.EndTime })
                .ToEnumerable()
                .OrderBy(e => e.StartTime).ThenBy(e => e.EndTime)
                .Select(e => e.StartTime + "-" + e.EndTime)
                .ToArray();
        }

        // Compares the lifetimes as one string, so a failure shows the whole output.
        private static void Same(string[] expected, string[] actual)
            => Assert.AreEqual(string.Join(" ", expected), string.Join(" ", actual));

        private static IStreamable<Empty, SessionCutPointInput> Grouped(IStreamable<Empty, SessionCutPointInput> input, long timeout = 3, long maxDuration = 4)
            => input.GroupApply(e => e.Key, g => g.SessionTimeoutWindow(timeout, maxDuration), (g, e) => e);

        // One-tick events for key a, with a punctuation at punctuationTime (by default punctuationAt) just before
        // the event at punctuationAt if one is given.
        private static IStreamable<Empty, SessionCutPointInput> Input(long[] times, long? punctuationAt, long? punctuationTime = null)
        {
            var events = new List<StreamEvent<SessionCutPointInput>>();
            foreach (var t in times)
            {
                if (t == punctuationAt) events.Add(StreamEvent.CreatePunctuation<SessionCutPointInput>(punctuationTime ?? t));
                events.Add(Interval(t, "a"));
            }
            return events.ToObservable().ToStreamable();
        }

        private static StreamEvent<SessionCutPointInput> Interval(long t, string key)
            => StreamEvent.CreateInterval(t, t + 1, new SessionCutPointInput { Key = key, Value = (int)t });

        // Each event's lifetime as start-end, from intervals and end edges, ordered by start and then end.
        private static string[] Lifetimes(IStreamable<Empty, SessionCutPointInput> result, bool withKey = false)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsInterval || e.IsEnd)
                .Select(e => new { e.Payload.Key, e.StartTime, e.EndTime })
                .ToEnumerable()
                .OrderBy(e => e.StartTime).ThenBy(e => e.EndTime)
                .Select(e => (withKey ? e.Key + ":" : string.Empty) + e.StartTime + "-" + e.EndTime)
                .ToArray();
    }

    [TestClass]
    public class SessionCutPointTestsRow : SessionCutPointTestsBase
    {
        public SessionCutPointTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class SessionCutPointTestsColumnar : SessionCutPointTestsBase
    {
        public SessionCutPointTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
