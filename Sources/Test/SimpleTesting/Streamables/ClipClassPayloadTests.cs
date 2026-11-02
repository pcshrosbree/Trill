// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // A class payload with default (reference) equality. Columnar code cannot use that equality.
    public sealed class ClipPayload
    {
        public string Key;
        public int Value;
    }

    // A class payload whose equality compares the whole object, through IEquatable<T>.
    public sealed class ClipEquatablePayload : IEquatable<ClipEquatablePayload>
    {
        public string Key;
        public int Value;

        public bool Equals(ClipEquatablePayload other) => other != null && this.Key == other.Key && this.Value == other.Value;

        public override bool Equals(object obj) => Equals(obj as ClipEquatablePayload);

        public override int GetHashCode() => this.Value;
    }

    // A payload that cannot be held in columns: its property has an init-only setter.
    public sealed class ClipRowOnlyPayload
    {
        public int Value { get; init; }
    }

    // ClipEventDuration, and ConsecutivePairs, which clips the stream with itself, on class payloads whose
    // equality columnar code cannot use. Row mode is the oracle: each test expects what row mode gives.
    public abstract class ClipClassPayloadTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const string Pairs = "Start 2 ab; End 2 3 ab; Start 4 bc; End 4 5 bc";

        internal ClipClassPayloadTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestMethod, TestCategory("Gated")]
        public void ConsecutivePairsOnAClassWithDefaultEquality()
        {
            var input = new[] { Point(1, "a", 1), Point(2, "b", 2), Point(4, "c", 3) }.ToObservable().ToStreamable();

            Assert.AreEqual(Pairs, Events(input.ConsecutivePairs((x, y) => x.Key + y.Key)));
        }

        [TestMethod, TestCategory("Gated")]
        public void ConsecutivePairsOnAClassThatImplementsIEquatable()
        {
            var input = new[]
            {
                StreamEvent.CreatePoint(1, new ClipEquatablePayload { Key = "a", Value = 1 }),
                StreamEvent.CreatePoint(2, new ClipEquatablePayload { Key = "b", Value = 2 }),
                StreamEvent.CreatePoint(4, new ClipEquatablePayload { Key = "c", Value = 3 }),
            }.ToObservable().ToStreamable();

            Assert.AreEqual(Pairs, Events(input.ConsecutivePairs((x, y) => x.Key + y.Key)));
        }

        // A left input with only start edges: each left event ends when the first right event after its start starts.
        [TestMethod, TestCategory("Gated")]
        public void ClipEventDurationOnStartEdgesOfAClassWithDefaultEquality()
        {
            var left = new[] { Point(1, "a", 1), Point(6, "b", 2) }.ToObservable().ToStreamable().AlterEventDuration(StreamEvent.InfinitySyncTime);
            var right = new[] { Point(5, "x", 0), Point(7, "y", 0) }.ToObservable().ToStreamable();

            Assert.AreEqual("End 1 5 a; End 6 7 b; Start 1 a; Start 6 b", Events(left.ClipEventDuration(right).Select(e => e.Key), sorted: true));
        }

        // A left input with a start edge and its end edge, which must be matched by payload equality.
        internal static IStreamable<Empty, ClipPayload> LeftEdges()
        {
            var a = new ClipPayload { Key = "a", Value = 1 };
            return new[] { StreamEvent.CreateStart(1, a), StreamEvent.CreateEnd(3, 1, a) }.ToObservable().ToStreamable();
        }

        internal static StreamEvent<ClipPayload> Point(long time, string key, int value)
            => StreamEvent.CreatePoint(time, new ClipPayload { Key = key, Value = value });

        // The data events, each as "Start start payload", "End start end payload" or "Interval start-end payload",
        // joined with "; ". Sorted ordinally if asked, so that the order of events at one time does not matter.
        internal static string Events(IStreamable<Empty, string> result, bool sorted = false)
        {
            var events = result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.IsStart ? "Start " + e.StartTime + " " + e.Payload
                    : e.IsEnd ? "End " + e.StartTime + " " + e.EndTime + " " + e.Payload
                    : "Interval " + e.StartTime + "-" + e.EndTime + " " + e.Payload)
                .ToEnumerable();
            return string.Join("; ", sorted ? events.OrderBy(s => s, StringComparer.Ordinal) : events);
        }
    }

    [TestClass]
    public class ClipClassPayloadTestsRow : ClipClassPayloadTestsBase
    {
        public ClipClassPayloadTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true)) { }

        // In row mode the end edge carries the same object as its start edge, so default equality matches them.
        [TestMethod, TestCategory("Gated")]
        public void ClipEventDurationOnEdgesOfAClassWithDefaultEquality()
        {
            var right = new[] { Point(5, "x", 0) }.ToObservable().ToStreamable();

            Assert.AreEqual("End 1 3 a; Start 1 a", Events(LeftEdges().ClipEventDuration(right).Select(e => e.Key), sorted: true));
        }
    }

    // Columnar mode with fallback to row mode allowed, the default.
    [TestClass]
    public class ClipClassPayloadTestsColumnar : ClipClassPayloadTestsBase
    {
        public ClipClassPayloadTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }

        // Clip falls back to row mode on a left input with only start edges, and the row-mode report says why.
        [TestMethod, TestCategory("Gated")]
        public void ClipReportsItsFallbackToRowMode()
        {
            var reports = new ConcurrentQueue<RowModeReport>();
            Config.RowModeReporter = reports.Enqueue;
            try
            {
                var input = new[] { Point(1, "a", 1), Point(2, "b", 2) }.ToObservable().ToStreamable();
                input.AlterEventDuration(StreamEvent.InfinitySyncTime).ClipEventDuration(input);
            }
            finally
            {
                Config.RowModeReporter = null;
            }

            var clip = reports.Where(r => r.Operator.GetType().Name.StartsWith("ClipJoinStreamable", StringComparison.Ordinal)).ToList();
            Assert.AreEqual(1, clip.Count, string.Join("; ", reports));
            Assert.AreEqual(RowModeReason.NotSupported, clip[0].Reason);
            StringAssert.Contains(clip[0].Detail, "does not have a valid equality operator for columnar mode");
        }

        // A comparer that reads only fields keeps Clip in columnar mode: no row-mode report for it.
        [TestMethod, TestCategory("Gated")]
        public void ClipStaysColumnarWithAFieldComparer()
        {
            var reports = new ConcurrentQueue<RowModeReport>();
            Config.RowModeReporter = reports.Enqueue;
            try
            {
                var input = new[] { Point(1, "a", 1), Point(2, "b", 2) }.ToObservable().ToStreamable()
                    .SetProperty().PayloadEqualityComparer(new EqualityComparerExpression<ClipPayload>((x, y) => x.Key == y.Key && x.Value == y.Value, x => x.Value));
                input.AlterEventDuration(StreamEvent.InfinitySyncTime).ClipEventDuration(input);
            }
            finally
            {
                Config.RowModeReporter = null;
            }

            Assert.AreEqual(0, reports.Count(r => r.Operator.GetType().Name.StartsWith("ClipJoinStreamable", StringComparison.Ordinal)), string.Join("; ", reports));
        }

        // A left end edge read from a columnar batch is a new object, so default equality could not match it to its
        // start edge in row mode either: Clip still refuses the query when it is built.
        [TestMethod, TestCategory("Gated")]
        public void ClipThrowsOnEdgesOfAClassWithDefaultEquality()
        {
            var right = new[] { Point(5, "x", 0) }.ToObservable().ToStreamable();

            var thrown = Assert.ThrowsException<InvalidOperationException>(() => LeftEdges().ClipEventDuration(right));
            StringAssert.Contains(thrown.Message, "PayloadEqualityComparer");
        }

        // The same when only the left input is columnar: Clip then converts it to rows without generating code.
        [TestMethod, TestCategory("Gated")]
        public void ClipThrowsOnEdgesWhenOnlyTheLeftInputIsColumnar()
        {
            var right = new[] { StreamEvent.CreatePoint(5, new ClipRowOnlyPayload { Value = 0 }) }.ToObservable().ToStreamable();
            Assert.IsFalse(right.Properties.IsColumnar);

            Assert.ThrowsException<InvalidOperationException>(() => LeftEdges().ClipEventDuration(right));
        }
    }

    // Columnar mode with fallback to row mode turned off: Clip refuses such a payload when the query is built.
    [TestClass]
    public class ClipClassPayloadTestsColumnarNoFallback : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public ClipClassPayloadTestsColumnarNoFallback()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }

        [TestMethod, TestCategory("Gated")]
        public void ClipThrowsOnStartEdgesWhenFallbackIsOff()
        {
            var input = new[] { ClipClassPayloadTestsBase.Point(1, "a", 1) }.ToObservable().ToStreamable();

            var thrown = Assert.ThrowsException<InvalidOperationException>(
                () => input.AlterEventDuration(StreamEvent.InfinitySyncTime).ClipEventDuration(input));
            StringAssert.Contains(thrown.Message, "does not have a valid equality operator for columnar mode");
            StringAssert.Contains(thrown.Message, "PayloadEqualityComparer");
        }

        // The same when only the left input is columnar, which Clip would otherwise convert to rows.
        [TestMethod, TestCategory("Gated")]
        public void ClipThrowsOnStartEdgesWhenFallbackIsOffAndOnlyTheLeftInputIsColumnar()
        {
            var left = new[] { ClipClassPayloadTestsBase.Point(1, "a", 1) }.ToObservable().ToStreamable().AlterEventDuration(StreamEvent.InfinitySyncTime);
            var right = new[] { StreamEvent.CreatePoint(5, new ClipRowOnlyPayload { Value = 0 }) }.ToObservable().ToStreamable();
            Assert.IsTrue(left.Properties.IsColumnar && left.Properties.IsStartEdgeOnly);
            Assert.IsFalse(right.Properties.IsColumnar);

            Assert.ThrowsException<InvalidOperationException>(() => left.ClipEventDuration(right));
        }

        // A comparer that reads only fields lets Clip run in columnar mode.
        [TestMethod, TestCategory("Gated")]
        public void ClipRunsColumnarWithAFieldComparer()
        {
            var input = new[] { ClipClassPayloadTestsBase.Point(1, "a", 1), ClipClassPayloadTestsBase.Point(2, "b", 2), ClipClassPayloadTestsBase.Point(4, "c", 3) }
                .ToObservable().ToStreamable()
                .SetProperty().PayloadEqualityComparer(new EqualityComparerExpression<ClipPayload>((x, y) => x.Key == y.Key && x.Value == y.Value, x => x.Value));

            var result = input.AlterEventDuration(StreamEvent.InfinitySyncTime).ClipEventDuration(input).Select(e => e.Key);

            Assert.AreEqual("End 1 2 a; End 2 4 b; Start 1 a; Start 2 b; Start 4 c", ClipClassPayloadTestsBase.Events(result, sorted: true));
        }
    }
}
