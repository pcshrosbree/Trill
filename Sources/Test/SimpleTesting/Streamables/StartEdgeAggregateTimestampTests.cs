// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class StartEdgeAggregateInput
    {
        public string Key;
        public int Value;
    }

    // Aggregates inside GroupApply on a stream that Trill knows has only start edges. Each later result of a group
    // starts when the event that changed it arrives. Row mode is the oracle: each test expects what row mode gives.
    public abstract class StartEdgeAggregateTimestampTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal StartEdgeAggregateTimestampTestsBase(ConfigModifier modifier) : base(modifier) { }

        // Group a's sum becomes 4 at 4, so its first result ends there and its second starts there.
        [TestMethod, TestCategory("Gated")]
        public void SumInsideGroupApply()
            => Assert.AreEqual(
                "End 1 4 a:1; Start 1 a:1; Start 2 b:2; Start 4 a:4",
                Events(Input().GroupApply(e => e.Key, g => g.Sum(e => e.Value), (g, s) => new StartEdgeAggregateInput { Key = g.Key, Value = s })));

        // A third result for a group: each result starts where the one before it ends.
        [TestMethod, TestCategory("Gated")]
        public void CountInsideGroupApplyWithThreeResultsForAGroup()
            => Assert.AreEqual(
                "End 1 4 a:1; End 2 7 b:1; End 4 9 a:2; Start 1 a:1; Start 2 b:1; Start 4 a:2; Start 7 b:2; Start 9 a:3",
                Events(Input(Point(7, "b", 4), Point(9, "a", 5)).GroupApply(e => e.Key, g => g.Count(), (g, n) => new StartEdgeAggregateInput { Key = g.Key, Value = (int)n })));

        // Two events of a group at the same time give one result, which starts then.
        [TestMethod, TestCategory("Gated")]
        public void SumInsideGroupApplyWithTwoEventsAtOneTime()
            => Assert.AreEqual(
                "End 1 4 a:1; Start 1 a:1; Start 2 b:2; Start 4 a:9",
                Events(Input(Point(4, "a", 5)).GroupApply(e => e.Key, g => g.Sum(e => e.Value), (g, s) => new StartEdgeAggregateInput { Key = g.Key, Value = s })));

        // Without a grouping key the start-edge snapshot keeps its state in a field, not in a dictionary entry.
        [TestMethod, TestCategory("Gated")]
        public void SumWithoutAGroupingKey()
            => Assert.AreEqual(
                "End 1 2 :1; End 2 4 :3; Start 1 :1; Start 2 :3; Start 4 :6",
                Events(Input().Sum(e => e.Value).Select(s => new StartEdgeAggregateInput { Key = string.Empty, Value = s })));

        private static IStreamable<Empty, StartEdgeAggregateInput> Input(params StreamEvent<StartEdgeAggregateInput>[] more)
            => new[] { Point(1, "a", 1), Point(2, "b", 2), Point(4, "a", 3) }
                .Concat(more)
                .ToObservable()
                .ToStreamable()
                .AlterEventDuration(StreamEvent.InfinitySyncTime);

        private static StreamEvent<StartEdgeAggregateInput> Point(long time, string key, int value)
            => StreamEvent.CreatePoint(time, new StartEdgeAggregateInput { Key = key, Value = value });

        // The data events, each as "Start start payload", "End start end payload" or "Interval start-end payload",
        // sorted ordinally so that the order of events at one time does not matter, and joined with "; ".
        private static string Events(IStreamable<Empty, StartEdgeAggregateInput> result)
            => string.Join("; ", result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => (e.IsStart ? "Start " + e.StartTime
                    : e.IsEnd ? "End " + e.StartTime + " " + e.EndTime
                    : "Interval " + e.StartTime + "-" + e.EndTime) + " " + e.Payload.Key + ":" + e.Payload.Value)
                .ToEnumerable()
                .OrderBy(s => s, StringComparer.Ordinal));
    }

    [TestClass]
    public class StartEdgeAggregateTimestampTestsRow : StartEdgeAggregateTimestampTestsBase
    {
        public StartEdgeAggregateTimestampTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class StartEdgeAggregateTimestampTestsColumnar : StartEdgeAggregateTimestampTestsBase
    {
        public StartEdgeAggregateTimestampTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
