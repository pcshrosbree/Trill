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
    public sealed class GroupAggregateInput
    {
        public string Key;
        public int Value;
    }

    // GroupAggregate whose merger returns a string, which cannot be held in columns. Row mode is the oracle: each
    // test expects what row mode gives.
    public abstract class GroupAggregateStringResultTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // Group a's count becomes 2 at 4, so its first result ends there.
        private const string NeverEnd = "End 1 4 a1; Start 1 a1; Start 2 b1; Start 4 a2";

        internal GroupAggregateStringResultTestsBase(ConfigModifier modifier) : base(modifier) { }

        // Trill knows these events never end, so on a stream without a grouping key GroupAggregate uses its
        // grouped-window operator.
        [TestMethod, TestCategory("Gated")]
        public void StringResultOnEventsThatNeverEnd()
            => Assert.AreEqual(
                NeverEnd,
                Events(Input().AlterEventDuration(StreamEvent.InfinitySyncTime).GroupAggregate(e => e.Key, w => w.Count(), (g, n) => g.Key + n)));

        // On point events GroupAggregate maps, aggregates and ungroups instead.
        [TestMethod, TestCategory("Gated")]
        public void StringResultOnPointEvents()
            => Assert.AreEqual(
                "End 1 2 a1; End 2 3 b1; End 4 5 a1; Start 1 a1; Start 2 b1; Start 4 a1",
                Events(Input().GroupAggregate(e => e.Key, w => w.Count(), (g, n) => g.Key + n)));

        // The same grouped-window path with a result that can be held in columns.
        [TestMethod, TestCategory("Gated")]
        public void ClassResultOnEventsThatNeverEnd()
            => Assert.AreEqual(
                NeverEnd,
                Events(Input().AlterEventDuration(StreamEvent.InfinitySyncTime)
                    .GroupAggregate(e => e.Key, w => w.Count(), (g, n) => new GroupAggregateInput { Key = g.Key, Value = (int)n })
                    .Select(r => r.Key + r.Value)));

        internal static IStreamable<Empty, GroupAggregateInput> Input()
            => new[]
            {
                StreamEvent.CreatePoint(1, new GroupAggregateInput { Key = "a", Value = 1 }),
                StreamEvent.CreatePoint(2, new GroupAggregateInput { Key = "b", Value = 2 }),
                StreamEvent.CreatePoint(4, new GroupAggregateInput { Key = "a", Value = 3 }),
            }.ToObservable().ToStreamable();

        // The data events, each as "Start start payload", "End start end payload" or "Interval start-end payload",
        // sorted ordinally so that the order of events at one time does not matter, and joined with "; ".
        private static string Events(IStreamable<Empty, string> result)
            => string.Join("; ", result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.IsStart ? "Start " + e.StartTime + " " + e.Payload
                    : e.IsEnd ? "End " + e.StartTime + " " + e.EndTime + " " + e.Payload
                    : "Interval " + e.StartTime + "-" + e.EndTime + " " + e.Payload)
                .ToEnumerable()
                .OrderBy(s => s, StringComparer.Ordinal));
    }

    [TestClass]
    public class GroupAggregateStringResultTestsRow : GroupAggregateStringResultTestsBase
    {
        public GroupAggregateStringResultTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class GroupAggregateStringResultTestsColumnar : GroupAggregateStringResultTestsBase
    {
        public GroupAggregateStringResultTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }

        // The grouped-window operator runs in row mode, and the row-mode report names the string result type.
        [TestMethod, TestCategory("Gated")]
        public void GroupedWindowReportsTheStringResult()
        {
            var reports = new ConcurrentQueue<RowModeReport>();
            Config.RowModeReporter = reports.Enqueue;
            try
            {
                Input().AlterEventDuration(StreamEvent.InfinitySyncTime).GroupAggregate(e => e.Key, w => w.Count(), (g, n) => g.Key + n);
            }
            finally
            {
                Config.RowModeReporter = null;
            }

            var grouped = GroupedWindowReports(reports);
            Assert.AreEqual(1, grouped.Count, string.Join("; ", reports));
            Assert.AreEqual(RowModeReason.TypeNotColumnar, grouped[0].Reason);
            Assert.AreEqual(typeof(string), grouped[0].NonColumnarType);
        }

        // A result that can be held in columns keeps the grouped-window operator in columnar mode.
        [TestMethod, TestCategory("Gated")]
        public void GroupedWindowStaysColumnarForAClassResult()
        {
            var reports = new ConcurrentQueue<RowModeReport>();
            Config.RowModeReporter = reports.Enqueue;
            try
            {
                Input().AlterEventDuration(StreamEvent.InfinitySyncTime)
                    .GroupAggregate(e => e.Key, w => w.Count(), (g, n) => new GroupAggregateInput { Key = g.Key, Value = (int)n });
            }
            finally
            {
                Config.RowModeReporter = null;
            }

            Assert.AreEqual(0, GroupedWindowReports(reports).Count, string.Join("; ", reports));
        }

        private static System.Collections.Generic.List<RowModeReport> GroupedWindowReports(ConcurrentQueue<RowModeReport> reports)
            => reports.Where(r => r.Operator.GetType().Name.StartsWith("GroupedWindowStreamable", StringComparison.Ordinal)).ToList();
    }
}
