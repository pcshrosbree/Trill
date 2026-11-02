// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class PercentileDiscreteZeroInput
    {
        public string Key;
        public int Value;
    }

    public abstract class PercentileDiscreteZeroTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal PercentileDiscreteZeroTestsBase(ConfigModifier modifier) : base(modifier) { }

        // The repro from the issue: over 3, 1 and 2, the percentile 0 is the smallest value, 1.
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteAtZeroIsTheSmallestValue()
        {
            var result = Input(Interval(1, 10, 3), Interval(1, 10, 1), Interval(1, 10, 2))
                .Aggregate(w => w.PercentileDiscrete(0.0, e => e.Value));
            CollectionAssert.AreEqual(new[] { "1:1" }, Starts(result));
        }

        // Negative values, so the answer cannot be mistaken for the default 0; the smallest value
        // changes as events start and end.
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteAtZeroFollowsTheSmallestLiveValue()
        {
            var result = Input(Interval(1, 10, -3), Interval(2, 5, -7), Interval(3, 10, -5))
                .Aggregate(w => w.PercentileDiscrete(0.0, e => e.Value));
            CollectionAssert.AreEqual(new[] { "1:-3", "2:-7", "3:-7", "5:-5" }, Starts(result));
        }

        // With a comparison that reverses the order, the percentile 0 is the largest value.
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteAtZeroWithAComparison()
        {
            var result = Input(Interval(1, 10, 3), Interval(1, 10, 1), Interval(1, 10, 2))
                .Aggregate(w => w.PercentileDiscrete((x, y) => y.CompareTo(x), 0.0, e => e.Value));
            CollectionAssert.AreEqual(new[] { "1:3" }, Starts(result));
        }

        // Controls: other percentiles were right before the fix and stay the same.
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteAboveZeroIsUnchanged()
        {
            var input = Input(Interval(1, 10, 3), Interval(1, 10, 1), Interval(1, 10, 2));
            CollectionAssert.AreEqual(new[] { "1:1" }, Starts(input.Aggregate(w => w.PercentileDiscrete(0.01, e => e.Value))));
            CollectionAssert.AreEqual(new[] { "1:2" }, Starts(input.Aggregate(w => w.PercentileDiscrete(0.5, e => e.Value))));
            CollectionAssert.AreEqual(new[] { "1:3" }, Starts(input.Aggregate(w => w.PercentileDiscrete(1.0, e => e.Value))));
        }

        private static IStreamable<Empty, PercentileDiscreteZeroInput> Input(params StreamEvent<PercentileDiscreteZeroInput>[] events)
            => events.ToObservable().ToStreamable();

        private static StreamEvent<PercentileDiscreteZeroInput> Interval(long start, long end, int value)
            => StreamEvent.CreateInterval(start, end, new PercentileDiscreteZeroInput { Key = "a", Value = value });

        private static List<string> Starts(IStreamable<Empty, double> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsStart)
                .Select(e => e.StartTime + ":" + e.Payload.ToString(CultureInfo.InvariantCulture))
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class PercentileDiscreteZeroTestsRow : PercentileDiscreteZeroTestsBase
    {
        public PercentileDiscreteZeroTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class PercentileDiscreteZeroTestsColumnar : PercentileDiscreteZeroTestsBase
    {
        public PercentileDiscreteZeroTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
