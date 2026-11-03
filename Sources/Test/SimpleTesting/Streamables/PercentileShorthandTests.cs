// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class PercentileShorthandInput
    {
        public string Key;
        public double Value;
    }

    public abstract class PercentileShorthandTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal PercentileShorthandTestsBase(ConfigModifier modifier) : base(modifier) { }

        private static readonly Expression<Comparison<double>> Reversed = (x, y) => y.CompareTo(x);

        // Events start and end at different times, so the set of live values changes.
        private static IStreamable<Empty, PercentileShorthandInput> Input() => new[]
        {
            StreamEvent.CreateInterval(1, 10, new PercentileShorthandInput { Key = "a", Value = 4 }),
            StreamEvent.CreateInterval(2, 6, new PercentileShorthandInput { Key = "a", Value = 1 }),
            StreamEvent.CreateInterval(3, 10, new PercentileShorthandInput { Key = "a", Value = 9 }),
            StreamEvent.CreateInterval(4, 8, new PercentileShorthandInput { Key = "a", Value = 6 }),
        }.ToObservable().ToStreamable();

        [TestMethod, TestCategory("Gated")]
        public void PercentileContinuousIsTheAggregate()
        {
            var expected = Starts(Input().Aggregate(w => w.PercentileContinuous(0.25, e => e.Value)));
            CollectionAssert.AreEqual(new[] { "1:4", "2:1.75", "3:2.5", "4:3.25", "6:5", "8:5.25" }, expected);
            CollectionAssert.AreEqual(expected, Starts(Input().PercentileContinuous(0.25, e => e.Value)));
        }

        [TestMethod, TestCategory("Gated")]
        public void PercentileContinuousWithAComparisonIsTheAggregate()
        {
            var expected = Starts(Input().Aggregate(w => w.PercentileContinuous(Reversed, 0.25, e => e.Value)));
            CollectionAssert.AreEqual(new[] { "1:4", "2:3.25", "3:6.5", "4:6.75", "6:7.5", "8:7.75" }, expected);
            CollectionAssert.AreEqual(expected, Starts(Input().PercentileContinuous(Reversed, 0.25, e => e.Value)));
        }

        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteIsTheAggregate()
        {
            var expected = Starts(Input().Aggregate(w => w.PercentileDiscrete(0.5, e => e.Value)));
            CollectionAssert.AreEqual(new[] { "1:4", "2:1", "3:4", "4:4", "6:6", "8:4" }, expected);
            CollectionAssert.AreEqual(expected, Starts(Input().PercentileDiscrete(0.5, e => e.Value)));
        }

        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteWithAComparisonIsTheAggregate()
        {
            var expected = Starts(Input().Aggregate(w => w.PercentileDiscrete(Reversed, 0.5, e => e.Value)));
            CollectionAssert.AreEqual(new[] { "1:4", "2:4", "3:4", "4:6", "6:6", "8:9" }, expected);
            CollectionAssert.AreEqual(expected, Starts(Input().PercentileDiscrete(Reversed, 0.5, e => e.Value)));
        }

        // Inside GroupApply, the shorthand is the aggregate of each group.
        [TestMethod, TestCategory("Gated")]
        public void PercentileShorthandsPerGroup()
        {
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 10, new PercentileShorthandInput { Key = "a", Value = 1 }),
                StreamEvent.CreateInterval(1, 10, new PercentileShorthandInput { Key = "b", Value = 5 }),
                StreamEvent.CreateInterval(2, 10, new PercentileShorthandInput { Key = "a", Value = 3 }),
                StreamEvent.CreateInterval(2, 10, new PercentileShorthandInput { Key = "b", Value = 7 }),
            }.ToObservable().ToStreamable();
            var result = input.GroupApply(e => e.Key, g => g.PercentileContinuous(0.5, e => e.Value), (g, p) => g.Key + "=" + p.ToString(CultureInfo.InvariantCulture));
            var starts = result.ToStreamEventObservable().Where(e => e.IsStart).Select(e => e.StartTime + ":" + e.Payload).ToEnumerable().OrderBy(s => s, StringComparer.Ordinal).ToList();
            CollectionAssert.AreEqual(new[] { "1:a=1", "1:b=5", "2:a=2", "2:b=6" }, starts);
        }

        // The arguments are checked as the window aggregates check them.
        [TestMethod, TestCategory("Gated")]
        public void PercentileShorthandArguments()
        {
            IStreamable<Empty, PercentileShorthandInput> none = null;
            Assert.ThrowsException<ArgumentNullException>(() => none.PercentileContinuous(0.5, e => e.Value));
            Assert.ThrowsException<ArgumentNullException>(() => none.PercentileDiscrete(Reversed, 0.5, e => e.Value));
            Assert.ThrowsException<ArgumentNullException>(() => Input().PercentileContinuous(0.5, null));
            Assert.ThrowsException<ArgumentNullException>(() => Input().PercentileDiscrete(0.5, null));
            Assert.ThrowsException<ArgumentNullException>(() => Input().PercentileContinuous(null, 0.5, e => e.Value));
            Assert.ThrowsException<ArgumentNullException>(() => Input().PercentileDiscrete(null, 0.5, e => e.Value));
            Assert.ThrowsException<ArgumentException>(() => Input().PercentileContinuous(1.5, e => e.Value));
            Assert.ThrowsException<ArgumentException>(() => Input().PercentileDiscrete(Reversed, -0.1, e => e.Value));
        }

        private static List<string> Starts(IStreamable<Empty, double> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsStart)
                .Select(e => e.StartTime + ":" + e.Payload.ToString(CultureInfo.InvariantCulture))
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class PercentileShorthandTestsRow : PercentileShorthandTestsBase
    {
        public PercentileShorthandTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class PercentileShorthandTestsColumnar : PercentileShorthandTestsBase
    {
        public PercentileShorthandTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
