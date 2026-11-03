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
    public abstract class PercentileDiscreteRankTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal PercentileDiscreteRankTestsBase(ConfigModifier modifier) : base(modifier) { }

        // Over the values 1 to 100, the percentiles 0.07, 0.14 and 0.56 are the values 7, 14 and 56.
        // In double arithmetic 0.07 * 100 is
        // 7.000000000000001, 0.14 * 100 is 14.000000000000002 and 0.56 * 100 is 56.00000000000001, so a ceiling
        // taken without care gives 8, 15 and 57.
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteOfAHundredValues()
        {
            var input = Values(100);
            CollectionAssert.AreEqual(new[] { "1:7" }, Starts(input.Aggregate(w => w.PercentileDiscrete(0.07, e => e))));
            CollectionAssert.AreEqual(new[] { "1:14" }, Starts(input.Aggregate(w => w.PercentileDiscrete(0.14, e => e))));
            CollectionAssert.AreEqual(new[] { "1:56" }, Starts(input.Aggregate(w => w.PercentileDiscrete(0.56, e => e))));
        }

        // Control: 0.57 * 100 is 56.99999999999999, just below 57, and the ceiling was already right.
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteJustBelowAWholeProduct()
            => CollectionAssert.AreEqual(new[] { "1:57" }, Starts(Values(100).Aggregate(w => w.PercentileDiscrete(0.57, e => e))));

        // Products that are not whole numbers still round up: 0.071 * 100 is 7.1, so the answer is the 8th value.
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteBetweenTwoValues()
        {
            var input = Values(100);
            CollectionAssert.AreEqual(new[] { "1:8" }, Starts(input.Aggregate(w => w.PercentileDiscrete(0.071, e => e))));
            CollectionAssert.AreEqual(new[] { "1:2" }, Starts(Values(3).Aggregate(w => w.PercentileDiscrete(0.34, e => e))));
            CollectionAssert.AreEqual(new[] { "1:1" }, Starts(Values(3).Aggregate(w => w.PercentileDiscrete(0.33, e => e))));
        }

        // A product just above a whole number by more than rounding error still rounds up: over 1 and 2, 0.5000000000001
        // gives 1.0000000000002, 901 units in the last place above 1, so the answer is the 2nd value.
        [TestMethod, TestCategory("Gated")]
        public void PercentileDiscreteJustAboveAWholeProduct()
            => CollectionAssert.AreEqual(new[] { "1:2" }, Starts(Values(2).Aggregate(w => w.PercentileDiscrete(0.5000000000001, e => e))));

        private static IStreamable<Empty, double> Values(int count)
            => Enumerable.Range(1, count).Select(v => StreamEvent.CreateInterval(1, 10, (double)v)).ToObservable().ToStreamable();

        private static List<string> Starts(IStreamable<Empty, double> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsStart)
                .Select(e => e.StartTime + ":" + e.Payload.ToString(CultureInfo.InvariantCulture))
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class PercentileDiscreteRankTestsRow : PercentileDiscreteRankTestsBase
    {
        public PercentileDiscreteRankTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class PercentileDiscreteRankTestsColumnar : PercentileDiscreteRankTestsBase
    {
        public PercentileDiscreteRankTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
