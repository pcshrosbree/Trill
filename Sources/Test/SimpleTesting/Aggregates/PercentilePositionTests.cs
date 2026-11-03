// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq.Expressions;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // Percentiles over states with more than 2^31 - 1 values, built with SortedMultiSet.Add(value, count) rather
    // than from events. The result is computed the same way in row and columnar mode.
    [TestClass]
    public class PercentilePositionTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const long Int32Limit = int.MaxValue;
        private const int TimeoutMilliseconds = 60000;

        [TestMethod, TestCategory("Gated"), Timeout(TimeoutMilliseconds)]
        public void PercentileDiscreteMedianPastInt32Positions()
            => Assert.AreEqual(2.0, Discrete(0.5, Int32Limit, Int32Limit + 2));

        // Controls: the median is at position 2^31 - 1, the last position an int can hold.
        [TestMethod, TestCategory("Gated"), Timeout(TimeoutMilliseconds)]
        public void PercentileDiscreteMedianAtTheLastInt32Position()
        {
            Assert.AreEqual(2.0, Discrete(0.5, Int32Limit - 1, Int32Limit + 1));
            Assert.AreEqual(1.0, Discrete(0.5, Int32Limit, Int32Limit));
        }

        // In descending order the twos come first, and position 3 * 2^30 is among the ones.
        [TestMethod, TestCategory("Gated"), Timeout(TimeoutMilliseconds)]
        public void PercentileDiscreteWithAComparerPastInt32Positions()
            => Assert.AreEqual(1.0, Discrete(0.75, Int32Limit, Int32Limit + 2, (x, y) => y.CompareTo(x)));

        [TestMethod, TestCategory("Gated"), Timeout(TimeoutMilliseconds)]
        public void PercentileContinuousMedianPastInt32Positions()
        {
            Assert.AreEqual(2.0, Continuous(0.5, Int32Limit, Int32Limit + 2));
            Assert.AreEqual(1.5, Continuous(0.5, Int32Limit + 1, Int32Limit + 1));
        }

        // Control: the median interpolates between positions 2^31 - 1 and 2^31, counting from 1.
        [TestMethod, TestCategory("Gated"), Timeout(TimeoutMilliseconds)]
        public void PercentileContinuousMedianAtTheLastInt32Position()
            => Assert.AreEqual(1.5, Continuous(0.5, Int32Limit, Int32Limit));

        // 2^53 + 3 converts to the double 2^53 + 4, one more than the number of values, so the position must be
        // limited to the last value.
        [TestMethod, TestCategory("Gated"), Timeout(TimeoutMilliseconds)]
        public void PercentileDiscreteLargestOfMoreThan2To53Values()
            => Assert.AreEqual(2.0, Discrete(1.0, (1L << 53) + 2, 1));

        [TestMethod, TestCategory("Gated"), Timeout(TimeoutMilliseconds)]
        public void PercentileContinuousLargestOfMoreThan2To53Values()
            => Assert.AreEqual(2.0, Continuous(1.0, (1L << 53) + 3, 1));

        private static double Discrete(double percentile, long ones, long twos, Expression<Comparison<double>> comparer = null)
        {
            var aggregate = comparer == null
                ? new PercentileDiscreteDoubleAggregate(percentile, new QueryContainer())
                : new PercentileDiscreteDoubleAggregate(percentile, new ComparerExpression<double>(comparer), new QueryContainer());
            return aggregate.CalculatePercentile(State(aggregate, ones, twos));
        }

        private static double Continuous(double percentile, long ones, long twos)
        {
            var aggregate = new PercentileContinuousDoubleAggregate(percentile, new QueryContainer());
            return aggregate.CalculatePercentile(State(aggregate, ones, twos));
        }

        private static SortedMultiSet<double> State(IAggregate<double, SortedMultiSet<double>, double> aggregate, long ones, long twos)
            => aggregate.InitialState().Compile()().Add(1.0, ones).Add(2.0, twos);
    }
}
