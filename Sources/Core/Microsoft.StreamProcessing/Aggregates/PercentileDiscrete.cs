// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class PercentileDiscreteDoubleAggregate : SortedMultisetAggregateBase<double, double>
    {
        private readonly double percentile;

        public PercentileDiscreteDoubleAggregate(double percentile, QueryContainer container)
            : this(percentile, ComparerExpression<double>.Default, container) { }

        public PercentileDiscreteDoubleAggregate(double percentile, IComparerExpression<double> rankComparer, QueryContainer container)
            : base(rankComparer, container)
        {
            Contract.Requires(rankComparer != null);
            Contract.Requires(percentile >= 0.0 && percentile <= 1.0);
            this.percentile = percentile;
        }

        public override Expression<Func<SortedMultiSet<double>, double>> ComputeResult() => set => CalculatePercentile(set);

        public double CalculatePercentile(SortedMultiSet<double> set)
        {
            double result = default;
            if (set.TotalCount > 0)
            {
                // The value at position ceiling(percentile * count), counting from 1. For a percentile of 0
                // that position is 0, so take the first value instead, as for any percentile just above 0.
                int rank = Math.Max(0, (int)CeilingIgnoringRoundingError(this.percentile * set.TotalCount) - 1);

                var enumerator = set.GetEnumerable().GetEnumerator();
                for (int index = 0; index <= rank; index++) enumerator.MoveNext();
                result = enumerator.Current;
            }

            return result;
        }

        /// <summary>
        /// The ceiling of percentile * count, ignoring rounding error. A percentile such as 0.07 has no exact double
        /// value, and 0.07 * 100 comes out as 7.000000000000001, whose plain ceiling, 8, would pick the 8th value
        /// instead of the 7th. Storing the percentile as a double and multiplying it by the count can each introduce
        /// rounding error of at most half a unit in the last place, so for normal (not subnormal) values the combined
        /// relative error is at most 2^-52. (A tiny product whose relative error is larger still gives the first value,
        /// through the clamp to position 0.)
        /// A product just above a whole number is taken to be that whole number if the relative gap is no more than
        /// twice that bound (2^-51). This is a deliberate tolerance: it cannot tell rounding error from a percentile
        /// set that close on purpose, so 0.5 + 2^-52 over two values, whose product is exactly 1 + 2^-51, also gives
        /// the 1st value.
        /// </summary>
        private static double CeilingIgnoringRoundingError(double product)
        {
            const double allowance = 4.4408920985006262E-16; // 2^-51
            double floor = Math.Floor(product);
            return product - floor <= allowance * Math.Max(1.0, floor) ? floor : Math.Ceiling(product);
        }
    }
 }
