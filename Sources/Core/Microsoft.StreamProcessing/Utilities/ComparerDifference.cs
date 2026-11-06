// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq.Expressions;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Decides whether two key equality comparers might differ, so that two inputs redistributed with them might
    /// have equal keys on different shards.
    /// </summary>
    /// <remarks>
    /// Both expressions are compared: with different Equals, a hash join can use default equality, which a shared
    /// hash need not respect. <see cref="EqualityComparer.IsEqual"/> ignores parameter names, but counts two trees that compute
    /// the same function differently as different. A tree that the comparison cannot handle counts as different.
    /// </remarks>
    internal static class ComparerDifference
    {
        /// <summary>
        /// Returns true if the two comparers' Equals or hash expressions are not the same tree.
        /// </summary>
        internal static bool MayDiffer<TKey>(IEqualityComparerExpression<TKey> first, IEqualityComparerExpression<TKey> second)
        {
            Contract.Requires(first != null);
            Contract.Requires(second != null);
            return !SameTree(first.GetEqualsExpr().InlineCapturedValues(), second.GetEqualsExpr().InlineCapturedValues())
                || !SameTree(first.GetGetHashCodeExpr().InlineCapturedValues(), second.GetGetHashCodeExpr().InlineCapturedValues());
        }

        /// <summary>
        /// Returns true if the two comparers are the same object, or their Equals and hash expressions are the same
        /// trees as built, before captured values are inlined. Two such comparers compute the same function whenever
        /// they run, even if a captured variable is assigned after the query is built.
        /// </summary>
        internal static bool SameAsBuilt<TKey>(IEqualityComparerExpression<TKey> first, IEqualityComparerExpression<TKey> second)
        {
            Contract.Requires(first != null);
            Contract.Requires(second != null);
            return ReferenceEquals(first, second)
                || (SameTree(first.GetEqualsExpr(), second.GetEqualsExpr()) && SameTree(first.GetGetHashCodeExpr(), second.GetGetHashCodeExpr()));
        }

        /// <summary>
        /// Returns the comparer with its captured values inlined, as <see cref="MayDiffer"/> compares it. A comparer
        /// that is not built from other comparers, captures no value that can be inlined and returns the same trees on
        /// every call is returned itself; otherwise the result is a new comparer.
        /// </summary>
        internal static IEqualityComparerExpression<TKey> Inlined<TKey>(IEqualityComparerExpression<TKey> comparer)
        {
            Contract.Requires(comparer != null);
            if (comparer is IInlinableEqualityComparer composite) return (IEqualityComparerExpression<TKey>)composite.WithInlinedCapturedValues();
            var equals = comparer.GetEqualsExpr().InlineCapturedValues();
            var hash = comparer.GetGetHashCodeExpr().InlineCapturedValues();
            return equals == comparer.GetEqualsExpr() && hash == comparer.GetGetHashCodeExpr()
                ? comparer
                : new EqualityComparerExpression<TKey>(equals, hash);
        }

        /// <summary>
        /// Returns the first comparer if no other comparer differs from it, and null if any differs or there are none.
        /// </summary>
        internal static IEqualityComparerExpression<TKey> Shared<TKey>(IEnumerable<IEqualityComparerExpression<TKey>> comparers)
        {
            Contract.Requires(comparers != null);
            IEqualityComparerExpression<TKey> shared = null;
            foreach (var comparer in comparers)
            {
                if (shared == null) shared = comparer;
                else if (MayDiffer(shared, comparer)) return null;
            }

            return shared;
        }

        private static bool SameTree(LambdaExpression first, LambdaExpression second)
            => EqualityComparer.IsEqual(first, second);
    }

    /// <summary>
    /// A comparer built from other comparers, which <see cref="ComparerDifference.Inlined"/> rebuilds from their inlined
    /// copies so that it keeps its class.
    /// </summary>
    internal interface IInlinableEqualityComparer
    {
        /// <summary>
        /// Returns a comparer of the same class whose parts have their captured values inlined.
        /// </summary>
        object WithInlinedCapturedValues();
    }
}
