// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
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
    /// the same function differently as different. It ignores the receiver of an instance method call, so the
    /// receivers are compared here. A tree that the comparison cannot handle counts as different.
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
        {
            try
            {
                return EqualityComparer.IsEqual(first, second) && SameReceivers(first, second);
            }
            catch (Exception)
            {
                // IsEqual is not total: it throws for a block's variables, a loop without a continue label, and a receiver
                // that declares a lambda. The detector must not break a query, so such a pair counts as different.
                return false;
            }
        }

        // IsEqual has already matched the two trees' shapes, so their instance calls pair up in visiting order. The
        // receivers are compared as one array, in a lambda over every parameter of the tree.
        private static bool SameReceivers(LambdaExpression first, LambdaExpression second)
            => EqualityComparer.IsEqual(ReceiverFinder.Receivers(first), ReceiverFinder.Receivers(second));

        private sealed class ReceiverFinder : ExpressionVisitor
        {
            private readonly List<Expression> receivers = new List<Expression>();
            private readonly List<ParameterExpression> parameters = new List<ParameterExpression>();

            public static LambdaExpression Receivers(LambdaExpression tree)
            {
                var finder = new ReceiverFinder();
                finder.Visit(tree);
                var array = Expression.NewArrayInit(typeof(object), finder.receivers.Select(r => (Expression)Expression.Convert(r, typeof(object))));
                return Expression.Lambda(array, finder.parameters);
            }

            protected override Expression VisitLambda<T>(Expression<T> node)
            {
                foreach (var p in node.Parameters)
                {
                    if (!this.parameters.Contains(p)) this.parameters.Add(p);
                }

                return base.VisitLambda(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Object != null) this.receivers.Add(node.Object);
                return base.VisitMethodCall(node);
            }
        }
    }
}
