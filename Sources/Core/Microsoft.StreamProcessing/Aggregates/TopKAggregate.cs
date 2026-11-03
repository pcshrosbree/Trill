// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Microsoft.StreamProcessing.Aggregates
{
    internal sealed class TopKAggregate<T> : SortedMultisetAggregateBase<T, List<RankedEvent<T>>>
    {
        private readonly Comparison<T> compiledRankComparer;
        private readonly int k;

        public TopKAggregate(int k, QueryContainer container) : this(k, ComparerExpression<T>.Default, container) { }

        public TopKAggregate(int k, IComparerExpression<T> rankComparer, QueryContainer container)
            : this(k, rankComparer, TieComparer(), container) { }

        public TopKAggregate(int k, IComparerExpression<T> rankComparer, IComparerExpression<T> overallComparer, QueryContainer container)
            : base(ThenOrderBy(Reverse(rankComparer), overallComparer), container)
        {
            Contract.Requires(rankComparer != null);
            Contract.Requires(overallComparer != null);
            Contract.Requires(k > 0);
            this.compiledRankComparer = Reverse(rankComparer).GetCompareExpr().Compile();
            this.k = k;
        }

        /// <summary>
        /// The comparison that orders payloads whose ordering values tie. A payload type with a comparison keeps it.
        /// For a type with none, the default comparer, Comparer{T}.Default, throws for two different payloads unless
        /// one of them implements IComparable. The comparison returned here gives 0 for the same object. Two
        /// payloads that implement IComparable at run time keep the default comparer's answer, and a payload without
        /// IComparable sorts before one with it. Otherwise it orders the payloads member by member, as Trill orders anonymous
        /// types: the payload's public instance fields, then its public instance properties that have a public
        /// getter and no index parameters, each with Trill's default comparison for its type. Unlike the
        /// anonymous-type order, a null member sorts first. Payloads equal in all of these count as copies of one
        /// payload, as equal values of a comparable type do. If the type has no such members, or the comparison of
        /// some member cannot be built, such as for a ReadOnlySpan property, the default comparer is kept.
        /// </summary>
        private static IComparerExpression<T> TieComparer()
        {
            var comparer = ComparerExpression<T>.Default;
            if (!(comparer is GenericComparerExpression<T>) || HasComparison(typeof(T))) return comparer;

            var left = Expression.Parameter(typeof(T), "left");
            var right = Expression.Parameter(typeof(T), "right");
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;
            var members = typeof(T).GetTypeInfo().GetFields(flags).Cast<MemberInfo>()
                .Concat(typeof(T).GetTypeInfo().GetProperties(flags)
                    .Where(p => p.GetIndexParameters().Length == 0 && p.GetGetMethod() != null))
                .ToList();
            if (members.Count == 0) return comparer;

            Expression body = Expression.Constant(0);
            try
            {
                for (int i = members.Count - 1; i >= 0; i--)
                {
                    var compare = CompareMember(Expression.MakeMemberAccess(left, members[i]), Expression.MakeMemberAccess(right, members[i]));
                    body = Expression.Condition(Expression.Equal(compare, Expression.Constant(0)), body, compare);
                }
            }
            catch (Exception e) when (e is ArgumentException || e is NotSupportedException || e is TargetInvocationException)
            {
                // A member type that cannot be a type argument (a byref-like or pointer type), or whose default
                // comparer cannot be made. Keep the old tie comparer, which throws only if two payloads tie.
                return comparer;
            }

            if (!typeof(T).GetTypeInfo().IsValueType)
            {
                // The same object is equal to itself, without reading its members. A payload of a derived type (or a
                // boxed value) may implement IComparable at run time: two such payloads keep the default comparer's
                // answer, and every payload without IComparable sorts before every payload with it. Ordering by that
                // first keeps the comparison transitive when a set holds payloads of both kinds.
                var defaultCompare = comparer.GetCompareExpr().ReplaceParametersInBody(left, right);
                var leftIsComparable = Expression.TypeIs(left, typeof(IComparable));
                var rightIsComparable = Expression.TypeIs(right, typeof(IComparable));
                body = Expression.Condition(
                    Expression.ReferenceEqual(Expression.Convert(left, typeof(object)), Expression.Convert(right, typeof(object))),
                    Expression.Constant(0),
                    Expression.Condition(
                        Expression.AndAlso(leftIsComparable, rightIsComparable),
                        defaultCompare,
                        Expression.Condition(
                            leftIsComparable,
                            Expression.Constant(1),
                            Expression.Condition(rightIsComparable, Expression.Constant(-1), body))));
            }

            return new ComparerExpression<T>(Expression.Lambda<Comparison<T>>(body, left, right));
        }

        /// <summary>
        /// Whether Comparer{T}.Default can compare two values of the type: it implements IComparable{T} or
        /// IComparable, or it is a nullable of such a type.
        /// </summary>
        private static bool HasComparison(Type type)
        {
            var underlying = Nullable.GetUnderlyingType(type);
            if (underlying != null) return HasComparison(underlying);
            return typeof(IComparable).GetTypeInfo().IsAssignableFrom(type)
                || typeof(IComparable<>).MakeGenericType(type).GetTypeInfo().IsAssignableFrom(type);
        }

        /// <summary>
        /// Compares two values of a member with Trill's default comparison for the member's type; for a reference
        /// type, null sorts before any other value.
        /// </summary>
        private static Expression CompareMember(Expression left, Expression right)
        {
            var type = left.Type;
            var comparer = typeof(ComparerExpression<>).MakeGenericType(type).GetTypeInfo().GetProperty("Default").GetValue(null);
            var lambda = (LambdaExpression)comparer.GetType().GetTypeInfo().GetMethod("GetCompareExpr").Invoke(comparer, null);
            var compare = lambda.ReplaceParametersInBody(left, right);
            if (type.GetTypeInfo().IsValueType) return compare;

            var nullObject = Expression.Constant(null, typeof(object));
            var leftIsNull = Expression.ReferenceEqual(Expression.Convert(left, typeof(object)), nullObject);
            var rightIsNull = Expression.ReferenceEqual(Expression.Convert(right, typeof(object)), nullObject);
            return Expression.Condition(
                leftIsNull,
                Expression.Condition(rightIsNull, Expression.Constant(0), Expression.Constant(-1)),
                Expression.Condition(rightIsNull, Expression.Constant(1), compare));
        }

        private static IComparerExpression<T> Reverse(IComparerExpression<T> comparer)
        {
            Contract.Requires(comparer != null);
            var expression = comparer.GetCompareExpr();
            Expression<Comparison<T>> template = (left, right) => CallInliner.Call(expression, right, left);
            var reversedExpression = template.InlineCalls();
            return new ComparerExpression<T>(reversedExpression);
        }

        private static IComparerExpression<T> ThenOrderBy(IComparerExpression<T> comparer1, IComparerExpression<T> comparer2)
        {
            Contract.Requires(comparer1 != null);
            Contract.Requires(comparer2 != null);
            var primary = comparer1.GetCompareExpr();
            var secondary = comparer2.GetCompareExpr();
            Expression<Comparison<T>> template =
                (left, right) =>
                CallInliner.Call(primary, left, right) == 0
                    ? CallInliner.Call(secondary, left, right)
                    : CallInliner.Call(primary, left, right);
            var newExpression = template.InlineCalls();
            return new ComparerExpression<T>(newExpression);
        }

        public override Expression<Func<SortedMultiSet<T>, List<RankedEvent<T>>>> ComputeResult() => set => GetTopK(set);

        private List<RankedEvent<T>> GetTopK(SortedMultiSet<T> set)
        {
            int count = (int)Math.Min(this.k, set.TotalCount);
            var result = new List<RankedEvent<T>>(count);
            int nextRank = 1;
            int outputRank = 1;
            bool first = true;
            T rankValue = default;
            foreach (var value in set.GetEnumerable())
            {
                if (first || this.compiledRankComparer(rankValue, value) != 0)
                {
                    if (result.Count >= count) break;

                    outputRank = nextRank;
                    rankValue = value;
                    first = false;
                }

                // Ranking has gaps on it, this is expected
                // Rank value follows the same as Sql Rank and not Dense_Rank
                result.Add(new RankedEvent<T>(outputRank, value));
                nextRank++;
            }

            return result;
        }
    }
}
