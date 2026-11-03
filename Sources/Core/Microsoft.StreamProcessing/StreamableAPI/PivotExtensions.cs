// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Static class for transformations to Streamables
    /// </summary>
    public static partial class Streamable
    {
        /// <summary>
        ///
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TInput"></typeparam>
        /// <typeparam name="TOutput"></typeparam>
        /// <typeparam name="TGroupKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <typeparam name="TAggValue"></typeparam>
        /// <typeparam name="TState"></typeparam>
        /// <param name="inputStreamable"></param>
        /// <param name="initializer"></param>
        /// <param name="keySelector"></param>
        /// <param name="attributeSelector"></param>
        /// <param name="valueSelector"></param>
        /// <param name="aggregate"></param>
        /// <returns></returns>
        public static IStreamable<TKey, TOutput> Pivot<TKey, TInput, TOutput, TGroupKey, TValue, TAggValue, TState>(
            this IStreamable<TKey, TInput> inputStreamable,
            Expression<Func<TOutput>> initializer,
            Expression<Func<TInput, TGroupKey>> keySelector,
            Expression<Func<TInput, string>> attributeSelector,
            Expression<Func<TInput, TValue>> valueSelector,
            Func<Window<CompoundGroupKey<TKey, TGroupKey>, TValue>, IAggregate<TValue, TState, TAggValue>> aggregate) where TOutput : new()
        {
            bool sourceHasNullableValues = IsNullable(typeof(TValue));
            if (initializer == null) throw new ArgumentNullException(nameof(initializer));

            var window = new Window<CompoundGroupKey<TKey, TGroupKey>, TValue>(inputStreamable.Properties.GroupNested(keySelector).Select<TValue>(valueSelector, false, false));
            var agg = aggregate(window);
            var valueAgg = agg.TransformInput(valueSelector);
            var outputPublicFields =
                typeof(TOutput).GetTypeInfo()
                .GetFields(BindingFlags.Public | BindingFlags.Instance)
                .Select(f => Tuple.Create(f.Name, f.FieldType, (MemberInfo)f, !sourceHasNullableValues && IsNullable(f.FieldType)))
                .Concat(
                typeof(TOutput).GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.GetIndexParameters().Length == 0)
                .Select(f => Tuple.Create(f.Name, f.PropertyType, (MemberInfo)f, !sourceHasNullableValues && IsNullable(f.PropertyType))))
                .Where(m => m.Item2.GetTypeInfo().IsAssignableFrom(typeof(TAggValue))).ToArray();
            var aggArray = outputPublicFields.Select(
                m => valueAgg.ApplyFilter(
                    Expression.Lambda<Func<TInput, bool>>(
                        Expression.Equal(attributeSelector.Body, Expression.Constant(m.Item1)),
                        attributeSelector.Parameters)));

            var initialState = Expression.Lambda<Func<TState[]>>(Expression.NewArrayInit(typeof(TState), aggArray.Select(a => a.InitialState().Body)));

            var state = Expression.Parameter(typeof(TState[]), "state");
            var time = Expression.Parameter(typeof(long), "time");
            var input = Expression.Parameter(typeof(TInput), "input");
            var accumulator = Expression.Lambda<Func<TState[], long, TInput, TState[]>>(
                Expression.NewArrayInit(
                    typeof(TState),
                    aggArray.Select((a, i) => a.Accumulate().ReplaceParametersInBody(Expression.ArrayIndex(state, Expression.Constant(i)), time, input))), state, time, input);
            var deaccumulator = Expression.Lambda<Func<TState[], long, TInput, TState[]>>(
                Expression.NewArrayInit(
                    typeof(TState),
                    aggArray.Select((a, i) => a.Deaccumulate().ReplaceParametersInBody(Expression.ArrayIndex(state, Expression.Constant(i)), time, input))), state, time, input);

            var state1 = Expression.Parameter(typeof(TState[]), "state1");
            var state2 = Expression.Parameter(typeof(TState[]), "state2");
            var difference = Expression.Lambda<Func<TState[], TState[], TState[]>>(
                Expression.NewArrayInit(
                    typeof(TState),
                    aggArray.Select((a, i) => a.Difference().ReplaceParametersInBody(Expression.ArrayIndex(state1, Expression.Constant(i)), Expression.ArrayIndex(state2, Expression.Constant(i))))), state1, state2);

            var resultSelector = Expression.Lambda<Func<TState[], TAggValue[]>>(
                Expression.NewArrayInit(
                    typeof(TAggValue),
                    aggArray.Select((a, i) => a.ComputeResult().ReplaceParametersInBody(Expression.ArrayIndex(state1, Expression.Constant(i))))), state1);
            var newAgg = GeneratedAggregate.Create(initialState, accumulator, deaccumulator, difference, resultSelector);

            if (initializer.Body as NewExpression == null) throw new ArgumentException("Initializer must return a constructor expression.", nameof(initializer));

            var groupkey = Expression.Parameter(typeof(GroupSelectorInput<TGroupKey>), "groupkey");
            var aggvalues = Expression.Parameter(typeof(TAggValue[]), "aggvalue");
            var keyAssignments = new List<MemberAssignment>();

            // Key selector magic!
            switch (keySelector.Body.NodeType)
            {
                case ExpressionType.MemberAccess:
                {
                    var selector = keySelector.Body as MemberExpression;
                    if (selector.Expression.NodeType == ExpressionType.Parameter)
                    {
                        keyAssignments.Add(Expression.Bind(
                            typeof(TOutput).GetTypeInfo().GetMember(selector.Member.Name).Single(),
                            Expression.PropertyOrField(groupkey, "Key")));
                    }
                    else throw new NotImplementedException();
                    break;
                }
                case ExpressionType.MemberInit:
                {
                    var memberInit = keySelector.Body as MemberInitExpression;
                    foreach (var init in memberInit.Bindings)
                    {
                        if (init.BindingType != MemberBindingType.Assignment) throw new NotImplementedException();
                        var assign = init as MemberAssignment;
                            if (assign.Expression is MemberExpression rightSide && rightSide.Expression.NodeType == ExpressionType.Parameter)
                            {
                                keyAssignments.Add(Expression.Bind(
                                    typeof(TOutput).GetTypeInfo().GetMember(assign.Member.Name).Single(),
                                    Expression.PropertyOrField(Expression.PropertyOrField(groupkey, "Key"), rightSide.Member.Name)));
                            }
                            else throw new NotImplementedException();
                        }
                    break;
                }
                case ExpressionType.New:
                {
                    var newInit = keySelector.Body as NewExpression;
                    if (newInit.Type.IsAnonymousTypeName())
                    {
                        for (int i = 0; i < newInit.Arguments.Count; i++)
                        {
                            var leftSide = newInit.Members[i];
                            if (newInit.Arguments[i] is MemberExpression rightSide)
                            {
                                keyAssignments.Add(Expression.Bind(
                                    typeof(TOutput).GetTypeInfo().GetMember(leftSide.Name).Single(),
                                    Expression.PropertyOrField(Expression.PropertyOrField(groupkey, "Key"), rightSide.Member.Name)));
                            }
                            else throw new NotImplementedException();
                        }
                    }
                    else throw new NotImplementedException();
                    break;
                }
                default:
                    throw new NotImplementedException();
            }

            var constructor = Expression.MemberInit(
                initializer.Body as NewExpression,
                keyAssignments.Concat(
                    outputPublicFields.Select(
                        (a, i) => Expression.Bind(
                            a.Item3,
                            a.Item4
                                ? (Expression)Expression.New(typeof(Nullable<>).MakeGenericType(typeof(TValue)).GetTypeInfo().GetConstructor(new[] { typeof(TValue) }), Expression.ArrayIndex(aggvalues, Expression.Constant(i)))
                                : Expression.ArrayIndex(aggvalues, Expression.Constant(i))))));
            var resultConstructor = Expression.Lambda<Func<GroupSelectorInput<TGroupKey>, TAggValue[], TOutput>>(
                constructor, groupkey, aggvalues);

            return inputStreamable.GroupAggregate(keySelector, newAgg, resultConstructor);
        }

        /// <summary>
        /// Rotates a stream of payload objects into a stream of key-attribute-value triples corresponding to the data from the input payloads.
        /// </summary>
        /// <typeparam name="TKey">The type of the grouping key of the input streamable.</typeparam>
        /// <typeparam name="TInput">The type of the payload of the input streamable.</typeparam>
        /// <typeparam name="TPivotKey">The type of the unpivot key.</typeparam>
        /// <typeparam name="TValue">The type of the value component of the unpivot operation.</typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="inputStreamable">The source streamable to unpivot.</param>
        /// <param name="initializer">A constructor describing how to create output values.</param>
        /// <param name="keySelector">A selector function returning the unpivot key.</param>
        /// <param name="attributeSelector">A selector function stating what field in the return type holds desired unpivoted attributes.</param>
        /// <param name="valueSelector">A selector function stating what field in the return type holds desired unpivoted values.</param>
        /// <returns>A stream of key-attribute-value triples corresponding to the original data stream.</returns>
        public static IStreamable<TKey, TResult> Unpivot<TKey, TInput, TPivotKey, TValue, TResult>(
            this IStreamable<TKey, TInput> inputStreamable,
            Expression<Func<TResult>> initializer,
            Expression<Func<TInput, TPivotKey>> keySelector,
            Expression<Func<TResult, string>> attributeSelector,
            Expression<Func<TResult, TValue>> valueSelector) where TResult : new()
        {
            Invariant.IsNotNull(inputStreamable, nameof(inputStreamable));
            Invariant.IsNotNull(initializer, nameof(initializer));
            Invariant.IsNotNull(keySelector, nameof(keySelector));
            Invariant.IsNotNull(attributeSelector, nameof(attributeSelector));
            Invariant.IsNotNull(valueSelector, nameof(valueSelector));

            if (!(initializer.Body is NewExpression newExpression)) throw new ArgumentException("Initializer must return a constructor expression.", nameof(initializer));
            if (!(attributeSelector.Body is MemberExpression attributeField)) throw new ArgumentException("Attribute selector expression must refer to a single field in the return type.");
            if (!(valueSelector.Body is MemberExpression valueField)) throw new ArgumentException("Value selector expression must refer to a single field in the return type.");

            return inputStreamable.SelectMany(UnpivotSelector<TInput, TPivotKey, TValue, TResult>.Create(keySelector, newExpression, attributeField, valueField));
        }

        // Builds Unpivot's selector as one expression that adds no captured delegate of its own, so columnar code
        // generation can write it as C#. (Expressions the caller passes in, such as the key selector, are kept as they
        // are.) For each member that fits the value field, in the order properties by name, then fields by name, the
        // selector yields one result unless the member is null.
        private static class UnpivotSelector<TInput, TPivotKey, TValue, TResult>
        {
            public static Expression<Func<TInput, IEnumerable<TResult>>> Create(Expression<Func<TInput, TPivotKey>> keySelector, NewExpression newExpression, MemberExpression attributeField, MemberExpression valueField)
            {
                var input = Expression.Parameter(typeof(TInput), "input");
                var keyAssignments = new List<MemberAssignment>();
                Expression body = null;
                var alwaysPresent = new List<Expression>();

                var keyFields = new HashSet<string>();

                // Key selector magic!
                switch (keySelector.Body.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        {
                            var selector = keySelector.Body as MemberExpression;
                            keyAssignments.Add(Expression.Bind(
                                typeof(TResult).GetTypeInfo().GetMember(selector.Member.Name).Single(),
                                Expression.PropertyOrField(input, selector.Member.Name)));
                            keyFields.Add(selector.Member.Name);
                            break;
                        }
                    case ExpressionType.MemberInit:
                        {
                            var memberInit = keySelector.Body as MemberInitExpression;
                            foreach (var init in memberInit.Bindings)
                            {
                                if (init.BindingType != MemberBindingType.Assignment) throw new NotSupportedException("Unpivot operation currently requires key member assignments to be simple field expressions.");
                                var singleInit = init as MemberAssignment;

                                if (singleInit.Expression is MemberExpression rightSide)
                                {
                                    keyAssignments.Add(Expression.Bind(
                                        typeof(TResult).GetTypeInfo().GetMember(singleInit.Member.Name).Single(),
                                        Expression.PropertyOrField(input, rightSide.Member.Name)));
                                    keyFields.Add(rightSide.Member.Name);
                                }
                                else
                                {
                                    keyAssignments.Add(Expression.Bind(
                                        typeof(TResult).GetTypeInfo().GetMember(singleInit.Member.Name).Single(), ParameterSubstituter.Replace(keySelector.Parameters[0], input, singleInit.Expression)));
                                }
                            }
                            break;
                        }
                    case ExpressionType.New:
                        {
                            var newInit = keySelector.Body as NewExpression;
                            if (newInit.Type.IsAnonymousTypeName())
                            {
                                for (int i = 0; i < newInit.Arguments.Count; i++)
                                {
                                    var leftSide = newInit.Members[i];
                                    if (newInit.Arguments[i] is MemberExpression rightSide)
                                    {
                                        keyAssignments.Add(Expression.Bind(
                                            typeof(TResult).GetTypeInfo().GetMember(leftSide.Name).Single(),
                                            Expression.PropertyOrField(input, rightSide.Member.Name)));
                                        keyFields.Add(rightSide.Member.Name);
                                    }
                                    else
                                    {
                                        keyAssignments.Add(Expression.Bind(
                                            typeof(TResult).GetTypeInfo().GetMember(leftSide.Name).Single(), ParameterSubstituter.Replace(keySelector.Parameters[0], input, newInit.Arguments[i])));
                                    }
                                }
                            }
                            else
                            {
                                // Currently do the same thing as the default case, but we might be able to be smarter here.
                                // If we have an arbitrary expression, assume we have only one remaining field that is not the attribute or value fields and assign to it.
                                var outputFields = typeof(TResult).GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance).OrderBy(o => o.Name).Select(o => o.Name)
                                    .Concat(
                                        typeof(TResult).GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0).OrderBy(o => o.Name).Select(o => o.Name))
                                    .Where(o => o != attributeField.Member.Name && o != valueField.Member.Name).ToList();
                                if (outputFields.Count != 1) throw new NotSupportedException("Unpivot operation could not determine a unique field to which to assign key values.");

                                keyAssignments.Add(Expression.Bind(
                                    typeof(TResult).GetTypeInfo().GetMember(outputFields.Single()).Single(), keySelector.ReplaceParametersInBody(input)));
                            }
                            break;
                        }
                    default:
                        {
                            // If we have an arbitrary expression, assume we have only one remaining field that is not the attribute or value fields and assign to it.
                            var outputFields = typeof(TResult).GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance).OrderBy(o => o.Name).Select(o => o.Name)
                                .Concat(
                                    typeof(TResult).GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetIndexParameters().Length == 0).OrderBy(o => o.Name).Select(o => o.Name))
                                .Where(o => o != attributeField.Member.Name && o != valueField.Member.Name).ToList();
                            if (outputFields.Count != 1) throw new NotSupportedException("Unpivot operation could not determine a unique field to which to assign key values.");

                            keyAssignments.Add(Expression.Bind(
                                typeof(TResult).GetTypeInfo().GetMember(outputFields.Single()).Single(), keySelector.ReplaceParametersInBody(input)));
                            break;
                        }
                }

                var members = typeof(TInput).GetTypeInfo().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.GetIndexParameters().Length == 0).OrderBy(o => o.Name).Select(p => new { p.Name, Type = p.PropertyType })
                    .Concat(typeof(TInput).GetTypeInfo().GetFields(BindingFlags.Public | BindingFlags.Instance).OrderBy(o => o.Name).Select(f => new { f.Name, Type = f.FieldType }))
                    .Where(m => !keyFields.Contains(m.Name));
                foreach (var m in members)
                {
                    var name = m.Name;
                    var type = m.Type;
                    var nullable = IsNullable(type);
                    if (!typeof(TValue).GetTypeInfo().IsAssignableFrom(nullable ? type.GetTypeInfo().GetGenericArguments()[0] : type)) continue;

                    // Only a member that fits is looked up by name. A type can hide a base member with new, and looking
                    // up that name throws AmbiguousMatchException, so a member that does not fit must stay unresolved.
                    var member = Expression.PropertyOrField(input, name);
                    var isNull = nullable ? Expression.Not(Expression.PropertyOrField(member, "HasValue")) : IsNullTest(member);
                    var value = nullable ? Expression.PropertyOrField(member, "Value") : member;

                    var memberAssignments = new[]
                    {
                        Expression.Bind(attributeField.Member, Expression.Constant(name)),
                        Expression.Bind(valueField.Member, ToValueType(value, valueField.Type)),
                    };
                    var result = Expression.MemberInit(newExpression, keyAssignments.Concat(memberAssignments));
                    if (isNull == null)
                    {
                        alwaysPresent.Add(result);
                        continue;
                    }

                    body = Append(body, alwaysPresent);
                    body = Append(body, Expression.Condition(isNull, Results(), Results(result)));
                }

                body = Append(body, alwaysPresent) ?? Results();
                if (body.Type != typeof(IEnumerable<TResult>)) body = Expression.Convert(body, typeof(IEnumerable<TResult>));
                return Expression.Lambda<Func<TInput, IEnumerable<TResult>>>(body, input);
            }

            // The selector is written out as C# by columnar code generation, which names no generic type arguments
            // in method calls. So results are collected in arrays, and Enumerable.Concat infers its type argument
            // from them.
            private static Expression Results(params Expression[] results) => Expression.NewArrayInit(typeof(TResult), results);

            private static Expression Append(Expression body, List<Expression> results)
            {
                if (results.Count == 0) return body;
                var array = Results(results.ToArray());
                results.Clear();
                return Append(body, array);
            }

            private static Expression Append(Expression body, Expression results)
                => body == null ? results : Expression.Call(typeof(Enumerable), nameof(Enumerable.Concat), new[] { typeof(TResult) }, body, results);

            // The null test for a member that is not Nullable<T>, or null when the member can never be null. A
            // value-type member is never null. A reference-type member is compared by reference, so a user-defined
            // == operator cannot hide a value. The member is converted to object first so that the C# that columnar
            // code generation writes, (object)member == null, also compares references.
            private static Expression IsNullTest(MemberExpression member)
                => member.Type.GetTypeInfo().IsValueType
                    ? null
                    : Expression.ReferenceEqual(Expression.Convert(member, typeof(object)), Expression.Constant(null));

            // Expression.Bind accepts a value-type expression for an object or interface member, but the compiled
            // assignment then stores the value without boxing it. The conversion boxes it.
            private static Expression ToValueType(Expression value, Type valueType)
                => value.Type == valueType ? value : Expression.Convert(value, valueType);
        }

        private static bool IsNullable(Type type) => type.GetTypeInfo().IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
    }
}