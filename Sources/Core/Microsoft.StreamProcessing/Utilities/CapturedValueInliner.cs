// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.StreamProcessing.Aggregates;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Replaces each read of a captured local variable of a literal type with a constant holding the
    /// variable's current value.
    /// </summary>
    /// <remarks>
    /// When a C# lambda reads a local variable of the enclosing method, the compiler moves the variable into a
    /// field of a compiler-generated closure class, and the expression tree reads that field. Generated code
    /// cannot name the closure class, so such a lambda forced row mode. Operators call this rewriter when the
    /// query is built, so a captured value of a literal type is read once, at that point, in every execution
    /// mode. A local assigned again after the query is built does not change the query.
    ///
    /// One exception: map-reduce calls its reduce function, such as the one given to <c>GroupApply</c>, each
    /// time the query is subscribed, and builds the operators inside it then. A value captured inside the
    /// reduce function is therefore read when the query is subscribed.
    ///
    /// A literal type is <see cref="bool"/>, <see cref="char"/>, <see cref="string"/>, a built-in numeric type
    /// or an enum. A captured variable of any other type, including a nullable value type, is left as it is, so
    /// the query still reads it as each event arrives. So is a member reached through a captured object, such as
    /// a field of <c>this</c>, and a captured variable that the expression itself writes, for example as an
    /// <c>out</c> argument.
    /// </remarks>
    internal static class CapturedValueInliner
    {
        /// <summary>
        /// Returns <paramref name="expression"/> with every read of a captured local of a literal type replaced
        /// by a constant. Returns the same instance if there is nothing to replace, and null for null.
        /// </summary>
        public static TExpression InlineCapturedValues<TExpression>(this TExpression expression)
            where TExpression : Expression
            => expression == null ? null : (TExpression)new Rewriter(WrittenFieldFinder.Find(expression)).Visit(expression);

        /// <summary>
        /// Returns <paramref name="aggregate"/> if none of its five expressions reads a captured local of a
        /// literal type. Otherwise returns an aggregate made of the five rewritten expressions.
        /// </summary>
        public static IAggregate<TInput, TState, TResult> InlineCapturedValues<TInput, TState, TResult>(
            this IAggregate<TInput, TState, TResult> aggregate)
        {
            if (aggregate == null) return null;
            var initialState = aggregate.InitialState();
            var accumulate = aggregate.Accumulate();
            var deaccumulate = aggregate.Deaccumulate();
            var difference = aggregate.Difference();
            var computeResult = aggregate.ComputeResult();
            var newInitialState = initialState.InlineCapturedValues();
            var newAccumulate = accumulate.InlineCapturedValues();
            var newDeaccumulate = deaccumulate.InlineCapturedValues();
            var newDifference = difference.InlineCapturedValues();
            var newComputeResult = computeResult.InlineCapturedValues();
            return newInitialState == initialState
                && newAccumulate == accumulate
                && newDeaccumulate == deaccumulate
                && newDifference == difference
                && newComputeResult == computeResult
                ? aggregate
                : GeneratedAggregate.Create(newInitialState, newAccumulate, newDeaccumulate, newDifference, newComputeResult);
        }

        internal static bool IsLiteralType(Type type)
        {
            // An enum's type code is its underlying type's, so enums are covered too.
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Boolean:
                case TypeCode.Char:
                case TypeCode.String:
                case TypeCode.SByte:
                case TypeCode.Byte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                case TypeCode.UInt64:
                case TypeCode.Single:
                case TypeCode.Double:
                case TypeCode.Decimal:
                    return true;
                default:
                    return false;
            }
        }

        // The C# compiler names its closure classes with a leading '<', which no C# identifier can have, and
        // marks them as compiler-generated.
        internal static bool IsClosureType(Type type)
            => type.Name.StartsWith("<", StringComparison.Ordinal)
            && type.GetTypeInfo().IsDefined(typeof(CompilerGeneratedAttribute), false);

        // Reads a field chain such as closure.CS$<>8__locals1.x, where every object on the way is a closure.
        private static bool TryReadCapturedField(MemberExpression node, out object value)
        {
            value = null;
            if (!(node.Member is FieldInfo field)) return false;

            object owner;
            switch (node.Expression)
            {
                case ConstantExpression constant:
                    owner = constant.Value;
                    break;
                case MemberExpression outer when TryReadCapturedField(outer, out var outerValue):
                    owner = outerValue;
                    break;
                default:
                    return false;
            }

            if (owner == null || !IsClosureType(owner.GetType())) return false;
            value = field.GetValue(owner);
            return true;
        }

        private sealed class Rewriter : ExpressionVisitor
        {
            private readonly HashSet<MemberInfo> written;

            public Rewriter(HashSet<MemberInfo> written) => this.written = written;

            protected override Expression VisitMember(MemberExpression node)
                => IsLiteralType(node.Type) && !this.written.Contains(node.Member) && TryReadCapturedField(node, out var value)
                    ? Expression.Constant(value, node.Type)
                    : base.VisitMember(node);
        }

        // Finds the fields the expression itself writes: the target of an assignment, a compound assignment or an
        // increment, and any field passed by reference. A read of such a field must stay a read of the field,
        // because the expression's own write changes it, and a constant cannot be written.
        private sealed class WrittenFieldFinder : ExpressionVisitor
        {
            private readonly HashSet<MemberInfo> written = new HashSet<MemberInfo>();

            public static HashSet<MemberInfo> Find(Expression expression)
            {
                var finder = new WrittenFieldFinder();
                finder.Visit(expression);
                return finder.written;
            }

            // Every assignment node type has "Assign" in its name: Assign, AddAssign, AddAssignChecked,
            // PreIncrementAssign and so on.
            private static bool IsAssignment(ExpressionType nodeType)
                => nodeType.ToString().IndexOf("Assign", StringComparison.Ordinal) >= 0;

            protected override Expression VisitBinary(BinaryExpression node)
            {
                if (IsAssignment(node.NodeType)) Mark(node.Left);
                return base.VisitBinary(node);
            }

            protected override Expression VisitUnary(UnaryExpression node)
            {
                if (IsAssignment(node.NodeType)) Mark(node.Operand);
                return base.VisitUnary(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                MarkByRef(node.Method.GetParameters(), node.Arguments);
                return base.VisitMethodCall(node);
            }

            protected override Expression VisitNew(NewExpression node)
            {
                if (node.Constructor != null) MarkByRef(node.Constructor.GetParameters(), node.Arguments);
                return base.VisitNew(node);
            }

            // Expression.Invoke takes a delegate or an Expression<TDelegate> as its target; in the second case the
            // parameters are those of TDelegate's Invoke method.
            protected override Expression VisitInvocation(InvocationExpression node)
            {
                var target = node.Expression.Type.GetTypeInfo();
                if (typeof(LambdaExpression).GetTypeInfo().IsAssignableFrom(target) && target.IsGenericType)
                {
                    target = target.GenericTypeArguments[0].GetTypeInfo();
                }

                var invoke = target.GetDeclaredMethod("Invoke");
                if (invoke != null) MarkByRef(invoke.GetParameters(), node.Arguments);
                return base.VisitInvocation(node);
            }

            private void MarkByRef(ParameterInfo[] parameters, IReadOnlyList<Expression> arguments)
            {
                for (int i = 0; i < parameters.Length && i < arguments.Count; i++)
                {
                    if (parameters[i].ParameterType.IsByRef) Mark(arguments[i]);
                }
            }

            private void Mark(Expression target)
            {
                if (target is MemberExpression member) this.written.Add(member.Member);
            }
        }
    }
}
