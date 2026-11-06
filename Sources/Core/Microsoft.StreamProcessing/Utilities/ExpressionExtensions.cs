// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    internal static class ExpressionExtensions
    {
        public static Expression<Comparison<TComparison>> InlineCalls<TComparison>(this Expression<Comparison<TComparison>> function)
        {
            Contract.Requires(function != null);
            return CallInlinerCallRewriter.Inline<Comparison<TComparison>>(function);
        }

        public static Expression<Func<TOutput>> InlineCalls<TOutput>(this Expression<Func<TOutput>> function)
        {
            Contract.Requires(function != null);
            return CallInlinerCallRewriter.Inline<Func<TOutput>>(function);
        }

        public static Expression<Func<T1, TOutput>> InlineCalls<T1, TOutput>(this Expression<Func<T1, TOutput>> function)
        {
            Contract.Requires(function != null);
            return CallInlinerCallRewriter.Inline<Func<T1, TOutput>>(function);
        }

        public static Expression<Func<T1, T2, TOutput>> InlineCalls<T1, T2, TOutput>(this Expression<Func<T1, T2, TOutput>> function)
        {
            Contract.Requires(function != null);
            return CallInlinerCallRewriter.Inline<Func<T1, T2, TOutput>>(function);
        }

        public static Expression<Func<T1, T2, T3, TOutput>> InlineCalls<T1, T2, T3, TOutput>(this Expression<Func<T1, T2, T3, TOutput>> function)
        {
            Contract.Requires(function != null);
            return CallInlinerCallRewriter.Inline<Func<T1, T2, T3, TOutput>>(function);
        }

        public static Expression<Func<T1, T2, T3, T4, TOutput>> InlineCalls<T1, T2, T3, T4, TOutput>(this Expression<Func<T1, T2, T3, T4, TOutput>> function)
        {
            Contract.Requires(function != null);
            return CallInlinerCallRewriter.Inline<Func<T1, T2, T3, T4, TOutput>>(function);
        }

        /// <summary>
        /// Decodes generated C# text so that a plain search finds every spelling of a name. In an identifier, C# reads
        /// a \u or \U escape as the character it names and ignores formatting characters, as the C# specification's
        /// section on identifiers says. A leading '@' needs nothing, since the name follows it in the text.
        /// </summary>
        internal static string DecodeIdentifiers(string name)
        {
            var key = new StringBuilder();
            for (int i = 0; i < name.Length; i++)
            {
                var digits = name[i] == '\\' && i + 1 < name.Length ? (name[i + 1] == 'u' ? 4 : name[i + 1] == 'U' ? 8 : 0) : 0;
                if (digits > 0 && i + 2 + digits <= name.Length
                    && int.TryParse(name.Substring(i + 2, digits), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var code)
                    && (digits == 4 || (code >= 0 && code <= 0x10FFFF && (code < 0xD800 || code > 0xDFFF))))
                {
                    key.Append(digits == 4 ? ((char)code).ToString() : char.ConvertFromUtf32(code));
                    i += 1 + digits;
                }
                else
                {
                    key.Append(name[i]);
                }
            }

            return new string(key.ToString().Where(c => CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.Format).ToArray());
        }

        /// <summary>
        /// Given an expression, <paramref name="e"/>, returns a string that
        /// should be acceptable to the C# compiler and which has the same
        /// semantics as <paramref name="e"/>.
        /// </summary>
        public static string ExpressionToCSharp(this Expression e)
        {
            var stringBuilder = new StringBuilder();
            var visitor = new ConvertToCSharp(new StringWriter(stringBuilder, CultureInfo.InvariantCulture));
            visitor.Visit(e);
            var s = stringBuilder.ToString();
            return s;
        }

        /// <summary>
        /// Returns text that identifies <paramref name="e"/> in a cache of generated code. That is its C# text,
        /// which keeps every constant exact, such as a NaN's bits. If <paramref name="e"/> cannot be written as
        /// C#, it is its debug text with a prefix: code generation fails for such an expression anyway, and falls
        /// back or throws by the usual policy, so the cache only records that failure.
        /// </summary>
        public static string CacheKeyText(this Expression e)
        {
            try
            {
                return e.ExpressionToCSharp();
            }
            catch (Exception)
            {
                return "unrendered: " + e.ToString();
            }
        }

        /// <summary>
        /// Given an expression, <paramref name="function"/>, that is a LambdaExpression,
        /// returns the body of the lambda
        /// with all references to parameters replaced by the corresponding string from
        /// <paramref name="arguments"/>.
        /// Note that this can result in an argument being evaluated more than once,
        /// so if it has any side-effects, this changes the semantics
        /// from applying the function to the arguments.
        /// </summary>
        public static string Inline(this LambdaExpression function, params string[] arguments)
        {
            Contract.Requires(function != null);

            var map = new Dictionary<ParameterExpression, string>(arguments.Length);
            for (int i = 0; i < arguments.Length; i++)
            {
                map.Add(function.Parameters[i], arguments[i]);
            }

            return ExpressionToCSharpStringWithParameterSubstitution(function.Body, map);
        }

        /// <summary>
        /// Just like <see cref="ExpressionToCSharp"/>, but while turning the expression
        /// into C# source, replaces all occurrences of any parameter given as a key
        /// in <paramref name="map"/> with the value that parameter is associated with in
        /// <paramref name="map"/>.
        /// Note that this can result in an argument being evaluated more than once,
        /// so if it has any side-effects, this changes the semantics
        /// from applying the function to the arguments.
        /// </summary>
        /// <param name="e"></param>
        /// <param name="map"></param>
        /// <returns></returns>
        public static string ExpressionToCSharpStringWithParameterSubstitution(this Expression e, Dictionary<ParameterExpression, string> map)
        {
            var stringBuilder = new StringBuilder();
            var visitor = new ConvertToCSharpButWithStringParameters(new StringWriter(stringBuilder, CultureInfo.InvariantCulture), map);
            visitor.Visit(e);
            return stringBuilder.ToString();
        }

        public static LambdaExpression RemoveCastToObject(this LambdaExpression lambda)
            => lambda.Body is UnaryExpression body
            && (body.NodeType == ExpressionType.Convert || body.NodeType == ExpressionType.TypeAs)
            && (body.Type == typeof(object))
                ? Expression.Lambda(body.Operand, lambda.Parameters)
                : lambda;

        public static Expression ReplaceParametersInBody(this LambdaExpression lambda, params Expression[] expressions)
            => ParameterSubstituter.Replace(lambda.Parameters, lambda.Body, expressions);
    }

    internal sealed class ConstantExpressionFinder : ExpressionVisitor
    {
        private bool isConstant = true;
        private readonly HashSet<ParameterExpression> parameters;

        private ConstantExpressionFinder(HashSet<ParameterExpression> parameters) => this.parameters = parameters;

        public static bool IsClosedExpression(LambdaExpression function)
        {
            Contract.Requires(function != null);

            var hashSet = new HashSet<ParameterExpression>(function.Parameters);
            var me = new ConstantExpressionFinder(hashSet);
            me.Visit(function.Body);
            return me.isConstant;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var t = node.Type;
            if (!t.GetTypeInfo().IsPrimitive) this.isConstant = false;
            return base.VisitConstant(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (!(node.Expression is MemberExpression)) // if it is a member expression, then let visitor recurse down to the left-most branch
            {
                if (!(node.Expression is ParameterExpression p) || !this.parameters.Contains(p)) this.isConstant = false;
            }
            return base.VisitMember(node);
        }
    }

    internal sealed class CallInlinerCallRewriter : ExpressionVisitor
    {
        // This replaces
        //     .Call CallInliner.Call(.Lambda (PARAMS) => { BODY }, ARGS)
        // with
        //     BODY/(PARAMS->ARGS)
        // i.e. BODY expression where PARAMS are replaced with ARGS substitutions
        public static Expression<TDelegate> Inline<TDelegate>(LambdaExpression lambda)
            => Expression.Lambda<TDelegate>(new CallInlinerCallRewriter().Visit(lambda.Body), lambda.Parameters);

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(CallInliner))
            {
                var callArguments = node.Arguments;
                var realFunction = (LambdaExpression)Expression.Lambda(callArguments[0]).Compile().DynamicInvoke();
                var realArguments = new Expression[callArguments.Count - 1];
                for (int i = 1; i < callArguments.Count; i++)
                {
                    realArguments[i - 1] = Visit(callArguments[i]);
                }

                return realFunction.ReplaceParametersInBody(realArguments);
            }

            return node;
        }
    }

    internal sealed class EqualityComparer : ExpressionVisitor
    {
        // Each tree has its own scope: the parameters and variables in scope, with the number of their declaration. Two
        // trees can share a parameter object and declare it in different places.
        private readonly Dictionary<ParameterExpression, int> leftScope = new Dictionary<ParameterExpression, int>();
        private readonly Dictionary<ParameterExpression, int> rightScope = new Dictionary<ParameterExpression, int>();
        private int uniqueParameterNumber;

        private EqualityComparer() => this.uniqueParameterNumber = 0;

        /// <summary>
        /// Returns true if the two trees are the same, up to the names of the parameters and variables they declare. A
        /// node type that the comparison does not handle, such as a goto or a switch, counts as different, and so does a
        /// pair whose comparison throws. For most callers that only gives up sharing or an optimization; a join then
        /// uses the default comparer, as it does for any two comparers that may differ.
        /// </summary>
        public static bool IsEqual(Expression e1, Expression e2)
        {
            try
            {
                var me = new EqualityComparer();
                return me.Equals(e1, e2);
            }
            catch (Exception)
            {
                return false;
            }
        }

        // A loop's labels are optional; two labels match if both are missing or both have the same name.
        private static bool SameLabel(LabelTarget label1, LabelTarget label2)
            => label1 == null ? label2 == null : label2 != null && label1.Name == label2.Name;

        private bool Equals(Expression e1, Expression e2)
        {
            // A subtree that both trees share computes the same thing only if its parameters are bound alike in both.
            if (e1 == e2 && (e1 == null || SameBindings(e1))) return true;
            if (e1 == null || e2 == null) return false;
            if (e1.NodeType != e2.NodeType) return false;

            // The type of a node is part of what it computes: a conversion to float and one to double differ.
            if (e1.Type != e2.Type) return false;
            if (e1 is BinaryExpression b1)
            {
                // The method of a user-defined operator is what the node calls, so it must match too.
                var b2 = e2 as BinaryExpression;
                return b1.Method == b2.Method
                    && Equals(b1.Left, b2.Left) && Equals(b1.Right, b2.Right) && Equals(b1.Conversion, b2.Conversion);
            }
            if (e1 is UnaryExpression u1)
            {
                var u2 = e2 as UnaryExpression;
                return u1.Method == u2.Method && Equals(u1.Operand, u2.Operand);
            }
            if (e1 is ConditionalExpression conditional1)
            {
                var conditional2 = e2 as ConditionalExpression;
                return Equals(conditional1.Test, conditional2.Test) && Equals(conditional1.IfTrue, conditional2.IfTrue) && Equals(conditional1.IfFalse, conditional2.IfFalse);
            }
            if (e1 is ConstantExpression constant1)
            {
                var constant2 = e2 as ConstantExpression;

                // double.Equals and float.Equals count every NaN as equal and 0.0 as equal to -0.0, but code can tell
                // them apart, so floating-point constants are compared by their bits.
                if (constant1.Value is double d1 && constant2.Value is double d2) return BitConverter.DoubleToInt64Bits(d1) == BitConverter.DoubleToInt64Bits(d2);
                if (constant1.Value is float f1 && constant2.Value is float f2) return BitConverter.SingleToInt32Bits(f1) == BitConverter.SingleToInt32Bits(f2);

                // Equals also ignores a decimal's scale, a DateTime's kind and a DateTimeOffset's offset, which code can read.
                if (constant1.Value is decimal m1 && constant2.Value is decimal m2) return decimal.GetBits(m1).SequenceEqual(decimal.GetBits(m2));
                if (constant1.Value is DateTime t1 && constant2.Value is DateTime t2) return t1.ToBinary() == t2.ToBinary();
                if (constant1.Value is DateTimeOffset o1 && constant2.Value is DateTimeOffset o2) return o1.EqualsExact(o2);
                return constant1.Value == null ? constant2.Value == null : constant1.Value.Equals(constant2.Value);
            }
            if (e1 is ParameterExpression param1)
            {
                var param2 = e2 as ParameterExpression;
                return this.leftScope[param1] == this.rightScope[param2];
            }
            if (e1 is IndexExpression index1)
            {
                var index2 = e2 as IndexExpression;
                return index1.Indexer == index2.Indexer && Equals(index1.Object, index2.Object) && Equals(index1.Arguments, index2.Arguments);
            }
            if (e1 is InvocationExpression invoke1)
            {
                var invoke2 = e2 as InvocationExpression;
                return Equals(invoke1.Expression, invoke2.Expression) && Equals(invoke1.Arguments, invoke2.Arguments);
            }
            if (e1 is LambdaExpression lambda1)
            {
                var lambda2 = e2 as LambdaExpression;
                if (!lambda1.ReturnType.Equals(lambda2.ReturnType)) return false;
                if (lambda1.Parameters.Count != lambda2.Parameters.Count) return false;
                if (!Declare(lambda1.Parameters, lambda2.Parameters, out var savedLeft, out var savedRight)) return false;
                var result = Equals(lambda1.Body, lambda2.Body);
                Restore(this.leftScope, savedLeft);
                Restore(this.rightScope, savedRight);
                return result;
            }
            if (e1 is MemberExpression member1)
            {
                var member2 = e2 as MemberExpression;
                return member1.Member.Equals(member2.Member) && Equals(member1.Expression, member2.Expression);
            }
            if (e1 is MethodCallExpression mc1)
            {
                var mc2 = e2 as MethodCallExpression;
                return mc1.Method.Equals(mc2.Method) && Equals(mc1.Object, mc2.Object) && Equals(mc1.Arguments, mc2.Arguments);
            }
            if (e1 is NewExpression new1)
            {
                var new2 = e2 as NewExpression;
                return new1.Constructor == null
                    ? new2.Constructor == null
                    : new1.Constructor.Equals(new2.Constructor) && Equals(new1.Arguments, new2.Arguments);
            }
            if (e1 is NewArrayExpression newarr1)
            {
                var newarr2 = e2 as NewArrayExpression;
                return newarr1.Type.Equals(newarr2.Type) && Equals(newarr1.Expressions, newarr2.Expressions);
            }
            if (e1 is MemberInitExpression memInit1)
            {
                var memInit2 = e2 as MemberInitExpression;
                return Equals(memInit1.NewExpression, memInit2.NewExpression) && memInit1.Bindings.Count == memInit2.Bindings.Count
                    && memInit1.Bindings.Zip(memInit2.Bindings, (a, b) => Tuple.Create(a, b)).All(r => Equals(r.Item1, r.Item2));
            }
            if (e1 is BlockExpression block1)
            {
                var block2 = e2 as BlockExpression;
                if (block1.Variables.Count != block2.Variables.Count) return false;
                for (int i = 0; i < block1.Variables.Count; i++)
                {
                    if (block1.Variables[i].Type != block2.Variables[i].Type) return false;
                }

                // A block's variables are declared like a lambda's parameters, so two blocks written the same way match.
                if (!Declare(block1.Variables, block2.Variables, out var savedLeft, out var savedRight)) return false;
                var result = Equals(block1.Expressions, block2.Expressions);
                Restore(this.leftScope, savedLeft);
                Restore(this.rightScope, savedRight);
                return result;
            }
            if (e1 is LoopExpression loop1)
            {
                var loop2 = e2 as LoopExpression;
                return Equals(loop1.Body, loop2.Body) && SameLabel(loop1.BreakLabel, loop2.BreakLabel) && SameLabel(loop1.ContinueLabel, loop2.ContinueLabel);
            }
            if (e1 is LabelExpression label1)
            {
                var label2 = e2 as LabelExpression;

                // A goto names its label by object, so two labels match only if they are the same target.
                return Equals(label1.DefaultValue, label2.DefaultValue) && label1.Target == label2.Target;
            }
            if (e1 is ListInitExpression listInit1)
            {
                var listInit2 = e2 as ListInitExpression;
                return Equals(listInit1.NewExpression, listInit2.NewExpression) && listInit1.Initializers.Count == listInit2.Initializers.Count
                    && Enumerable.Range(0, listInit1.Initializers.Count).All(o => listInit1.Initializers[o].AddMethod == listInit2.Initializers[o].AddMethod
                        && Equals(listInit1.Initializers[o].Arguments, listInit2.Initializers[o].Arguments));
            }
            return false;
        }

        // True if every parameter that a shared subtree uses is bound to the same declaration in both trees, or in neither.
        private bool SameBindings(Expression shared)
        {
            if (this.leftScope.Count == 0 && this.rightScope.Count == 0) return true;
            foreach (var parameter in ParameterCollector.Collect(shared))
            {
                var inLeft = this.leftScope.TryGetValue(parameter, out var left);
                var inRight = this.rightScope.TryGetValue(parameter, out var right);
                if (inLeft != inRight || left != right) return false;
            }

            return true;
        }

        // Gives the i-th parameter or variable that each of two lambdas or blocks declares one number, each in its own
        // tree's scope, and returns what each scope held before, so that Restore can undo a declaration that shadows an
        // enclosing one. Returns false, declaring nothing, if one parameter object is declared at different places in the
        // two lists: the trees would then bind it differently, so they count as different.
        private bool Declare(
            ReadOnlyCollection<ParameterExpression> first,
            ReadOnlyCollection<ParameterExpression> second,
            out List<KeyValuePair<ParameterExpression, int?>> savedLeft,
            out List<KeyValuePair<ParameterExpression, int?>> savedRight)
        {
            savedLeft = new List<KeyValuePair<ParameterExpression, int?>>();
            savedRight = new List<KeyValuePair<ParameterExpression, int?>>();
            for (int i = 0; i < first.Count; i++)
            {
                var j = second.IndexOf(first[i]);
                if (j >= 0 && j != i) return false;
            }

            for (int i = 0; i < first.Count; i++)
            {
                savedLeft.Add(Bind(this.leftScope, first[i]));
                savedRight.Add(Bind(this.rightScope, second[i]));
                this.uniqueParameterNumber++;
            }

            return true;
        }

        private KeyValuePair<ParameterExpression, int?> Bind(Dictionary<ParameterExpression, int> scope, ParameterExpression parameter)
        {
            var saved = new KeyValuePair<ParameterExpression, int?>(parameter, scope.TryGetValue(parameter, out var old) ? old : (int?)null);
            scope[parameter] = this.uniqueParameterNumber;
            return saved;
        }

        private void Restore(Dictionary<ParameterExpression, int> scope, List<KeyValuePair<ParameterExpression, int?>> saved)
        {
            for (int i = saved.Count - 1; i >= 0; i--)
            {
                if (saved[i].Value.HasValue) scope[saved[i].Key] = saved[i].Value.Value;
                else scope.Remove(saved[i].Key);
            }
        }

        private bool Equals(ReadOnlyCollection<Expression> list1, ReadOnlyCollection<Expression> list2)
        {
            if (list1.Count != list2.Count) return false;
            for (int i = 0; i < list1.Count; i++)
            {
                if (!Equals(list1[i], list2[i])) return false;
            }
            return true;
        }

        private bool Equals(MemberBinding mb1, MemberBinding mb2)
        {
            if (mb1.Member != mb2.Member) return false;
            switch (mb1.BindingType)
            {
                case MemberBindingType.Assignment:
                    {
                        if (mb2.BindingType != MemberBindingType.Assignment) return false;
                        var a1 = mb1 as MemberAssignment;
                        var a2 = mb2 as MemberAssignment;
                        return Equals(a1.Expression, a2.Expression);
                    }
                case MemberBindingType.ListBinding:
                    {
                        if (mb2.BindingType != MemberBindingType.ListBinding) return false;
                        var l1 = mb1 as MemberListBinding;
                        var l2 = mb2 as MemberListBinding;
                        return l1.Initializers.Count == l2.Initializers.Count
                            && Enumerable.Range(0, l1.Initializers.Count).All(o => l1.Initializers[o].AddMethod == l2.Initializers[o].AddMethod && Equals(l1.Initializers[o].Arguments, l2.Initializers[o].Arguments));
                    }
                case MemberBindingType.MemberBinding:
                    {
                        if (mb2.BindingType != MemberBindingType.MemberBinding) return false;
                        var v1 = mb1 as MemberMemberBinding;
                        var v2 = mb2 as MemberMemberBinding;
                        return v1.Bindings.Count == v2.Bindings.Count
                            && Enumerable.Range(0, v1.Bindings.Count).All(o => Equals(v1.Bindings[o], v2.Bindings[o]));
                    }
                default: throw new InvalidOperationException("Switch statement meant to be exhaustive.");
            }
        }

        // Collects the parameters and variables that a subtree uses.
        private sealed class ParameterCollector : ExpressionVisitor
        {
            private readonly HashSet<ParameterExpression> found = new HashSet<ParameterExpression>();

            public static HashSet<ParameterExpression> Collect(Expression tree)
            {
                var collector = new ParameterCollector();
                collector.Visit(tree);
                return collector.found;
            }

            protected override Expression VisitParameter(ParameterExpression node)
            {
                this.found.Add(node);
                return node;
            }
        }
    }

    internal sealed class VariableFinder : ExpressionVisitor
    {
        private readonly List<object> foundVariables = new List<object>();

        public static List<object> Find(Expression exp)
        {
            var v = new VariableFinder();
            v.Visit(exp);
            return v.foundVariables;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Looking for pattern Constant.Member, where Constant is not a primitive
            if (node.Expression != null
                && node.Expression.NodeType == ExpressionType.Constant
                && node.Member is FieldInfo
                && !node.Expression.Type.GetTypeInfo().IsPrimitive)
            {
                var c = (ConstantExpression)node.Expression;
                var f = (FieldInfo)node.Member;
                this.foundVariables.Add(f.GetValue(c.Value));
            }
            return node;
        }
    }

    internal sealed class ParameterSubstituter : ExpressionVisitor
    {
        private readonly Dictionary<ParameterExpression, Expression> arguments;

        public static Expression InlineInvocation(InvocationExpression invokeExpression)
        {
            if (!(invokeExpression.Expression is LambdaExpression lambda)) return invokeExpression;
            var map = new Dictionary<ParameterExpression, Expression>(invokeExpression.Arguments.Count);
            for (int i = 0; i < invokeExpression.Arguments.Count; i++)
            {
                map.Add(lambda.Parameters[i], invokeExpression.Arguments[i]);
            }
            var visitor = new ParameterSubstituter(map);
            return visitor.Visit(lambda.Body);
        }

        public static Expression Replace(ParameterExpression eOld, Expression eNew, Expression expression)
            => new ParameterSubstituter(new Dictionary<ParameterExpression, Expression> { { eOld, eNew } }).Visit(expression);

        public static Expression Replace(ReadOnlyCollection<ParameterExpression> parameters, Expression expression, params Expression[] substitutes)
        {
            var dictionary = new Dictionary<ParameterExpression, Expression>();
            for (int i = 0; i < Math.Min(parameters.Count, substitutes.Length); i++) dictionary.Add(parameters[i], substitutes[i]);
            return new ParameterSubstituter(dictionary).Visit(expression);
        }

        public static Expression<Func<TPayload, PartitionKey<TPartitionKey>>> AddPartitionKey<TPartitionKey, TPayload>(
            Expression<Func<TPayload, TPartitionKey>> expression)
        {
            // payload => new PartitionKey<TPartitionKey>(<the extractor's body>), over the extractor's own parameter.
            Expression<Func<TPartitionKey, PartitionKey<TPartitionKey>>> box = (o) => new PartitionKey<TPartitionKey>(o);
            var newBody = Replace(box.Parameters[0], expression.Body, box.Body);
            return Expression.Lambda<Func<TPayload, PartitionKey<TPartitionKey>>>(newBody, expression.Parameters);
        }

        private ParameterSubstituter(Dictionary<ParameterExpression, Expression> arguments)
            => this.arguments = new Dictionary<ParameterExpression, Expression>(arguments);

        protected override Expression VisitParameter(ParameterExpression node)
            => this.arguments.TryGetValue(node, out var replacement)
                ? replacement
                : base.VisitParameter(node);
    }

    /// <summary>
    /// Emits C# source code equivalent to expression trees.
    /// </summary>
    internal class ConvertToCSharp : ExpressionVisitor
    {
        public readonly TextWriter writer;
        private readonly HashSet<ExpressionType> nonExpressionFeatures = new HashSet<ExpressionType>
        {
            ExpressionType.Loop,
            ExpressionType.Block
        };

        public ConvertToCSharp(TextWriter writer) => this.writer = writer ?? throw new ArgumentNullException(nameof(writer));

        private new void Visit(ReadOnlyCollection<Expression> es)
        {
            this.writer.Write("(");
            var first = true;
            foreach (var e in es)
            {
                if (!first) this.writer.Write(", ");
                first = false;
                Visit(e);
            }

            this.writer.Write(")");
        }

        private void Visit(ReadOnlyCollection<ParameterExpression> ps)
        {
            foreach (var p in ps)
            {
                var typeName = GetTypeName(p.Type);
                this.writer.Write(typeName);
                this.writer.Write(" ");
                this.writer.Write(p.Name);
                this.writer.Write(";");
            }
        }

        public override Expression Visit(Expression node) => base.Visit(node);

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (!node.NodeType.ToString().Contains("Assign"))
            {
                this.writer.Write("(");
            }
            Visit(node.Left);
            Visit(node.NodeType);
            Visit(node.Right);
            if (!node.NodeType.ToString().Contains("Assign"))
            {
                this.writer.Write(")");
            }
            return null;
        }

        protected override Expression VisitBlock(BlockExpression node)
        {
            this.writer.WriteLine("{");
            Visit(node.Variables);
            foreach (var e in node.Expressions)
            {
                Visit(e);
                this.writer.WriteLine(";");
            }

            this.writer.WriteLine("}");
            return null;
        }

        protected override CatchBlock VisitCatchBlock(CatchBlock node)
        {
            this.writer.Write(node.ToString());
            return base.VisitCatchBlock(node);
        }

        protected override Expression VisitConditional(ConditionalExpression node)
        {
            if (node.Type != typeof(void)
                && !this.nonExpressionFeatures.Contains(node.IfTrue.NodeType)
                && !this.nonExpressionFeatures.Contains(node.IfFalse.NodeType))
            {
                this.writer.Write("(");
                base.Visit(node.Test);
                this.writer.Write(" ? ");
                base.Visit(node.IfTrue);
                this.writer.Write(" : ");
                base.Visit(node.IfFalse);
                this.writer.Write(")");
            }
            else
            {
                this.writer.Write("if (");
                base.Visit(node.Test);
                this.writer.Write(") { ");
                base.Visit(node.IfTrue);
                this.writer.Write(" }");
                if (node.IfFalse != null && !(node.IfFalse.NodeType == ExpressionType.Default && node.IfFalse.Type.Equals(typeof(void))))
                {
                    this.writer.Write(" else { ");
                    base.Visit(node.IfFalse);
                    this.writer.Write(" }");
                }
            }
            return null;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            var t = node.Type;
            var v = node.Value;
            var literal = LiteralText(v);
            if (literal != null)
            {
                this.writer.Write(literal);
                return null;
            }
            var typeInfo = t.GetTypeInfo();
            if (typeof(Type).GetTypeInfo().IsAssignableFrom(typeInfo))
            {
                this.writer.Write("typeof({0})", GetTypeName((Type)v));
                return null;
            }
            if (!typeInfo.IsPrimitive && typeInfo.IsValueType && Nullable.GetUnderlyingType(t) == null && v.Equals(Activator.CreateInstance(t)))
            {
                // A constant of a non-primitive struct type must be a default value for that type
                // At least, that is what is assumed here.
                this.writer.Write($"default({GetTypeName(t)})");
                return null;
            }

            if (v == null) this.writer.Write("null");
            else this.writer.Write(Convert.ToString(v, CultureInfo.InvariantCulture));
            return null;
        }

        /// <summary>
        /// Returns C# source that the compiler reads back as exactly <paramref name="value"/>, with the
        /// same type, if that type has a C# literal form: <see cref="bool"/>, <see cref="char"/>,
        /// <see cref="string"/>, a built-in numeric type or an enum. Returns null for any other type and for null.
        /// The text never depends on the current culture.
        /// </summary>
        internal static string LiteralText(object value)
        {
            if (value == null) return null;
            var type = value.GetType();
            if (type.GetTypeInfo().IsEnum)
            {
                var underlying = Enum.GetUnderlyingType(type);
                return "((" + GetTypeName(type) + ")" + LiteralText(Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture)) + ")";
            }

            switch (value)
            {
                case bool b: return b ? "true" : "false";
                case char c: return CharLiteral(c);
                case string s: return SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(s)).ToString();
                case sbyte n: return "((sbyte)" + Invariant(n) + ")";
                case byte n: return "((byte)" + Invariant(n) + ")";
                case short n: return "((short)" + Invariant(n) + ")";
                case ushort n: return "((ushort)" + Invariant(n) + ")";
                case int n: return Signed(Invariant(n));
                case uint n: return Invariant(n) + "U";
                case long n: return Signed(Invariant(n) + "L");
                case ulong n: return Invariant(n) + "UL";
                case float f:
                    if (float.IsNaN(f))
                    {
                        // A NaN other than float.NaN keeps its sign and payload by being rebuilt from its bits.
                        var bits = BitConverter.SingleToInt32Bits(f);
                        return bits == BitConverter.SingleToInt32Bits(float.NaN)
                            ? "float.NaN"
                            : "global::System.BitConverter.Int32BitsToSingle(" + Signed(Invariant(bits)) + ")";
                    }
                    if (float.IsPositiveInfinity(f)) return "float.PositiveInfinity";
                    if (float.IsNegativeInfinity(f)) return "float.NegativeInfinity";
                    return Signed(f.ToString("R", CultureInfo.InvariantCulture) + "F");
                case double d:
                    if (double.IsNaN(d))
                    {
                        var bits = BitConverter.DoubleToInt64Bits(d);
                        return bits == BitConverter.DoubleToInt64Bits(double.NaN)
                            ? "double.NaN"
                            : "global::System.BitConverter.Int64BitsToDouble(" + Signed(Invariant(bits) + "L") + ")";
                    }
                    if (double.IsPositiveInfinity(d)) return "double.PositiveInfinity";
                    if (double.IsNegativeInfinity(d)) return "double.NegativeInfinity";
                    return Signed(d.ToString("R", CultureInfo.InvariantCulture) + "D");
                case decimal m:
                    {
                        // Formatting drops the sign of a negative zero, and the compiler folds -0M to a positive
                        // zero, so a negative zero is rebuilt with its sign and scale.
                        var flags = decimal.GetBits(m)[3];
                        var negativeZero = m == 0m && flags < 0;
                        return negativeZero
                            ? "new global::System.Decimal(0, 0, 0, true, " + Invariant((flags >> 16) & 0xFF) + ")"
                            : Signed(Invariant(m) + "M");
                    }
                default: return null;
            }
        }

        private static string Invariant(IFormattable n) => n.ToString(null, CultureInfo.InvariantCulture);

        // A negative literal is parenthesized. After a cast to a named type, such as an enum, an unparenthesized
        // minus would read as a subtraction.
        private static string Signed(string literal) => literal[0] == '-' ? "(" + literal + ")" : literal;

        // Printable ASCII is written as itself, other characters as \u escapes.
        private static string CharLiteral(char c)
            => c >= ' ' && c <= '~' && c != '\'' && c != '\\'
            ? "'" + c + "'"
            : string.Format(CultureInfo.InvariantCulture, "'\\u{0:X4}'", (int)c);

        protected override Expression VisitDebugInfo(DebugInfoExpression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitDebugInfo(node);
        }

        protected override Expression VisitDefault(DefaultExpression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitDefault(node);
        }

        protected override ElementInit VisitElementInit(ElementInit node)
        {
            this.writer.Write(node.ToString());
            return base.VisitElementInit(node);
        }

        protected override Expression VisitExtension(Expression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitExtension(node);
        }

        protected override Expression VisitGoto(GotoExpression node)
        {
            if (node.Kind == GotoExpressionKind.Break)
            {
                this.writer.Write("break;");
                return null;
            }
            throw new NotImplementedException();
        }

        protected override Expression VisitIndex(IndexExpression node)
        {
            Visit(node.Object);
            this.writer.Write("[");
            var first = true;
            foreach (var arg in node.Arguments)
            {
                if (!first) this.writer.Write(", ");
                first = false;
                Visit(arg);
            }

            this.writer.Write("]");
            return null;
        }

        protected override Expression VisitInvocation(InvocationExpression node)
        {
            var inlinedLambda = ParameterSubstituter.InlineInvocation(node);
            if (!(inlinedLambda is InvocationExpression))
            {
                Visit(inlinedLambda);
                return null;
            }

            this.writer.Write("Invoke(");
            base.Visit(node.Expression);
            this.writer.Write(",");
            Visit(node.Arguments);
            this.writer.Write(")");
            return null;
        }

        protected override Expression VisitLabel(LabelExpression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitLabel(node);
        }

        protected override LabelTarget VisitLabelTarget(LabelTarget node)
        {
            this.writer.Write(node.Name);
            return null;
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            this.writer.Write("(");
            if (node.Parameters.Count == 1) base.Visit(node.Parameters.ElementAt(0));
            else
            {
                var first = true;
                this.writer.Write("(");
                foreach (var p in node.Parameters)
                {
                    if (!first) this.writer.Write(",");
                    base.Visit(p);
                    first = false;
                }

                this.writer.Write(")");
            }

            this.writer.Write(" => ");
            base.Visit(node.Body);
            this.writer.Write(")");
            return null;
        }

        protected override Expression VisitListInit(ListInitExpression node)
        {
            Visit(node.NewExpression);
            this.writer.Write("{");
            int j = 0;
            foreach (var i in node.Initializers)
            {
                if (j++ > 0) this.writer.Write(", ");
                if (i.Arguments.Count > 1) this.writer.Write(" {");
                this.writer.Write(" ");
                VisitCommaDelimitedList(i.Arguments);
                if (i.Arguments.Count > 1) this.writer.Write(" }");
            }

            this.writer.Write("}");
            return null;
        }

        protected override Expression VisitLoop(LoopExpression node)
        {
            this.writer.Write("while (true) {");
            base.Visit(node.Body);
            this.writer.Write("}");
            if (!string.IsNullOrEmpty(node.BreakLabel.Name))
            {
                this.writer.Write(node.BreakLabel.Name);
                this.writer.Write(": /* nothing to do */");
            }
            return null;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != null) Visit(node.Expression);
            else
                this.writer.Write(GetTypeName(node.Member.DeclaringType));
            this.writer.Write(".");
            this.writer.Write(node.Member.Name);
            return null;
        }

        protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
        {
            this.writer.Write(node.Member.Name);
            this.writer.Write(" = ");
            Visit(node.Expression);
            return null;
        }

        protected override MemberBinding VisitMemberBinding(MemberBinding node)
            => base.VisitMemberBinding(node); // taken care of by VisitMemberAssignment, but what if there are other subtypes of MemberBinding?

        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            Visit(node.NewExpression);
            var first = true;
            this.writer.Write("{");
            foreach (var b in node.Bindings)
            {
                if (!first) this.writer.Write(", ");
                VisitMemberBinding(b);
                first = false;
            }

            this.writer.Write("}");
            return null;
        }

        protected override MemberListBinding VisitMemberListBinding(MemberListBinding node)
        {
            this.writer.Write(node.ToString());
            return base.VisitMemberListBinding(node);
        }

        protected override MemberMemberBinding VisitMemberMemberBinding(MemberMemberBinding node)
        {
            this.writer.Write(node.ToString());
            return base.VisitMemberMemberBinding(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var isIndexer = node.Method.IsSpecialName && node.Method.Name.Equals("get_Item");
            if (node.Object != null)
            {
                Visit(node.Object);
                if (!isIndexer) this.writer.Write(".");
            }
            else
            {
                this.writer.Write(GetTypeName(node.Method.DeclaringType));
                this.writer.Write(".");
            }
            if (isIndexer)
            {
                this.writer.Write("[");
            }
            else
            {
                this.writer.Write(node.Method.Name);
                this.writer.Write("(");
            }
            var first = true;
            var byRef = new List<bool>();
            foreach (var param in node.Method.GetParameters())
            {
                byRef.Add(param.ParameterType.IsByRef);
            }

            var iter = 0;
            foreach (var arg in node.Arguments)
            {
                if (!first) this.writer.Write(", ");
                first = false;
                if (byRef[iter]) this.writer.Write("ref ");
                iter++;
                Visit(arg);
            }

            if (isIndexer)
                this.writer.Write("]");
            else
                this.writer.Write(")");
            return null;
        }

        protected override Expression VisitNew(NewExpression node)
        {
            this.writer.Write("new ");
            var isAnon = node.Type.IsAnonymousType();
            List<string> anonymousTypePropertyNames = null;
            if (isAnon)
            {
                this.writer.Write("{");
                anonymousTypePropertyNames = new List<string>(node.Type.GetRuntimeProperties().Select(p => p.Name));
            }
            else
            {
                var typeName = GetTypeName(node.Type);
                this.writer.Write(typeName);
                this.writer.Write("(");
            }
            var i = 0;
            foreach (var arg in node.Arguments)
            {
                if (i > 0) this.writer.Write(", ");
                if (isAnon) this.writer.Write("{0} = ", anonymousTypePropertyNames[i]);
                Visit(arg);
                i++;
            }

            if (isAnon)
                this.writer.Write("}");
            else
                this.writer.Write(")");
            return null;
        }

        protected override Expression VisitNewArray(NewArrayExpression node)
        {
            this.writer.Write("new {0} {{ ", GetTypeName(node.Type));
            VisitCommaDelimitedList(node.Expressions);

            if (node.Expressions.Count > 0) this.writer.Write(" ");

            this.writer.Write("}");
            return null;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitParameter(node);
        }

        protected override Expression VisitRuntimeVariables(RuntimeVariablesExpression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitRuntimeVariables(node);
        }

        protected override Expression VisitSwitch(SwitchExpression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitSwitch(node);
        }

        protected override SwitchCase VisitSwitchCase(SwitchCase node)
        {
            this.writer.Write(node.ToString());
            return base.VisitSwitchCase(node);
        }

        protected override Expression VisitTry(TryExpression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitTry(node);
        }

        protected override Expression VisitTypeBinary(TypeBinaryExpression node)
        {
            this.writer.Write(node.ToString());
            return base.VisitTypeBinary(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            this.writer.Write("(");

            if (node.NodeType == ExpressionType.Convert)
            {
                this.writer.Write("(");
                this.writer.Write(GetTypeName(node.Type));
                this.writer.Write(")");
                Visit(node.Operand);
            }
            else
            {
                Visit(node.NodeType);
                Visit(node.Operand);
            }

            this.writer.Write(")");
            return null;
        }

        private static string GetTypeName(Type type) => type.GetCSharpSourceSyntax();

        private void Visit(ExpressionType expressionType)
        {
            this.writer.Write(" ");
            switch (expressionType)
            {
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    this.writer.Write("+"); break;

                // Summary:
                //     A bitwise or logical AND operation, such as (a & b) in C# and (a And b) in
                //     Visual Basic.
                case ExpressionType.And:
                    this.writer.Write("&"); break;

                // Summary:
                //     A conditional AND operation that evaluates the second operand only if the
                //     first operand evaluates to true. It corresponds to (a && b) in C# and (a
                //     AndAlso b) in Visual Basic.
                case ExpressionType.AndAlso:
                    this.writer.Write("&&"); break;

                // Summary:
                //     A node that represents a null coalescing operation, such as (a ?? b) in C#
                //     or If(a: break; b) in Visual Basic.
                case ExpressionType.Coalesce:
                    this.writer.Write("??"); break;

                // Summary:
                //     A division operation, such as (a / b), for numeric operands.
                case ExpressionType.Divide:
                    this.writer.Write("/"); break;

                // Summary:
                //     A node that represents an equality comparison, such as (a == b) in C# or
                //     (a = b) in Visual Basic.
                case ExpressionType.Equal:
                    this.writer.Write("=="); break;

                // Summary:
                //     A bitwise or logical XOR operation, such as (a ^ b) in C# or (a Xor b) in
                //     Visual Basic.
                case ExpressionType.ExclusiveOr:
                    this.writer.Write("^"); break;

                // Summary:
                //     A "greater than" comparison, such as (a > b).
                case ExpressionType.GreaterThan:
                    this.writer.Write(">"); break;

                // Summary:
                //     A "greater than or equal to" comparison, such as (a >= b).
                case ExpressionType.GreaterThanOrEqual:
                    this.writer.Write(">="); break;

                // Summary:
                //     A bitwise left-shift operation: break; such as (a << b).
                case ExpressionType.LeftShift:
                    this.writer.Write("<<"); break;

                // Summary:
                //     A "less than" comparison: break; such as (a < b).
                case ExpressionType.LessThan:
                    this.writer.Write("<"); break;

                // Summary:
                //     A "less than or equal to" comparison: break; such as (a <= b).
                case ExpressionType.LessThanOrEqual:
                    this.writer.Write("<="); break;

                // Summary:
                //     An arithmetic remainder operation: break; such as (a % b) in C# or (a Mod b) in
                //     Visual Basic.
                case ExpressionType.Modulo:
                    this.writer.Write("%"); break;

                // Summary:
                //     A multiplication operation: break; such as (a * b): break; without overflow checking: break; for
                //     numeric operands.
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    this.writer.Write("*"); break;

                // Summary:
                //     An arithmetic negation operation: break; such as (-a). The object a should not be
                //     modified in place.
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    this.writer.Write("-"); break;

                // Summary:
                //     A unary plus operation: break; such as (+a). The result of a predefined unary plus
                //     operation is the value of the operand: break; but user-defined implementations might
                //     have unusual results.
                case ExpressionType.UnaryPlus:
                    this.writer.Write("+"); break;

                // Summary:
                //     A bitwise complement or logical negation operation. In C#: break; it is equivalent
                //     to (~a) for integral types and to (!a) for Boolean values. In Visual Basic: break;
                //     it is equivalent to (Not a). The object a should not be modified in place.
                case ExpressionType.Not:
                    this.writer.Write("!"); break; // BUG

                // Summary:
                //     An inequality comparison: break; such as (a != b) in C# or (a <> b) in Visual Basic.
                case ExpressionType.NotEqual:
                    this.writer.Write("!="); break;

                // Summary:
                //     A bitwise or logical OR operation: break; such as (a | b) in C# or (a Or b) in Visual
                //     Basic.
                case ExpressionType.Or:
                    this.writer.Write("|"); break;

                // Summary:
                //     A short-circuiting conditional OR operation: break; such as (a || b) in C# or (a
                //     OrElse b) in Visual Basic.
                case ExpressionType.OrElse:
                    this.writer.Write("||"); break;

                // Summary:
                //     A bitwise right-shift operation: break; such as (a >> b).
                case ExpressionType.RightShift:
                    this.writer.Write(">>"); break;

                // Summary:
                //     A subtraction operation: break; such as (a - b): break; without overflow checking: break; for
                //     numeric operands.
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    this.writer.Write("-"); break;

                // Summary:
                //     An assignment operation: break; such as (a = b).
                case ExpressionType.Assign:
                    this.writer.Write("="); break;

                // Summary:
                //     A unary decrement operation: break; such as (a - 1) in C# and Visual Basic. The
                //     object a should not be modified in place.
                case ExpressionType.Decrement:
                    this.writer.Write("-"); break;

                // Summary:
                //     A unary increment operation: break; such as (a + 1) in C# and Visual Basic. The
                //     object a should not be modified in place.
                case ExpressionType.Increment:
                    this.writer.Write("+"); break;

                // Summary:
                //     An addition compound assignment operation: break; such as (a += b): break; without overflow
                //     checking: break; for numeric operands.
                case ExpressionType.AddAssign:
                case ExpressionType.AddAssignChecked:
                    this.writer.Write("+="); break;

                // Summary:
                //     A bitwise or logical AND compound assignment operation: break; such as (a &= b)
                //     in C#.
                case ExpressionType.AndAssign:
                    this.writer.Write("&="); break;

                // Summary:
                //     An division compound assignment operation: break; such as (a /= b): break; for numeric
                //     operands.
                case ExpressionType.DivideAssign:
                    this.writer.Write("/="); break;

                // Summary:
                //     A bitwise or logical XOR compound assignment operation: break; such as (a ^= b)
                //     in C#.
                case ExpressionType.ExclusiveOrAssign:
                    this.writer.Write("^="); break;

                // Summary:
                //     A bitwise left-shift compound assignment: break; such as (a <<= b).
                case ExpressionType.LeftShiftAssign:
                    this.writer.Write("<<="); break;

                // Summary:
                //     An arithmetic remainder compound assignment operation: break; such as (a %= b) in
                //     C#.
                case ExpressionType.ModuloAssign:
                    this.writer.Write("%="); break;

                // Summary:
                //     A multiplication compound assignment operation: break; such as (a *= b): break; without
                //     overflow checking: break; for numeric operands.
                case ExpressionType.MultiplyAssign:
                case ExpressionType.MultiplyAssignChecked:
                    this.writer.Write("*="); break;

                // Summary:
                //     A bitwise or logical OR compound assignment: break; such as (a |= b) in C#.
                case ExpressionType.OrAssign:
                    this.writer.Write("|="); break;

                // Summary:
                //     A bitwise right-shift compound assignment operation: break; such as (a >>= b).
                case ExpressionType.RightShiftAssign:
                    this.writer.Write(">>="); break;

                // Summary:
                //     A subtraction compound assignment operation: break; such as (a -= b): break; without overflow
                //     checking: break; for numeric operands.
                case ExpressionType.SubtractAssign:
                case ExpressionType.SubtractAssignChecked:
                    this.writer.Write("-="); break;

                // Summary:
                //     A unary prefix increment: break; such as (++a). The object a should be modified
                //     in place.
                case ExpressionType.PreIncrementAssign:
                    this.writer.Write("++"); break;

                // Summary:
                //     A unary prefix decrement: break; such as (--a). The object a should be modified
                //     in place.
                case ExpressionType.PreDecrementAssign:
                    this.writer.Write("--"); break;

                // Summary:
                //     A unary postfix increment: break; such as (a++). The object a should be modified
                //     in place.
                case ExpressionType.PostIncrementAssign:
                    this.writer.Write("++"); break;

                // Summary:
                //     A unary postfix decrement: break; such as (a--). The object a should be modified
                //     in place.
                case ExpressionType.PostDecrementAssign:
                    this.writer.Write("--"); break;

                // Summary:
                //     A ones complement operation: break; such as (~a) in C#.
                case ExpressionType.OnesComplement:
                    this.writer.Write("~"); break;

                // Summary:
                //     A true condition value.
                case ExpressionType.IsTrue:
                    this.writer.Write("true"); break;

                // Summary:
                //     A false condition value.
                case ExpressionType.IsFalse:
                    this.writer.Write("false"); break;
            }

            this.writer.Write(" ");
        }

        private void VisitCommaDelimitedList(IEnumerable<Expression> es) => VisitDelimitedList(es, ", ");

        private void VisitDelimitedList(IEnumerable<Expression> es, string delimiter)
        {
            var first = true;
            foreach (var e in es)
            {
                if (!first) this.writer.Write(delimiter);
                first = false;
                Visit(e);
            }
        }

        private void WriteDelimitedList(IEnumerable<string> values, string delimiter)
        {
            var first = true;
            foreach (string value in values)
            {
                if (!first) this.writer.Write(delimiter);
                first = false;
                this.writer.Write(value);
            }
        }
    }

    internal sealed class ConvertToCSharpButWithStringParameters : ConvertToCSharp
    {
        private readonly Dictionary<string, string> arguments;

        public ConvertToCSharpButWithStringParameters(TextWriter writer, Dictionary<ParameterExpression, string> arguments)
            : base(writer)
        {
            this.arguments = new Dictionary<string, string>(arguments.Count);
            foreach (var kv in arguments)
                this.arguments.Add(kv.Key.Name, kv.Value);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (this.arguments.TryGetValue(node.Name, out string replacement))
                this.writer.Write(replacement);
            else base.VisitParameter(node);

            return null;
        }
    }

    internal sealed class ColumnOriented : ExpressionVisitor
    {
        private struct ParameterInformation
        {
            public Expression ArrayVariable;
            public Expression IndexVariable;
        }

        private Dictionary<Tuple<ParameterExpression, string>, ParameterInformation> parameterTableForDecomposableTypes;
        private Dictionary<ParameterExpression, ParameterInformation> parameterTableForAtomicTypes;
        private bool reconstructsValue;

        public struct SubstitutionInformation
        {
            public ColumnarRepresentation columnarRepresentation;
            public string nameForIndexVariable;
            public Type typeOfBatchVariable;
        }

        private ColumnOriented() { }

        /// <summary>
        /// Transforms a row-oriented lambda into a column-oriented lambda.
        /// For each parameter, e, that is referenced in the body of the lambda as
        /// "e.f" (i.e., a field/property dereference for decomposable types or else
        /// just "e" for atomic types like ints), the reference is changed into "b.f.col[i]"
        /// where b is a batch variable, f is a column on the batch that corresponds to the field
        /// f, and i is the index variable for the row in the column.
        /// </summary>
        /// <param name="rowOrientedLambda"></param>
        /// <param name="substitutionInformation"></param>
        /// <returns>Null if the body could not be transformed</returns>
        public static LambdaExpression/*?*/ Transform(LambdaExpression rowOrientedLambda, IDictionary<ParameterExpression, SubstitutionInformation> substitutionInformation)
            => Transform(rowOrientedLambda, substitutionInformation, out _);

        /// <summary>As the overload above, and reports whether the result rebuilds a parameter's value from its columns.</summary>
        public static LambdaExpression/*?*/ Transform(LambdaExpression rowOrientedLambda, IDictionary<ParameterExpression, SubstitutionInformation> substitutionInformation, out bool reconstructsValue)
        {
            Contract.Requires(rowOrientedLambda != null);
            Contract.Requires(substitutionInformation != null);
            Contract.Requires(Contract.ForAll(substitutionInformation.Keys, k => rowOrientedLambda.Parameters.Contains(k)));

            var me = new ColumnOriented
            {
                parameterTableForDecomposableTypes = new Dictionary<Tuple<ParameterExpression, string>, ParameterInformation>(),
                parameterTableForAtomicTypes = new Dictionary<ParameterExpression, ParameterInformation>()
            };
            var parameterMapping = new Dictionary<ParameterExpression, ParameterExpression>();
            var i = 0;

            // Create a mapping for each parameter/field pair, <p,f> to the indexable expression that will be substituted
            // for occurrences of "p.f" in the body of the function.
            // For fields represented as ColumnBatch<T>, this is "b.f.col", i.e., directly to the array contained in the column batch.
            // For fields represented as MultiString, this is "b.f", since generated code uses the MultiString indexer.
            foreach (var kv in substitutionInformation)
            {
                var parameter = kv.Key;
                var si = kv.Value;
                if (!rowOrientedLambda.Parameters.Contains(parameter))
                    throw new InvalidOperationException("Key in dictionary must be a parameter of the lambda!");
                var cr = si.columnarRepresentation;
                var batchName = "b" + (i++).ToString(CultureInfo.InvariantCulture);
                var batchType = si.typeOfBatchVariable;
                var batchVariable = Expression.Variable(batchType, batchName);
                parameterMapping.Add(parameter, batchVariable);
                var indexVariableName = si.nameForIndexVariable;
                var indexExpression = Expression.Variable(typeof(int), indexVariableName);
                if (cr.noFields)
                {
                    var columnarField = cr.PseudoField;
                    var batchField = batchType.GetTypeInfo().GetField(columnarField.Name);
                    var columnBatch = Expression.MakeMemberAccess(batchVariable, batchField);
                    var colArrayOfColumnBatch = Expression.MakeMemberAccess(columnBatch, batchField.FieldType.GetTypeInfo().GetField("col"));
                    me.parameterTableForAtomicTypes.Add(
                        parameter,
                        new ParameterInformation { ArrayVariable = colArrayOfColumnBatch, IndexVariable = indexExpression, });
                }
                else
                {
                    foreach (var f in cr.Fields.Values)
                    {
                        Expression arrayToUse;
                        var batchField = batchType.GetTypeInfo().GetField(f.Name);
                        var batchFieldType = batchField.FieldType;
                        if (batchFieldType.GetTypeInfo().IsGenericType && batchFieldType.GetGenericTypeDefinition().Equals(typeof(ColumnBatch<>)))
                        {
                            // ColumBatch<T> for some T
                            var columnBatch = Expression.MakeMemberAccess(batchVariable, batchField);
                            var colArrayOfColumnBatch = Expression.MakeMemberAccess(columnBatch, batchField.FieldType.GetTypeInfo().GetField("col"));
                            arrayToUse = colArrayOfColumnBatch;
                        }
                        else if (batchFieldType.Equals(typeof(MultiString)))
                        {
                            // MultiString
                            arrayToUse = Expression.MakeMemberAccess(batchVariable, batchField);
                        }
                        else continue; // Possible error? Should this just be silent?
                        me.parameterTableForDecomposableTypes.Add(
                            Tuple.Create(parameter, f.OriginalName),
                            new ParameterInformation { ArrayVariable = arrayToUse, IndexVariable = indexExpression, });
                    }
                }
            }
            var transformedBody = me.Visit(rowOrientedLambda.Body);
            var transformedParameters = rowOrientedLambda.Parameters.Select(p => parameterMapping.ContainsKey(p) ? parameterMapping[p] : p).ToArray();
            var transformedLambda = Expression.Lambda(transformedBody, transformedParameters);
            reconstructsValue = me.reconstructsValue;
            return transformedLambda;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (this.parameterTableForAtomicTypes.TryGetValue(node, out var parameterInfo))
            {
                var arrayAccess = Expression.ArrayAccess(parameterInfo.ArrayVariable, parameterInfo.IndexVariable);
                return arrayAccess;
            }

            if (this.parameterTableForDecomposableTypes.Keys.Any(k => k.Item1.Equals(node)))
            {
                // Then the parameter was not used as a receiver for a field dereference.
                // In that case, the whole value is needed but its type will have been
                // decomposed into a columnar representation.
                // So its value must be reconstructed from its columnar representation.
                // Flag that this is happening so clients can decide what to do about it.
                this.reconstructsValue = true;
                return ReconstructValue(node);
            }
            return base.VisitParameter(node);
        }

        private Expression ReconstructValue(ParameterExpression p)
        {
            var t = p.Type;
            var fieldsDictionary = this.parameterTableForDecomposableTypes
                .Where(kv => kv.Key.Item1.Equals(p))
                ;

            if (!t.IsAnonymousTypeName())
            {
                // case: the type t is not anonymous.
                // it must have a nullary ctor and public fields/properties that can be assigned to
                // generate: new T() {f1 = f1_col[i], f2 = f2_col[i], ... fn = fn_col[i] }
                var newExpression = Expression.New(t);
                var memberInitExpression = Expression.MemberInit(
                    newExpression,
                    fieldsDictionary
                    .Select(kv =>
                    {
                        var memberInfo = t.GetTypeInfo().GetField(kv.Key.Item2) as MemberInfo;
                        if (memberInfo == null) memberInfo = t.GetTypeInfo().GetProperty(kv.Key.Item2) as MemberInfo;
                        return Expression.Bind(memberInfo, MakeIndexedAccessExpression(kv.Value));
                    }));
                return memberInitExpression;
            }
            else
            {
                // case: the type t is anonymous.
                // Then t has a constructor that takes n arguments.
                // generate: (T)Activator.CreateInstance(typeof(T), new object[]{f1_col[i], f2_col[i], ..., fn_col[i]})
                var argsToCtor = new List<Expression> { Expression.Constant(t, typeof(Type)) };
                var args = fieldsDictionary
                    .Select(kv => MakeIndexedAccessExpression(kv.Value))
                    .Select(a => Expression.Convert(a, typeof(object)));
                argsToCtor.Add(Expression.NewArrayInit(typeof(object), args.ToArray()));

                var createInstanceCall = Expression.Call(typeof(Activator), "CreateInstance", null, argsToCtor.ToArray());
                var cast = Expression.Convert(createInstanceCall, t);
                return cast;
            }
        }

        /// <summary>
        /// Converts expressions of the form "t.F" to "batch.F'[i]"
        /// when F is a field (of type A) defined in the type T that the user query is written over.
        /// batch is a parameter whose type is the batch class generated from T.
        /// F' is the indexable expression that was computed in the Transform method.
        /// i is the a variable of type int
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        protected override Expression VisitMember(MemberExpression node)
        {
            var m = node.Member;
            if (node.Expression is ParameterExpression parameter)
            {
                if (this.parameterTableForDecomposableTypes.TryGetValue(Tuple.Create(parameter, m.Name), out var parameterInfo))
                    return MakeIndexedAccessExpression(parameterInfo);
                if (this.parameterTableForAtomicTypes.TryGetValue(parameter, out _))
                    throw new InvalidOperationException();
            }

            return base.VisitMember(node);
        }

        private static Expression MakeIndexedAccessExpression(ParameterInformation pi)
        {
            var a = pi.ArrayVariable;
            var i = pi.IndexVariable;
            if (a.Type.IsArray)
            {
                var arrayAccess = Expression.ArrayAccess(a, i);
                return arrayAccess;
            }
            else if (a.Type.Equals(typeof(MultiString)))
            {
                return Expression.Property(a, "Item", i);
            }
            else throw new InvalidOperationException();
        }
    }

    /// <summary>
    /// Finds whether a lambda can write into storage of its parameter that columnar code maps to a column element:
    /// a column member or a field inside one, or the parameter itself when it is stored in one column.
    /// </summary>
    internal sealed class ColumnWriteFinder : ExpressionVisitor
    {
        private readonly ParameterExpression parameter;
        private readonly HashSet<string> columns; // null when the payload is stored in one column
        private bool found;

        private ColumnWriteFinder(ParameterExpression parameter, HashSet<string> columns)
        {
            this.parameter = parameter;
            this.columns = columns;
        }

        public static bool Find(LambdaExpression function, ColumnarRepresentation representation)
        {
            var parameter = function.Parameters[0];
            var columns = representation.noFields ? null : new HashSet<string>(representation.Fields.Values.Select(f => f.OriginalName));
            var finder = new ColumnWriteFinder(parameter, columns);
            finder.Visit(function.Body);
            return finder.found;
        }

        // A method or getter called on struct storage gets that storage by reference and can change it. It cannot when
        // the struct type is readonly, or when the member is readonly and declared on the struct itself. A member
        // declared elsewhere, such as on an interface or on object, can run the struct's own code, which may write.
        private static bool MayWrite(Type receiver, MethodInfo method)
            => receiver.GetTypeInfo().IsValueType && !IsReadOnly(receiver.GetTypeInfo())
                && !(method.DeclaringType == receiver && IsReadOnly(method));

        private static bool IsReadOnly(MemberInfo member)
            => member.GetCustomAttributesData().Any(a => a.AttributeType.FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Object != null && IsColumnStorage(node.Object) && MayWrite(node.Object.Type, node.Method)) this.found = true;
            CheckByRef(node.Method.GetParameters(), node.Arguments);
            return base.VisitMethodCall(node);
        }

        protected override Expression VisitNew(NewExpression node)
        {
            if (node.Constructor != null) CheckByRef(node.Constructor.GetParameters(), node.Arguments);
            return base.VisitNew(node);
        }

        protected override Expression VisitInvocation(InvocationExpression node)
        {
            var invoke = node.Expression.Type.GetTypeInfo().GetMethod("Invoke");
            if (invoke != null) CheckByRef(invoke.GetParameters(), node.Arguments);
            return base.VisitInvocation(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member is PropertyInfo property && node.Expression != null && IsColumnStorage(node.Expression)
                && property.GetMethod != null && MayWrite(node.Expression.Type, property.GetMethod))
            {
                this.found = true;
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitIndex(IndexExpression node)
        {
            if (node.Object != null && IsColumnStorage(node.Object) && node.Indexer?.GetMethod != null && MayWrite(node.Object.Type, node.Indexer.GetMethod))
                this.found = true;
            return base.VisitIndex(node);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.Assign:
                case ExpressionType.AddAssign:
                case ExpressionType.AddAssignChecked:
                case ExpressionType.AndAssign:
                case ExpressionType.DivideAssign:
                case ExpressionType.ExclusiveOrAssign:
                case ExpressionType.LeftShiftAssign:
                case ExpressionType.ModuloAssign:
                case ExpressionType.MultiplyAssign:
                case ExpressionType.MultiplyAssignChecked:
                case ExpressionType.OrAssign:
                case ExpressionType.PowerAssign:
                case ExpressionType.RightShiftAssign:
                case ExpressionType.SubtractAssign:
                case ExpressionType.SubtractAssignChecked:
                    if (IsWriteTarget(node.Left)) this.found = true;
                    break;
            }

            return base.VisitBinary(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.PreIncrementAssign:
                case ExpressionType.PreDecrementAssign:
                case ExpressionType.PostIncrementAssign:
                case ExpressionType.PostDecrementAssign:
                    if (IsWriteTarget(node.Operand)) this.found = true;
                    break;
            }

            return base.VisitUnary(node);
        }

        private void CheckByRef(ParameterInfo[] parameters, ReadOnlyCollection<Expression> arguments)
        {
            for (int i = 0; i < parameters.Length && i < arguments.Count; i++)
            {
                if (parameters[i].ParameterType.IsByRef && IsColumnStorage(arguments[i])) this.found = true;
            }
        }

        // An assignment or increment writes column storage when it targets that storage, or a member of it or an
        // element through its indexer when the storage is a struct.
        private bool IsWriteTarget(Expression e)
        {
            if (IsColumnStorage(e)) return true;
            var receiver = e is MemberExpression member ? member.Expression : (e as IndexExpression)?.Object;
            return receiver != null && receiver.Type.GetTypeInfo().IsValueType && IsColumnStorage(receiver);
        }

        private bool IsColumnStorage(Expression e)
        {
            if (e == this.parameter) return this.columns == null;
            if (!(e is MemberExpression member)) return false;
            if (member.Expression == this.parameter) return this.columns != null && this.columns.Contains(member.Member.Name);
            return member.Member is FieldInfo && member.Expression != null && member.Expression.Type.GetTypeInfo().IsValueType
                && IsColumnStorage(member.Expression);
        }
    }

    internal sealed class IntroduceArrayVariables : ExpressionVisitor
    {
        private IDictionary<ParameterExpression, string> parameterSubstitutionNames;
        private ReadOnlyCollection<ParameterExpression> lambdaParameters;

        private IntroduceArrayVariables() { }

        /// <summary>
        /// Tranforms a lambda's body into a new expression.
        /// The input lambda is of the form (b_1, ..., b_n) => B.
        /// The body, B, has expressions of the form "b_i.f.col[i]".
        /// The new expression replaces such expressions with "x_i_f_col[i]"
        /// where "x_i_f_col" is a newly created (array) variable of the correct type.
        /// It is up to the caller to make sure that variables with the same name are
        /// defined in the scope for which the returned expression is used.
        /// The value of "x" for each parameter b_i is specified in the <paramref name="parameterSubstitutionNames"/>.
        /// If a parameter, b_i, is not a key in the dictionary, then any references to it will be left
        /// unchanged.
        /// </summary>
        /// <param name="inputLambda"></param>
        /// <param name="parameterSubstitutionNames">If provided, used to map parameters of the lambda to the
        /// name to use as the first part of the array variable. If not provided, then all parameters will
        /// be substituted for and the name of the parameter will be used for "x".
        /// </param>
        /// <returns></returns>
        public static LambdaExpression Transform(LambdaExpression inputLambda, IDictionary<ParameterExpression, string> parameterSubstitutionNames = null)
        {
            Contract.Requires(inputLambda != null);
            Contract.Requires(parameterSubstitutionNames == null || Contract.ForAll(parameterSubstitutionNames.Keys, p => inputLambda.Parameters.Contains(p)));

            var me = new IntroduceArrayVariables
            {
                lambdaParameters = inputLambda.Parameters,
                parameterSubstitutionNames = parameterSubstitutionNames
            };
            var transformedBody = me.Visit(inputLambda.Body);
            var newParameters = new List<ParameterExpression>();
            foreach (var p in inputLambda.Parameters)
            {
                if (parameterSubstitutionNames.ContainsKey(p))
                    newParameters.Add(p);
            }
            return Expression.Lambda(transformedBody, newParameters.ToArray());
        }

        protected override Expression VisitIndex(IndexExpression node)
        {
            if (!(node.Object is MemberExpression colFieldAccess)) goto JustVisit;
            var colMember = colFieldAccess.Member as FieldInfo;
            if (colMember == null) goto JustVisit;
            ParameterExpression batchVariable;
            FieldInfo fieldInfo;
            if (colMember.FieldType.IsArray)
            {
                // then the colMember should be "col" in the expression "b.f.col"
                if (!colMember.Name.Equals("col")) goto JustVisit;
                if (!(colFieldAccess.Expression is MemberExpression FAccess)) goto JustVisit;
                fieldInfo = FAccess.Member as FieldInfo;
                if (fieldInfo == null) goto JustVisit;
                batchVariable = FAccess.Expression as ParameterExpression;
                if (batchVariable == null) goto JustVisit;
            }
            else if (colMember.FieldType.Equals(typeof(MultiString)))
            {
                // then the colMember is f in the expression "b.f"
                fieldInfo = colMember;
                batchVariable = colFieldAccess.Expression as ParameterExpression;
                if (batchVariable == null) goto JustVisit;

            }
            else goto JustVisit;

            if (!this.lambdaParameters.Contains(batchVariable)) goto JustVisit; // Could be a nested lambda, so not an error.
            string nameToUse = batchVariable.Name;
            if (this.parameterSubstitutionNames != null)
            {
                if (this.parameterSubstitutionNames.TryGetValue(batchVariable, out string preferredName))
                    nameToUse = preferredName;
                else goto JustVisit;
            }

            // At this point we know the index expression is "b.F.col[i]" or "b.F.Item[i]" (for a MultiString).
            // Create a new variable b_F_col with the right type and return "b_F_col[i]".
            // REVIEW: Should the variables be cached (or created eagerly at the beginning) so at
            // most one gets created for each field F?
            var arrayVariableName = string.Format(CultureInfo.InvariantCulture, "{0}{1}{2}_col", nameToUse, string.IsNullOrWhiteSpace(nameToUse) ? string.Empty : "_", fieldInfo.Name);
            var arrayVariable = Expression.Variable(colMember.FieldType, arrayVariableName);
            if (colMember.FieldType.IsArray)
            {
                // F is a ColumnBatch<T> for some T
                return Expression.ArrayAccess(arrayVariable, node.Arguments.ToArray());
            }
            else
            {
                // F is a MultiString
                return Expression.Property(arrayVariable, "Item", node.Arguments.ToArray());
            }

        JustVisit:
            return base.VisitIndex(node);
        }
    }

    internal sealed class GroupInputAndKeyInliner<TReduceKey, TBind, TOutput> : ExpressionVisitor
    {
        private GroupInputAndKeyInliner() { }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member.Name == "Key" && node.Expression.NodeType == ExpressionType.New)
            {
                var newExp = (NewExpression)node.Expression;
                if (newExp.Type == typeof(GroupSelectorInput<TReduceKey>)) return newExp.Arguments[0];
            }
            return base.VisitMember(node);
        }

        public static Expression<Func<TReduceKey, TBind, TOutput>> Transform(LambdaExpression input)
            => new GroupInputAndKeyInliner<TReduceKey, TBind, TOutput>().Visit(input) as Expression<Func<TReduceKey, TBind, TOutput>>;
    }

    internal sealed class GroupInputInliner<TReduceKey, TBind, TOutput> : ExpressionVisitor
    {
        private readonly ParameterExpression parameter;
        private readonly ParameterExpression newParameter;

        private GroupInputInliner(ParameterExpression parameter, ParameterExpression newParameter)
        {
            this.parameter = parameter;
            this.newParameter = newParameter;
        }

        protected override Expression VisitMember(MemberExpression node)
            => node.Member.Name == "Key" && node.Expression == this.parameter
                ? this.newParameter
                : base.VisitMember(node);

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == this.parameter) throw new InvalidOperationException("Cannot use a group key outside of accessing its key.");
            return base.VisitParameter(node);
        }

        public static Expression<Func<TReduceKey, TBind, TOutput>> Transform(Expression<Func<GroupSelectorInput<TReduceKey>, TBind, TOutput>> input)
        {
            var newParameter = Expression.Parameter(typeof(TReduceKey), input.Parameters[0].Name);
            var newBody = new GroupInputInliner<TReduceKey, TBind, TOutput>(input.Parameters[0], newParameter).Visit(input.Body);
            return Expression.Lambda<Func<TReduceKey, TBind, TOutput>>(newBody, newParameter, input.Parameters[1]);
        }
    }

    /// <summary>
    /// Finds occurrences of parameter instances that are *not* dereferenced by a field/property
    /// access.
    /// </summary>
    internal sealed class ParameterInstanceFinder : ExpressionVisitor
    {
        private bool foundInstance = false;
        private readonly HashSet<ParameterExpression> parameters;

        private ParameterInstanceFinder(HashSet<ParameterExpression> parameters) => this.parameters = parameters;

        public static bool FoundInstance(LambdaExpression function)
        {
            Contract.Requires(function != null);

            var hashSet = new HashSet<ParameterExpression>(function.Parameters);
            var me = new ParameterInstanceFinder(hashSet);
            me.Visit(function.Body);
            return me.foundInstance;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Object != null)
            {
                if (node.Object is ParameterExpression p && this.parameters.Contains(p)) this.foundInstance = true;
                Visit(node.Object);
            }
            foreach (var arg in node.Arguments)
            {
                if (arg is ParameterExpression p && this.parameters.Contains(p)) this.foundInstance = true;
                Visit(arg);
            }
            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.Left is ParameterExpression p && this.parameters.Contains(p)) this.foundInstance = true;
            Visit(node.Left);

            p = node.Right as ParameterExpression;
            if (p != null && this.parameters.Contains(p)) this.foundInstance = true;
            Visit(node.Right);

            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.Operand is ParameterExpression p && this.parameters.Contains(p)) this.foundInstance = true;
            Visit(node.Operand);

            return node;
        }

        protected override Expression VisitConditional(ConditionalExpression node)
        {
            if (node.Test is ParameterExpression p && this.parameters.Contains(p)) this.foundInstance = true;
            Visit(node.Test);

            p = node.IfTrue as ParameterExpression;
            if (p != null && this.parameters.Contains(p)) this.foundInstance = true;
            Visit(node.IfTrue);

            p = node.IfFalse as ParameterExpression;
            if (p != null && this.parameters.Contains(p)) this.foundInstance = true;
            Visit(node.IfFalse);

            return node;
        }
    }
}