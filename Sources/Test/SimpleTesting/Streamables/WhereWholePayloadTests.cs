using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

// Named like the generated local, and in the global namespace so that generated code names it without a namespace: a
// local of that name would hide it.
[System.Diagnostics.CodeAnalysis.SuppressMessage("StyleCop.CSharp.NamingRules", "SA1300:ElementMustBeginWithUpperCaseLetter", Justification = "The test needs a type named like the generated local.")]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1050:DeclareTypesInNamespaces", Justification = "The test needs a type the generated code names without a namespace.")]
public static class rowPayload
{
    public static bool Allowed() => true;
}

namespace SimpleTesting
{
    public static class WholePayloadHelpers
    {
        public static bool Increment(ref int value)
        {
            ++value;
            return true;
        }
    }

    public struct WholePayloadStruct
    {
        public int A;
        public int B;
        public string Name;

        public static string Stamp(int first, int second) => second == first + 1 ? "n" + (first - 1) : "x";

        public bool Allowed() => false;

        public int Next() => ++this.B;
    }

    public sealed class WholePayloadClass
    {
        public int A;
        public int B;
        public string Name;

        public int Next() => ++this.B;
    }

    // A parameterless constructor with an effect of its own: each construction is counted.
    public sealed class CountedConstructionPayload
    {
        public static int Constructions;
        public int B;

        public CountedConstructionPayload() => Constructions++;

        public static int ReadConstructions() => Constructions;

        public bool Positive() => this.B > 0;
    }

    public interface IWholePayloadNext
    {
        int Next();
    }

    public struct WholePayloadInner : IWholePayloadNext
    {
        public int B;

        // The readonly getters keep a read from counting as a write, so only the setter makes these writes.
        public int Value
        {
            readonly get => this.B;
            set => this.B = value;
        }

        public int this[string key]
        {
            readonly get => this.B;
            set => this.B = value;
        }

        public int this[int index] => ++this.B;

        public int Next() => ++this.B;
    }

    public sealed class WholePayloadHolder
    {
        public int A;
        public bool Flag;
        public WholePayloadInner Inner;

        public static bool Accept(int value) => value > 0;

        public static bool Accept(bool value) => value;

        public bool Positive() => this.A > 0;
    }

    public abstract class WhereWholePayloadTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private readonly bool columnar;

        internal WhereWholePayloadTestsBase(bool columnar, bool multiString, Config.CodegenOptions.MultiStringFlags transforms = Config.CodegenOptions.MultiStringFlags.None)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(columnar).UseMultiString(multiString).MultiStringTransforms(transforms))
            => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void StructMethodCalledTwice()
            => Assert.AreEqual(string.Empty, Run(Structs(3).Where(p => p.Next() == p.Next())));

        [TestMethod, TestCategory("Gated")]
        public void StructMethodThenMember()
            => Assert.AreEqual("3", Run(Structs(3).Where(p => p.Next() == p.B)));

        [TestMethod, TestCategory("Gated")]
        public void StructMemberMethodMember()
            => Assert.AreEqual("3", Run(Structs(3).Where(p => p.B == 3 && p.Next() == 4 && p.B == 4)));

        [TestMethod, TestCategory("Gated")]
        public void StructSumOfTwoCalls()
            => Assert.AreEqual("3", Run(Structs(3).Where(p => p.Next() + p.Next() == 9)));

        [TestMethod, TestCategory("Gated")]
        public void StructEachRowItsOwnValue()
            => Assert.AreEqual("2", Run(Structs(1, 2, 3, 4).Where(p => p.Next() == p.B && p.B == 3)));

        [TestMethod, TestCategory("Gated")]
        public void StructStringMemberAndMethod()
            => Assert.AreEqual("3 5", Run(Structs(3, 5).Where(p => p.Name.StartsWith("n") && p.Next() == p.B)));

        [TestMethod, TestCategory("Gated")]
        public void ParameterNamedLikeAGeneratedLocal()
            => Assert.AreEqual(string.Empty, Run(Structs(3).Where(i => i.Next() == i.Next())));

        [TestMethod, TestCategory("Gated")]
        public void StructCallsInsideAStringCall()
            => Assert.AreEqual("3 5", Run(Structs(3, 5).Where(p => p.Name.Equals(WholePayloadStruct.Stamp(p.Next(), p.Next())))));

        [TestMethod, TestCategory("Gated")]
        public void NestedLambdaNamedLikeTheLocal()
            => Assert.AreEqual("3", Run(Structs(3).Where(p => p.Next() > 0 && new[] { 1 }.Any(rowPayload => rowPayload == 1))));

        [TestMethod, TestCategory("Gated")]
        public void TypeNamedLikeTheLocal()
            => Assert.AreEqual("3", Run(Structs(3).Where(p => p.Next() > 0 && rowPayload.Allowed())));

        [TestMethod, TestCategory("Gated")]
        public void ConstructorWithEffectsRunsOnlyWhereThePredicateReachesIt()
        {
            var query = Separate(new[] { new CountedConstructionPayload { B = 3 } })
                .Where(p => CountedConstructionPayload.ReadConstructions() == 0 && (p.B == 3 || p.Positive()))
                .Select(p => p.B);
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            CountedConstructionPayload.Constructions = 0;
            Assert.AreEqual("3", string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload)));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClassStructFieldChangedWithAWholeUse()
        {
            var query = Separate(new[] { new WholePayloadHolder { A = 1, Inner = new WholePayloadInner { B = 3 } } })
                .Where(p => p.Inner.Next() > 0 && p.Positive());
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            Assert.AreEqual("4", string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.Inner.B)));
        }

        [TestMethod, TestCategory("Gated")]
        public void ClassFieldByReferenceWithAWholeUse()
        {
            var query = Separate(new[] { new WholePayloadHolder { A = 1, Inner = new WholePayloadInner { B = 3 } } })
                .Where(p => WholePayloadHelpers.Increment(ref p.A) && p.Positive());
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            Assert.AreEqual("2", string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.A)));
        }

        // C# cannot write an assignment inside an expression lambda, so the next seven predicates are built by hand.
        // Each one writes into the row and then uses the whole payload.
        [TestMethod, TestCategory("Gated")]
        public void ClassFieldAssignedWithAWholeUse()
            => Assert.AreEqual("True", RunHolder(
                p => Expression.Assign(Expression.Field(p, "Flag"), Expression.Call(p, nameof(WholePayloadHolder.Positive), null)),
                h => h.Flag.ToString()));

        [TestMethod, TestCategory("Gated")]
        public void ClassFieldCompoundAssignedWithAWholeUse()
            => Assert.AreEqual("2", RunHolder(
                p => Expression.AddAssign(Expression.Field(p, "A"), Expression.Constant(1)),
                h => h.A.ToString()));

        [TestMethod, TestCategory("Gated")]
        public void ClassFieldIncrementedWithAWholeUse()
            => Assert.AreEqual("2", RunHolder(p => Expression.PreIncrementAssign(Expression.Field(p, "A")), h => h.A.ToString()));

        [TestMethod, TestCategory("Gated")]
        public void ClassStructPropertySetWithAWholeUse()
            => Assert.AreEqual("5", RunHolder(
                p => Expression.Assign(Expression.Property(Expression.Field(p, "Inner"), "Value"), Expression.Constant(5)),
                h => h.Inner.B.ToString()));

        [TestMethod, TestCategory("Gated")]
        public void ClassStructIndexerSetWithAWholeUse()
            => Assert.AreEqual("7", RunHolder(
                p => Expression.Assign(
                    Expression.MakeIndex(Expression.Field(p, "Inner"), typeof(WholePayloadInner).GetProperty("Item", new[] { typeof(string) }), new[] { Expression.Constant("b") }),
                    Expression.Constant(7)),
                h => h.Inner.B.ToString()));

        [TestMethod, TestCategory("Gated")]
        public void ClassStructIndexerGetWithAWholeUse()
            => Assert.AreEqual("4", RunHolder(Indexer, h => h.Inner.B.ToString()));

        [TestMethod, TestCategory("Gated")]
        public void ClassStructInterfaceMethodWithAWholeUse()
            => Assert.AreEqual("4", RunHolder(
                p => Expression.Call(Expression.Field(p, "Inner"), typeof(IWholePayloadNext).GetMethod(nameof(IWholePayloadNext.Next))),
                h => h.Inner.B.ToString()));

        // C# reads each of these nested lambda parameter names as rowPayload.
        [TestMethod, TestCategory("Gated")]
        public void NestedLambdaNamedWithAnAt()
            => Assert.AreEqual("3", RunNestedLambda("@rowPayload"));

        [TestMethod, TestCategory("Gated")]
        public void NestedLambdaNamedWithAnEscape()
            => Assert.AreEqual("3", RunNestedLambda("\\u0072owPayload"));

        [TestMethod, TestCategory("Gated")]
        public void NestedLambdaNamedWithAFormattingCharacter()
            => Assert.AreEqual("3", RunNestedLambda("row\u00ADPayload"));

        [TestMethod, TestCategory("Gated")]
        public void StructWholeValueOnceControl()
            => Assert.AreEqual("3", Run(Structs(3, 5).Where(p => p.Equals(new WholePayloadStruct { A = 1, B = 3, Name = "n3" }))));

        [TestMethod, TestCategory("Gated")]
        public void StructMemberOnlyControl()
            => Assert.AreEqual("5", Run(Structs(3, 5).Where(p => p.B == 5 && p.Name == "n5")));

        [TestMethod, TestCategory("Gated")]
        public void ClassMethodCalledTwice()
            => Assert.AreEqual(string.Empty, Run(Classes(3).Where(p => p.Next() == p.Next())));

        [TestMethod, TestCategory("Gated")]
        public void ClassSameReference()
            => Assert.AreEqual("3 5", Run(Classes(3, 5).Where(p => ReferenceEquals(p, p))));

        [TestMethod, TestCategory("Gated")]
        public void ClassMemberOnlyControl()
            => Assert.AreEqual("5", Run(Classes(3, 5).Where(p => p.B == 5 && p.Name == "n5")));

        // The Union keeps the Where out of the ingress, so it runs as a separate operator.
        private static IStreamable<Empty, WholePayloadStruct> Structs(params int[] values)
            => Separate(values.Select(b => new WholePayloadStruct { A = 1, B = b, Name = "n" + b }));

        private static IStreamable<Empty, WholePayloadClass> Classes(params int[] values)
            => Separate(values.Select(b => new WholePayloadClass { A = 1, B = b, Name = "n" + b }));

        private static IStreamable<Empty, T> Separate<T>(System.Collections.Generic.IEnumerable<T> values)
            => values.Select((v, i) => StreamEvent.CreatePoint(i + 1, v)).ToObservable().ToStreamable()
                .Union(Array.Empty<StreamEvent<T>>().ToObservable().ToStreamable());

        // p.Inner[0], built as an IndexExpression.
        private static Expression Indexer(ParameterExpression p)
            => Expression.MakeIndex(Expression.Field(p, "Inner"), typeof(WholePayloadInner).GetProperty("Item", new[] { typeof(int) }), new[] { Expression.Constant(0) });

        // Runs p => p.Next() > 0 && new[] { 1 }.Any(x => x == 1), where the nested parameter x has the given name.
        private string RunNestedLambda(string name)
        {
            var p = Expression.Parameter(typeof(WholePayloadStruct), "p");
            var x = Expression.Parameter(typeof(int), name);
            var any = Expression.Call(
                typeof(Enumerable),
                nameof(Enumerable.Any),
                new[] { typeof(int) },
                Expression.NewArrayInit(typeof(int), Expression.Constant(1)),
                Expression.Lambda<Func<int, bool>>(Expression.Equal(x, Expression.Constant(1)), x));
            var next = Expression.Call(p, nameof(WholePayloadStruct.Next), null);
            return Run(Structs(3).Where(Expression.Lambda<Func<WholePayloadStruct, bool>>(
                Expression.AndAlso(Expression.GreaterThan(next, Expression.Constant(0)), any), p)));
        }

        // Runs WholePayloadHolder.Accept(write(p)) && p.Positive() over one holder with A = 1 and Inner.B = 3.
        private string RunHolder(Func<ParameterExpression, Expression> write, Func<WholePayloadHolder, string> show)
        {
            var p = Expression.Parameter(typeof(WholePayloadHolder), "p");
            var written = write(p);
            var accept = typeof(WholePayloadHolder).GetMethod(nameof(WholePayloadHolder.Accept), new[] { written.Type });
            var predicate = Expression.Lambda<Func<WholePayloadHolder, bool>>(
                Expression.AndAlso(Expression.Call(accept, written), Expression.Call(p, nameof(WholePayloadHolder.Positive), null)), p);
            var query = Separate(new[] { new WholePayloadHolder { A = 1, Inner = new WholePayloadInner { B = 3 } } }).Where(predicate);
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            return string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => show(e.Payload)));
        }

        private string Run(IStreamable<Empty, WholePayloadStruct> query)
        {
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            return string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.B));
        }

        private string Run(IStreamable<Empty, WholePayloadClass> query)
        {
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            return string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.B));
        }
    }

    [TestClass]
    public class WhereWholePayloadTestsRow : WhereWholePayloadTestsBase
    {
        public WhereWholePayloadTestsRow() : base(false, false) { }
    }

    [TestClass]
    public class WhereWholePayloadTestsColumnar : WhereWholePayloadTestsBase
    {
        public WhereWholePayloadTestsColumnar() : base(true, false) { }
    }

    [TestClass]
    public class WhereWholePayloadTestsColumnarMultiString : WhereWholePayloadTestsBase
    {
        public WhereWholePayloadTestsColumnarMultiString() : base(true, true) { }
    }

    [TestClass]
    public class WhereWholePayloadTestsColumnarMultiStringTransforms : WhereWholePayloadTestsBase
    {
        public WhereWholePayloadTestsColumnarMultiStringTransforms()
            : base(true, true, Config.CodegenOptions.MultiStringFlags.Wrappers | Config.CodegenOptions.MultiStringFlags.VectorOperations) { }
    }
}
