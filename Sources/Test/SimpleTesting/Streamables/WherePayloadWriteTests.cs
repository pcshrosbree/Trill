using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public static class PayloadWriteHelpers
    {
        public static bool Increment(ref int value)
        {
            ++value;
            return true;
        }
    }

    public struct WritePoint
    {
        public int A;
        public int B;
    }

    public sealed class WritePointClass
    {
        public int A;
        public int B;
    }

    // A parameterless constructor that is not trivial.
    public struct ConstructedPoint
    {
        public int A;
        public int B;

        public ConstructedPoint()
        {
            this.A = 0;
            this.B = 0;
        }
    }

    public struct WriteCounter
    {
        public int B;

        public int Bumped
        {
            get { return ++this.B; }
        }

        public int Next() => ++this.B;
    }

    public struct WriteHolder
    {
        public int A;
        public WriteCounter Counter;
    }

    public struct FixedCounter
    {
        public int B;

        public int Twice() => this.B * 2;
    }

    public struct ReadonlyCounter
    {
        public int B;

        public readonly int Read() => this.B;
    }

    public sealed class ReadonlyCounterHolder
    {
        public int B;
        public ReadonlyCounter Counter;

        public int Next() => ++this.B;
    }

    public struct FixedHolder
    {
        public int A;
        public FixedCounter Counter;
    }

    public abstract class WherePayloadWriteTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private readonly bool columnar;

        internal WherePayloadWriteTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(columnar))
            => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void SeparateFieldByReference()
            => Assert.AreEqual("3", Run(Separate(new WritePoint { A = 1, B = 3 }).Where(p => PayloadWriteHelpers.Increment(ref p.B)), p => p.B));

        [TestMethod, TestCategory("Gated")]
        public void FusedFieldByReference()
            => Assert.AreEqual("3", Run(Fused(new WritePoint { A = 1, B = 3 }).Where(p => PayloadWriteHelpers.Increment(ref p.B)), p => p.B));

        [TestMethod, TestCategory("Gated")]
        public void SeparateNestedFieldByReference()
            => Assert.AreEqual("3", Run(Separate(Holder(3)).Where(p => PayloadWriteHelpers.Increment(ref p.Counter.B)), p => p.Counter.B));

        [TestMethod, TestCategory("Gated")]
        public void SeparateMethodOnAStructField()
            => Assert.AreEqual("3", Run(Separate(Holder(3)).Where(p => p.Counter.Next() > 0), p => p.Counter.B));

        [TestMethod, TestCategory("Gated")]
        public void SeparateGetterOnAStructField()
            => Assert.AreEqual("3", Run(Separate(Holder(3)).Where(p => p.Counter.Bumped > 0), p => p.Counter.B));

        [TestMethod, TestCategory("Gated")]
        public void SeparateIncrementInAHandBuiltPredicate()
        {
            var p = Expression.Parameter(typeof(WritePoint), "p");
            var predicate = Expression.Lambda<Func<WritePoint, bool>>(
                Expression.Equal(Expression.PreIncrementAssign(Expression.Field(p, "B")), Expression.Constant(4)), p);
            Assert.AreEqual("3", Run(Separate(new WritePoint { A = 1, B = 3 }).Where(predicate), q => q.B));
        }

        [TestMethod, TestCategory("Gated")]
        public void FusedMethodOnAStructField()
            => Assert.AreEqual("3", Run(Fused(Holder(3)).Where(p => p.Counter.Next() > 0), p => p.Counter.B));

        [TestMethod, TestCategory("Gated")]
        public void FusedMethodOnThePayload()
            => Assert.AreEqual("3", Run(Fused(new WriteCounter { B = 3 }).Where(p => p.Next() > 0), p => p.B));

        [TestMethod, TestCategory("Gated")]
        public void SeparateWholePayloadByReference()
            => Assert.AreEqual("3", Run(Separate(3).Where(p => PayloadWriteHelpers.Increment(ref p)), p => p));

        [TestMethod, TestCategory("Gated")]
        public void FusedWholePayloadByReference()
            => Assert.AreEqual("3", Run(Fused(3).Where(p => PayloadWriteHelpers.Increment(ref p)), p => p));

        [TestMethod, TestCategory("Gated")]
        public void SeparateEachRowItsOwnCopy()
        {
            var query = Separate(new WritePoint { B = 1 }, new WritePoint { B = 2 }, new WritePoint { B = 3 })
                .Where(p => PayloadWriteHelpers.Increment(ref p.B) && p.B > 2);
            Assert.AreEqual("2 3", Run(query, p => p.B));
        }

        [TestMethod, TestCategory("Gated")]
        public void FusedConsecutiveWheresEachGetACopy()
            => Assert.AreEqual("3", Run(Fused(new WritePoint { A = 1, B = 3 }).Where(p => PayloadWriteHelpers.Increment(ref p.B)).Where(p => p.B == 3), p => p.B));

        [TestMethod, TestCategory("Gated")]
        public void FusedNestedLambdaNamedLikeAnEarlierLocal()
            => Assert.AreEqual("3", Run(Fused(new WritePoint { A = 1, B = 3 }).Where(p => new[] { 3 }.Any(var1 => var1 == p.B)), p => p.B));

        [TestMethod, TestCategory("Gated")]
        public void FusedNestedLambdaNamedLikeTheCopy()
            => Assert.AreEqual("3", Run(Fused(new WritePoint { A = 1, B = 3 }).Where(p => new[] { 3 }.Any(wherePayload => wherePayload == p.B)), p => p.B));

        // C# reads this nested parameter's name as wherePayload.
        [TestMethod, TestCategory("Gated")]
        public void FusedNestedLambdaNamedLikeTheCopyWithAnEscape()
        {
            var p = Expression.Parameter(typeof(WritePoint), "p");
            var x = Expression.Parameter(typeof(int), "\\u0077herePayload");
            var any = Expression.Call(
                typeof(Enumerable),
                nameof(Enumerable.Any),
                new[] { typeof(int) },
                Expression.NewArrayInit(typeof(int), Expression.Constant(3)),
                Expression.Lambda<Func<int, bool>>(Expression.Equal(x, Expression.Field(p, "B")), x));
            var predicate = Expression.Lambda<Func<WritePoint, bool>>(any, p);
            Assert.AreEqual("3", Run(Fused(new WritePoint { A = 1, B = 3 }).Where(predicate), q => q.B));
        }

        // The Where runs in row mode, so the query is not columnar in either class.
        [TestMethod, TestCategory("Gated")]
        public void SeparateStructWithAConstructor()
        {
            var query = Separate(new ConstructedPoint { A = 1, B = 3 }).Where(p => PayloadWriteHelpers.Increment(ref p.B));
            Assert.IsFalse(query.Properties.IsColumnar);
            Assert.AreEqual("3", string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.B)));
        }

        // A readonly struct method does not write, so the class predicate keeps #101's one payload per row, and
        // the two calls to Next compare 4 with 5, as in row mode.
        [TestMethod, TestCategory("Gated")]
        public void SeparateClassWithAReadonlyStructMethod()
        {
            var query = Separate(new ReadonlyCounterHolder { B = 3, Counter = new ReadonlyCounter { B = 1 } })
                .Where(p => p.Counter.Read() > 0 && p.Next() == p.Next());
            Assert.AreEqual(string.Empty, Run(query, p => p.B));
        }

        [TestMethod, TestCategory("Gated")]
        public void SeparateMethodThatDoesNotWriteControl()
            => Assert.AreEqual("3", Run(Separate(new FixedHolder { A = 1, Counter = new FixedCounter { B = 3 } }).Where(p => p.Counter.Twice() == 6), p => p.Counter.B));

        [TestMethod, TestCategory("Gated")]
        public void FusedMethodThatDoesNotWriteControl()
            => Assert.AreEqual("3", Run(Fused(new FixedHolder { A = 1, Counter = new FixedCounter { B = 3 } }).Where(p => p.Counter.Twice() == 6), p => p.Counter.B));

        [TestMethod, TestCategory("Gated")]
        public void SeparateClassFieldByReferenceControl()
            => Assert.AreEqual("4", Run(Separate(new WritePointClass { A = 1, B = 3 }).Where(p => PayloadWriteHelpers.Increment(ref p.B)), p => p.B));

        [TestMethod, TestCategory("Gated")]
        public void FusedClassFieldByReferenceControl()
            => Assert.AreEqual("4", Run(Fused(new WritePointClass { A = 1, B = 3 }).Where(p => PayloadWriteHelpers.Increment(ref p.B)), p => p.B));

        [TestMethod, TestCategory("Gated")]
        public void SeparateMemberReadControl()
            => Assert.AreEqual("3", Run(Separate(new WritePoint { A = 1, B = 3 }, new WritePoint { A = 1, B = 5 }).Where(p => p.B == 3), p => p.B));

        private static WriteHolder Holder(int b) => new WriteHolder { A = 1, Counter = new WriteCounter { B = b } };

        private static IStreamable<Empty, T> Fused<T>(params T[] values)
            => values.Select((v, i) => StreamEvent.CreatePoint(i + 1, v)).ToObservable().ToStreamable();

        // The Union keeps the Where out of the ingress, so it runs as a separate operator.
        private static IStreamable<Empty, T> Separate<T>(params T[] values)
            => Fused(values).Union(Array.Empty<StreamEvent<T>>().ToObservable().ToStreamable());

        private string Run<T>(IStreamable<Empty, T> query, Func<T, int> show)
        {
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            return string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => show(e.Payload)));
        }
    }

    [TestClass]
    public class WherePayloadWriteTestsRow : WherePayloadWriteTestsBase
    {
        public WherePayloadWriteTestsRow() : base(false) { }
    }

    [TestClass]
    public class WherePayloadWriteTestsColumnar : WherePayloadWriteTestsBase
    {
        public WherePayloadWriteTestsColumnar() : base(true) { }
    }
}
