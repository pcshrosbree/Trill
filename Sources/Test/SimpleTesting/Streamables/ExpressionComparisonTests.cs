using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Sharding;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct ExpressionComparisonPair
    {
        public Guid A;
        public Guid B;
    }

    public struct ExpressionComparisonEvent
    {
        public long K;
        public int I;
        public double D;
        public ExpressionComparisonPair P;
        public long X;
        public long Y;
    }

    // One class whose behavior depends on a field: two instances differ only in the receiver of a call.
    public sealed class ExpressionComparisonScale
    {
        public static readonly ExpressionComparisonScale Two = new ExpressionComparisonScale(2);
        public static readonly ExpressionComparisonScale Three = new ExpressionComparisonScale(3);
        private readonly long factor;

        public ExpressionComparisonScale(long factor) => this.factor = factor;

        public long Apply(long x) => this.factor * x;
    }

    // Byte arithmetic that saturates at 255.
    public struct ExpressionComparisonSat8
    {
        public readonly int V;

        public ExpressionComparisonSat8(int v) => this.V = v;

        public static implicit operator ExpressionComparisonSat8(long x) => new ExpressionComparisonSat8((int)Math.Min(255, Math.Max(0, x)));

        public static implicit operator long(ExpressionComparisonSat8 s) => s.V;

        public static ExpressionComparisonSat8 operator +(ExpressionComparisonSat8 a, ExpressionComparisonSat8 b) => new ExpressionComparisonSat8(Math.Min(255, a.V + b.V));
    }

    // Byte arithmetic that wraps.
    public struct ExpressionComparisonWrap8
    {
        public readonly int V;

        public ExpressionComparisonWrap8(int v) => this.V = v;

        public static implicit operator ExpressionComparisonWrap8(long x) => new ExpressionComparisonWrap8((int)(x & 255));

        public static implicit operator long(ExpressionComparisonWrap8 s) => s.V;

        public static ExpressionComparisonWrap8 operator +(ExpressionComparisonWrap8 a, ExpressionComparisonWrap8 b) => new ExpressionComparisonWrap8((a.V + b.V) & 255);
    }

    // Static methods for trees built with the Expression API, which can name the method of an operator node.
    public static class ExpressionComparisonOps
    {
        public static long Twice(long a) => 2 * a;

        public static long Thrice(long a) => 3 * a;

        public static long Plus(long a, long b) => a + b;

        public static long Times(long a, long b) => a * b;

        public static bool SameParity(long a, long b) => (a & 1) == (b & 1);

        public static bool Same(long a, long b) => a == b;

        public static int Low(long a) => (int)a;

        public static int Parity(long a) => (int)(a & 1);
    }

    // Two interfaces with an indexer each, implemented explicitly by one class with different results.
    public interface IExpressionComparisonFirstIndex
    {
        int this[int i] { get; }
    }

    public interface IExpressionComparisonSecondIndex
    {
        int this[int i] { get; }
    }

    public sealed class ExpressionComparisonTwoIndexers : IExpressionComparisonFirstIndex, IExpressionComparisonSecondIndex
    {
        int IExpressionComparisonFirstIndex.this[int i] => 1;

        int IExpressionComparisonSecondIndex.this[int i] => 2;
    }

    // An indexer, and one that hides it in a derived class.
    public class ExpressionComparisonBaseIndexer
    {
        public int this[int i] => 1;
    }

    public sealed class ExpressionComparisonHidingIndexer : ExpressionComparisonBaseIndexer
    {
        public new int this[int i] => 2;
    }

    internal static class ExpressionComparisonTrees
    {
        public const long NegativeNaN = unchecked((long)0xFFF8000000000000);
        public const long PositiveNaN = 0x7FF8000000000000;

        // e => (long)Math.CopySign((double)e.X, c), with c the double whose bits are given. The C# compiler folds
        // -double.NaN back to double.NaN, so NaN constants with other bits are built with Expression.Constant.
        public static Expression<Func<ExpressionComparisonEvent, long>> CopySignX(long bits)
        {
            var e = Expression.Parameter(typeof(ExpressionComparisonEvent), "e");
            return Expression.Lambda<Func<ExpressionComparisonEvent, long>>(Expression.Convert(CopySign(Expression.Convert(Expression.Field(e, "X"), typeof(double)), bits), typeof(long)), e);
        }

        // a => (int)Math.CopySign((double)a, c).
        public static Expression<Func<long, int>> CopySignHash(long bits)
        {
            var a = Expression.Parameter(typeof(long), "a");
            return Expression.Lambda<Func<long, int>>(Expression.Convert(CopySign(Expression.Convert(a, typeof(double)), bits), typeof(int)), a);
        }

        private static Expression CopySign(Expression value, long bits)
            => Expression.Call(typeof(Math).GetMethod("CopySign", new[] { typeof(double), typeof(double) }), value, Expression.Constant(BitConverter.Int64BitsToDouble(bits)));

        // e => Negate(e.X, method).
        public static Expression<Func<ExpressionComparisonEvent, long>> Negate(string method)
        {
            var e = Expression.Parameter(typeof(ExpressionComparisonEvent), "e");
            return Expression.Lambda<Func<ExpressionComparisonEvent, long>>(Expression.Negate(Expression.Field(e, "X"), typeof(ExpressionComparisonOps).GetMethod(method)), e);
        }

        // e => Add(e.X, e.X, method).
        public static Expression<Func<ExpressionComparisonEvent, long>> Add(string method)
        {
            var e = Expression.Parameter(typeof(ExpressionComparisonEvent), "e");
            var x = Expression.Field(e, "X");
            return Expression.Lambda<Func<ExpressionComparisonEvent, long>>(Expression.Add(x, x, typeof(ExpressionComparisonOps).GetMethod(method)), e);
        }

        // (a, b) => Equal(a, b, method).
        public static Expression<Func<long, long, bool>> EqualVia(string method)
        {
            var a = Expression.Parameter(typeof(long), "a");
            var b = Expression.Parameter(typeof(long), "b");
            return Expression.Lambda<Func<long, long, bool>>(Expression.Equal(a, b, false, typeof(ExpressionComparisonOps).GetMethod(method)), a, b);
        }

        // a => Convert(a, int, method).
        public static Expression<Func<long, int>> HashVia(string method)
        {
            var a = Expression.Parameter(typeof(long), "a");
            return Expression.Lambda<Func<long, int>>(Expression.Convert(a, typeof(int), typeof(ExpressionComparisonOps).GetMethod(method)), a);
        }

        // A hash that reads the low bit of a captured double.
        public static Expression<Func<long, int>> TagHash(double tag) => a => (int)a + (int)(BitConverter.DoubleToInt64Bits(tag) & 1);

        // A comparer factory: each call makes a new closure over salt. Equality compares the bits of two doubles.
        public static IEqualityComparerExpression<double> BitsComparer()
        {
            int salt = 0;
            return new EqualityComparerExpression<double>(
                (x, y) => BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y),
                x => unchecked((int)BitConverter.DoubleToInt64Bits(x)) ^ salt);
        }

        // A comparer factory whose hash reads a captured salt, with an action that assigns the salt later.
        public static IEqualityComparerExpression<long> SaltComparer(out Action<int> setSalt)
        {
            int salt = 0;
            setSalt = value => salt = value;
            return new EqualityComparerExpression<long>((a, b) => a == b, a => (int)a ^ salt);
        }

        // A comparer factory whose Equals reads a captured flag; its hash captures nothing.
        public static IEqualityComparerExpression<long> FlagComparer()
        {
            bool flag = true;
            return new EqualityComparerExpression<long>((a, b) => a == b && flag, a => (int)a);
        }

        // e => { var local = Scale.Two; return local; }.Apply(e.X): a receiver that declares a block variable.
        public static Expression<Func<ExpressionComparisonEvent, long>> BlockReceiver()
        {
            var e = Expression.Parameter(typeof(ExpressionComparisonEvent), "e");
            var local = Expression.Variable(typeof(ExpressionComparisonScale), "local");
            var receiver = Expression.Block(new[] { local }, Expression.Assign(local, Expression.Field(null, typeof(ExpressionComparisonScale), "Two")), local);
            return Expression.Lambda<Func<ExpressionComparisonEvent, long>>(Expression.Call(receiver, typeof(ExpressionComparisonScale).GetMethod("Apply"), Expression.Field(e, "X")), e);
        }

        // e => { loop { break; } return Scale.Two; }.Apply(e.X): a receiver with a loop that has no continue label.
        // Both selectors share the loop's body and label.
        public static Expression<Func<ExpressionComparisonEvent, long>> LoopReceiver(LabelTarget done, Expression body)
        {
            var e = Expression.Parameter(typeof(ExpressionComparisonEvent), "e");
            var receiver = Expression.Block(Expression.Loop(body, done), Expression.Field(null, typeof(ExpressionComparisonScale), "Two"));
            return Expression.Lambda<Func<ExpressionComparisonEvent, long>>(Expression.Call(receiver, typeof(ExpressionComparisonScale).GetMethod("Apply"), Expression.Field(e, "X")), e);
        }

        // e => ((p => ((p => Scale.Two)(p)))(e.X)).Apply(e.X): a receiver whose inner lambda declares the outer lambda's
        // parameter again.
        public static Expression<Func<ExpressionComparisonEvent, long>> ShadowReceiver(ParameterExpression p)
        {
            var e = Expression.Parameter(typeof(ExpressionComparisonEvent), "e");
            var inner = Expression.Lambda<Func<long, ExpressionComparisonScale>>(Expression.Field(null, typeof(ExpressionComparisonScale), "Two"), p);
            var outer = Expression.Lambda<Func<long, ExpressionComparisonScale>>(Expression.Invoke(inner, p), p);
            var receiver = Expression.Invoke(outer, Expression.Field(e, "X"));
            return Expression.Lambda<Func<ExpressionComparisonEvent, long>>(Expression.Call(receiver, typeof(ExpressionComparisonScale).GetMethod("Apply"), Expression.Field(e, "X")), e);
        }

        // Parity equality, with a hash whose receiver declares a lambda: two copies are the same comparer.
        public static IEqualityComparerExpression<long> ParityViaConvertAll()
            => new EqualityComparerExpression<long>((a, b) => (a & 1) == (b & 1), a => new List<int>(new[] { 0, 1 }).ConvertAll(item => item).IndexOf((int)(a & 1)));

        // p => ((bool?)(p == 0)) ?? true ? 1 : 0, with the comparison built with the given liftToNull. At p = null, false
        // gives 0 and true gives 1. The conversion to bool? gives both trees the same types above the comparison.
        public static Expression<Func<long?, int>> LiftedHash(bool liftToNull)
        {
            var p = Expression.Parameter(typeof(long?), "p");
            var equal = Expression.Equal(p, Expression.Constant(0L, typeof(long?)), liftToNull, null);
            var test = Expression.Coalesce(Expression.Convert(equal, typeof(bool?)), Expression.Constant(true));
            return Expression.Lambda<Func<long?, int>>(Expression.Condition(test, Expression.Constant(1), Expression.Constant(0)), p);
        }

        // Parity equality, with a hash whose receiver is a list initializer.
        public static IEqualityComparerExpression<long> ParityViaListInit()
            => new EqualityComparerExpression<long>((x, y) => (x & 1) == (y & 1), a => new List<int> { 0, 1 }.IndexOf((int)(a & 1)));

        // Parity equality, with a hash whose receiver is a block that declares an empty list: the hash is always -1.
        public static IEqualityComparerExpression<long> ParityViaBlockVariable()
        {
            var a = Expression.Parameter(typeof(long), "a");
            var list = Expression.Variable(typeof(List<int>), "list");
            var receiver = Expression.Block(new[] { list }, Expression.Assign(list, Expression.New(typeof(List<int>))), list);
            var hash = Expression.Lambda<Func<long, int>>(Expression.Call(receiver, typeof(List<int>).GetMethod("IndexOf", new[] { typeof(int) }), Expression.Convert(a, typeof(int))), a);
            return new EqualityComparerExpression<long>((x, y) => (x & 1) == (y & 1), hash);
        }

        // p => (int)(p ?? 0, converted with v => v + add): a coalesce whose conversion lambda is the only difference.
        public static Expression<Func<long?, int>> CoalesceHash(long add)
        {
            var p = Expression.Parameter(typeof(long?), "p");
            var v = Expression.Parameter(typeof(long), "v");
            var conversion = Expression.Lambda<Func<long, long>>(Expression.Add(v, Expression.Constant(add)), v);
            return Expression.Lambda<Func<long?, int>>(Expression.Convert(Expression.Coalesce(p, Expression.Constant(0L), conversion), typeof(int)), p);
        }

        // (x, y, z) => x + 0 and (y, z, x) => z + 0 over one set of parameter objects, each called from a hash as
        // a => f(a, 0, 0): at key 1 the hashes are 1 and 0.
        public static void CyclicHashes(out Expression<Func<long, int>> first, out Expression<Func<long, int>> second)
        {
            var x = Expression.Parameter(typeof(long), "x");
            var y = Expression.Parameter(typeof(long), "y");
            var z = Expression.Parameter(typeof(long), "z");
            var f = Expression.Lambda<Func<long, long, long, long>>(Expression.Add(x, Expression.Constant(0L)), x, y, z);
            var g = Expression.Lambda<Func<long, long, long, long>>(Expression.Add(z, Expression.Constant(0L)), y, z, x);
            Expression<Func<long, int>> Hash(Expression function)
            {
                var a = Expression.Parameter(typeof(long), "a");
                return Expression.Lambda<Func<long, int>>(Expression.Convert(Expression.Invoke(function, a, Expression.Constant(0L), Expression.Constant(0L)), typeof(int)), a);
            }

            first = Hash(f);
            second = Hash(g);
        }

        // The default comparer's Equals, and a hash that reads A twice: the default hash with the second receiver changed.
        public static IEqualityComparerExpression<ExpressionComparisonPair> PairHash()
            => new EqualityComparerExpression<ExpressionComparisonPair>(EqualityComparerExpression<ExpressionComparisonPair>.Default.GetEqualsExpr(), a => a.A.GetHashCode() ^ a.A.GetHashCode());
    }

    // Two expressions that compute different functions must not compare equal, wherever Trill compares expressions.
    // Most pairs below differ in one kind of node: the receiver of a call, the method of an operator, the target type
    // of a conversion, or the bits of a double constant. The rest are equivalent trees that must still be computed or
    // matched. Every test runs in row mode and in both columnar modes.
    public abstract class ExpressionComparisonTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal ExpressionComparisonTestsBase(bool columnar, bool strict)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(strict)) { }

        internal static IStreamable<Empty, ExpressionComparisonEvent> Input(long k = 0, long x = 0, long y = 0, double d = 0)
            => new[]
            {
                StreamEvent.CreateInterval(1, 11, new ExpressionComparisonEvent
                {
                    K = k, X = x, Y = y, D = d,
                    P = new ExpressionComparisonPair { A = new Guid("00000000-0000-0000-0000-000000000001"), B = new Guid("00000000-0000-0000-0000-000000000002") },
                }),
            }.ToObservable().ToStreamable();

        private static string Sums(long x, long y, Expression<Func<ExpressionComparisonEvent, long>> first, Expression<Func<ExpressionComparisonEvent, long>> second)
            => string.Join(", ", Input(x: x, y: y).Aggregate(w => w.Sum(first), w => w.Sum(second), (a, b) => a + " and " + b)
                .ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable().Where(e => e.IsData).Select(e => e.Payload));

        internal static IStreamable<Empty, ExpressionComparisonEvent> WithK(
            IStreamable<Empty, ExpressionComparisonEvent> s, Expression<Func<long, long, bool>> equals, Expression<Func<long, int>> hash)
            => s.SetProperty().EqualityComparer(e => e.K, equals, hash);

        internal static int Join<T>(IStreamable<Empty, ExpressionComparisonEvent> left, IStreamable<Empty, ExpressionComparisonEvent> right, Expression<Func<ExpressionComparisonEvent, T>> key)
            => left.Join(right, key, key, (l, r) => l.X + r.X).ToStreamEventObservable().ToEnumerable().Count(e => e.IsData);

        internal static IShardedStreamable<T, ExpressionComparisonEvent> Redistributed<T>(
            IStreamable<Empty, ExpressionComparisonEvent> input, Expression<Func<ExpressionComparisonEvent, T>> key, int shards)
            => input.Shard(1).ReKey(key).Query(s => s.Select(e => new ExpressionComparisonEvent { K = e.K, I = e.I, D = e.D, P = e.P, X = e.X, Y = e.Y }))
                .ReDistribute(new LocalLocationDescriptor(shards));

        internal static void GuardThrows(Func<int, IShardedStreamable<long, ExpressionComparisonEvent>> left, Func<int, IShardedStreamable<long, ExpressionComparisonEvent>> right)
        {
            foreach (var shards in new[] { 2, 6, 8 })
            {
                var l = left(shards);
                var r = right(shards);
                Assert.ThrowsException<ShardedKeyComparerMismatchException>(() => l.Query(r, (a, b) => a.WhereNotExists(b)), "at " + shards + " shards");
            }
        }

        // Aggregate: Combine shares one state between two aggregates whose expressions compare equal.
        [TestMethod, TestCategory("Gated")]
        public void SumsOnDifferentReceiversAreBothComputed()
            => Assert.AreEqual("10 and 15", Sums(5, 0, e => ExpressionComparisonScale.Two.Apply(e.X), e => ExpressionComparisonScale.Three.Apply(e.X)));

        [TestMethod, TestCategory("Gated")]
        public void SumsWithDifferentUserOperatorsAreBothComputed()
            => Assert.AreEqual("255 and 44", Sums(200, 100, e => (long)((ExpressionComparisonSat8)e.X + (ExpressionComparisonSat8)e.Y), e => (long)((ExpressionComparisonWrap8)e.X + (ExpressionComparisonWrap8)e.Y)));

        [TestMethod, TestCategory("Gated")]
        public void SumsWithDifferentUnaryMethodsAreBothComputed()
            => Assert.AreEqual("10 and 15", Sums(5, 0, ExpressionComparisonTrees.Negate("Twice"), ExpressionComparisonTrees.Negate("Thrice")));

        [TestMethod, TestCategory("Gated")]
        public void SumsWithDifferentBinaryMethodsAreBothComputed()
            => Assert.AreEqual("6 and 9", Sums(3, 0, ExpressionComparisonTrees.Add("Plus"), ExpressionComparisonTrees.Add("Times")));

        [TestMethod, TestCategory("Gated")]
        public void SumsWithDifferentConversionTypesAreBothComputed()
            => Assert.AreEqual("16777216 and 16777217", Sums(16777217, 0, e => (long)(float)e.X, e => (long)(double)e.X));

        [TestMethod, TestCategory("Gated")]
        public void SumsWithNaNConstantsOfDifferentSignsAreBothComputed()
            => Assert.AreEqual("-5 and 5", Sums(5, 0, ExpressionComparisonTrees.CopySignX(ExpressionComparisonTrees.NegativeNaN), ExpressionComparisonTrees.CopySignX(ExpressionComparisonTrees.PositiveNaN)));

        [TestMethod, TestCategory("Gated")]
        public void SumsWithSignedZeroConstantsAreBothComputed()
            => Assert.AreEqual("5 and -5", Sums(5, 0, e => (long)Math.CopySign(e.X, 0.0), e => (long)Math.CopySign(e.X, -0.0)));

        // Two equivalent selectors whose receivers declare their own block variables: comparing them must not throw.
        [TestMethod, TestCategory("Gated")]
        public void SumsWithBlockVariablesInTheirReceiversAreComputed()
            => Assert.AreEqual("10 and 10", Sums(5, 0, ExpressionComparisonTrees.BlockReceiver(), ExpressionComparisonTrees.BlockReceiver()));

        // Receivers with a loop that has no continue label, or with a lambda that declares its enclosing lambda's
        // parameter again: comparing them must not throw, and both sums are computed.
        [TestMethod, TestCategory("Gated")]
        public void SumsWithLoopsInTheirReceiversAreComputed()
        {
            var done = Expression.Label("done");
            var body = Expression.Break(done);
            Assert.AreEqual("10 and 10", Sums(5, 0, ExpressionComparisonTrees.LoopReceiver(done, body), ExpressionComparisonTrees.LoopReceiver(done, body)));
        }

        [TestMethod, TestCategory("Gated")]
        public void SumsWithShadowedParametersInTheirReceiversAreComputed()
        {
            var p = Expression.Parameter(typeof(long), "p");
            Assert.AreEqual("10 and 10", Sums(5, 0, ExpressionComparisonTrees.ShadowReceiver(p), ExpressionComparisonTrees.ShadowReceiver(p)));
        }

        // Two copies of one comparer whose Equals reads a captured flag: the join keeps their function, as a comparer that
        // generated code can write.
        [TestMethod, TestCategory("Gated")]
        public void JoinOfTwoCopiesOfAComparerWithACapturedFlagFindsTheMatch()
            => Assert.AreEqual(1, Join(
                Input(k: 5).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.FlagComparer()),
                Input(k: 5).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.FlagComparer()), e => e.K));

        // Two copies of one comparer whose hash has a lambda in a receiver: the join keeps their equality, so 1 and 3
        // match.
        [TestMethod, TestCategory("Gated")]
        public void JoinOfTwoCopiesOfAComparerWithALambdaInAReceiverFindsTheMatch()
            => Assert.AreEqual(1, Join(
                Input(k: 1).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.ParityViaConvertAll()),
                Input(k: 3).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.ParityViaConvertAll()), e => e.K));

        // Join: inputs with different comparers fall back to the default comparer and are rehashed.
        [TestMethod, TestCategory("Gated")]
        public void JoinWithComparersOnDifferentReceiversFindsTheMatch()
            => Assert.AreEqual(1, Join(
                WithK(Input(k: 5), (a, b) => a == b, a => (int)ExpressionComparisonScale.Two.Apply(a)),
                WithK(Input(k: 5), (a, b) => a == b, a => (int)ExpressionComparisonScale.Three.Apply(a)), e => e.K));

        [TestMethod, TestCategory("Gated")]
        public void JoinWithComparersUsingDifferentUserOperatorsFindsTheMatch()
            => Assert.AreEqual(1, Join(
                WithK(Input(k: 200), (a, b) => a == b, a => (int)(long)((ExpressionComparisonSat8)a + (ExpressionComparisonSat8)a)),
                WithK(Input(k: 200), (a, b) => a == b, a => (int)(long)((ExpressionComparisonWrap8)a + (ExpressionComparisonWrap8)a)), e => e.K));

        [TestMethod, TestCategory("Gated")]
        public void JoinWithFloatAndDoubleComparersFindsTheMatch()
            => Assert.AreEqual(1, Join(
                WithK(Input(k: 16777217), (a, b) => (float)a == (float)b, a => (int)(float)a),
                WithK(Input(k: 16777217), (a, b) => (double)a == (double)b, a => (int)(double)a), e => e.K));

        [TestMethod, TestCategory("Gated")]
        public void JoinWithNaNConstantsOfDifferentSignsFindsTheMatch()
            => Assert.AreEqual(1, Join(
                WithK(Input(k: 1), (a, b) => a == b, ExpressionComparisonTrees.CopySignHash(ExpressionComparisonTrees.NegativeNaN)),
                WithK(Input(k: 1), (a, b) => a == b, ExpressionComparisonTrees.CopySignHash(ExpressionComparisonTrees.PositiveNaN)), e => e.K));

        [TestMethod, TestCategory("Gated")]
        public void JoinWithSignedZeroConstantsFindsTheMatch()
            => Assert.AreEqual(1, Join(
                WithK(Input(k: 1), (a, b) => a == b, a => (int)Math.CopySign(a, 0.0)),
                WithK(Input(k: 1), (a, b) => a == b, a => (int)Math.CopySign(a, -0.0)), e => e.K));

        // The default comparer of a struct of two Guids, against its Equals with a hash that reads A twice.
        [TestMethod, TestCategory("Gated")]
        public void JoinOfTheDefaultComparerAndAHashWithAnotherReceiverFindsTheMatch()
            => Assert.AreEqual(1, Join(Input(), Input().SetProperty().EqualityComparer(e => e.P, ExpressionComparisonTrees.PairHash()), e => e.P));

        // Fork #150: SetProperty().KeyEqualityComparer sets a comparer whose hash differs from the default's on a
        // stream whose stored hashes are the default's. The other input's hashes really are that comparer's.
        private static IShardedStreamable<int, ExpressionComparisonEvent> Keys(bool withComparer)
        {
            var events = Enumerable.Range(0, 50).Select(k => StreamEvent.CreateInterval(1, 11, new ExpressionComparisonEvent { I = k, X = k })).ToArray();
            IStreamable<Empty, ExpressionComparisonEvent> input = events.ToObservable().ToStreamable();
            if (withComparer) input = input.SetProperty().EqualityComparer(e => e.I, (x, y) => x == y, x => x ^ 1);
            return input.Shard(1).ReKey(e => e.I);
        }

        private static readonly IEqualityComparerExpression<int> XorOne = new EqualityComparerExpression<int>((x, y) => x == y, x => x ^ 1);

        [TestMethod, TestCategory("Gated")]
        public void JoinAfterTheKeyComparerSetterFindsEveryMatch()
        {
            var result = Keys(false).Query(Keys(true), (l, r) => l.SetProperty().KeyEqualityComparer(XorOne).Join(r, (a, b) => a.X + b.X));
            Assert.AreEqual(50, result.Unshuffle().Unshard().ToStreamEventObservable().ToEnumerable().Count(e => e.IsData));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereNotExistsAfterTheKeyComparerSetterRemovesEveryEvent()
        {
            var result = Keys(false).Query(Keys(true), (l, r) => l.SetProperty().KeyEqualityComparer(XorOne).WhereNotExists(r));
            Assert.AreEqual(0, result.Unshuffle().Unshard().ToStreamEventObservable().ToEnumerable().Count(e => e.IsData));
        }

        // The guard: inputs redistributed with comparers that differ in one part of the tree must throw.
        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForComparersOnDifferentReceivers()
            => GuardThrows(
                n => Redistributed(WithK(Input(k: 5), (a, b) => a == b, a => (int)ExpressionComparisonScale.Two.Apply(a)), e => e.K, n),
                n => Redistributed(WithK(Input(k: 5), (a, b) => a == b, a => (int)ExpressionComparisonScale.Three.Apply(a)), e => e.K, n));

        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForComparersUsingDifferentUserOperators()
            => GuardThrows(
                n => Redistributed(WithK(Input(k: 200), (a, b) => a == b, a => (int)(long)((ExpressionComparisonSat8)a + (ExpressionComparisonSat8)a)), e => e.K, n),
                n => Redistributed(WithK(Input(k: 200), (a, b) => a == b, a => (int)(long)((ExpressionComparisonWrap8)a + (ExpressionComparisonWrap8)a)), e => e.K, n));

        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForFloatAndDoubleComparers()
            => GuardThrows(
                n => Redistributed(WithK(Input(k: 16777217), (a, b) => (float)a == (float)b, a => (int)(float)a), e => e.K, n),
                n => Redistributed(WithK(Input(k: 16777217), (a, b) => (double)a == (double)b, a => (int)(double)a), e => e.K, n));

        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForNaNConstantsOfDifferentSigns()
            => GuardThrows(
                n => Redistributed(WithK(Input(k: 1), (a, b) => a == b, ExpressionComparisonTrees.CopySignHash(ExpressionComparisonTrees.NegativeNaN)), e => e.K, n),
                n => Redistributed(WithK(Input(k: 1), (a, b) => a == b, ExpressionComparisonTrees.CopySignHash(ExpressionComparisonTrees.PositiveNaN)), e => e.K, n));

        // Lifted comparisons that differ only in IsLiftedToNull, under a conversion to bool?.
        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForLiftedComparisonsThatDifferInLiftToNull()
        {
            foreach (var shards in new[] { 2, 6, 8 })
            {
                var l = Redistributed(Input(k: 1).SetProperty().EqualityComparer(e => (long?)e.K, (a, b) => a == b, ExpressionComparisonTrees.LiftedHash(false)), e => (long?)e.K, shards);
                var r = Redistributed(Input(k: 1).SetProperty().EqualityComparer(e => (long?)e.K, (a, b) => a == b, ExpressionComparisonTrees.LiftedHash(true)), e => (long?)e.K, shards);
                Assert.ThrowsException<ShardedKeyComparerMismatchException>(() => l.Query(r, (a, b) => a.WhereNotExists(b)), "at " + shards + " shards");
            }
        }

        // Coalesce nodes that differ only in their conversion lambda.
        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForCoalesceConversionsThatDiffer()
        {
            foreach (var shards in new[] { 2, 6, 8 })
            {
                var l = Redistributed(Input(k: 1).SetProperty().EqualityComparer(e => (long?)e.K, (a, b) => a == b, ExpressionComparisonTrees.CoalesceHash(0)), e => (long?)e.K, shards);
                var r = Redistributed(Input(k: 1).SetProperty().EqualityComparer(e => (long?)e.K, (a, b) => a == b, ExpressionComparisonTrees.CoalesceHash(1)), e => (long?)e.K, shards);
                Assert.ThrowsException<ShardedKeyComparerMismatchException>(() => l.Query(r, (a, b) => a.WhereNotExists(b)), "at " + shards + " shards");
            }
        }

        // Hashes over one set of parameter objects declared in another order compute different functions.
        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForHashesThatDeclareSharedParametersInAnotherOrder()
        {
            ExpressionComparisonTrees.CyclicHashes(out var first, out var second);
            GuardThrows(
                n => Redistributed(WithK(Input(k: 1), (a, b) => a == b, first), e => e.K, n),
                n => Redistributed(WithK(Input(k: 1), (a, b) => a == b, second), e => e.K, n));
        }

        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForSignedZeroConstants()
            => GuardThrows(
                n => Redistributed(WithK(Input(k: 1), (a, b) => a == b, a => (int)Math.CopySign(a, 0.0)), e => e.K, n),
                n => Redistributed(WithK(Input(k: 1), (a, b) => a == b, a => (int)Math.CopySign(a, -0.0)), e => e.K, n));
    }

    [TestClass]
    public class ExpressionComparisonTestsRow : ExpressionComparisonTestsBase
    {
        public ExpressionComparisonTestsRow() : base(false, false) { }
    }

    [TestClass]
    public class ExpressionComparisonTestsColumnar : ExpressionComparisonTestsBase
    {
        public ExpressionComparisonTestsColumnar() : base(true, false) { }
    }

    [TestClass]
    public class ExpressionComparisonTestsColumnarStrict : ExpressionComparisonTestsBase
    {
        public ExpressionComparisonTestsColumnarStrict() : base(true, true) { }
    }

    // Most of these comparers read a captured local or use a method on a conversion. Code generation cannot write
    // them, so this group runs in row mode and in columnar mode with fallback allowed.
    public abstract class ExpressionComparisonFallbackTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal ExpressionComparisonFallbackTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(false)) { }

        private static IShardedStreamable<double, ExpressionComparisonEvent> OnD(double key, IEqualityComparerExpression<double> comparer, int shards)
            => ExpressionComparisonTestsBase.Redistributed(ExpressionComparisonTestsBase.Input(d: key).SetProperty().EqualityComparer(e => e.D, comparer), e => e.D, shards);

        private static IShardedStreamable<double, ExpressionComparisonEvent> OnD(double key, int shards)
            => ExpressionComparisonTestsBase.Redistributed(ExpressionComparisonTestsBase.Input(d: key), e => e.D, shards);

        private static int Joined(IShardedStreamable<double, ExpressionComparisonEvent> left, IShardedStreamable<double, ExpressionComparisonEvent> right)
            => left.Query(right, (l, r) => l.Join(r, (a, b) => a.X + b.X)).Unshuffle().Unshard().ToStreamEventObservable().ToEnumerable().Count(e => e.IsData);

        private static readonly double NaN0 = BitConverter.Int64BitsToDouble(0x7FF8000000000000);
        private static readonly double NaN1 = BitConverter.Int64BitsToDouble(0x7FF8000000000001);

        // Two comparers from one factory are the same to the guard, so the join must keep them: the two NaN keys
        // differ under bit equality at every shard count.
        [TestMethod, TestCategory("Gated")]
        public void JoinOfTwoCopiesOfOneComparerKeepsItAtEveryShardCount()
        {
            foreach (var shards in new[] { 1, 2, 8 })
                Assert.AreEqual(0, Joined(OnD(NaN0, ExpressionComparisonTrees.BitsComparer(), shards), OnD(NaN1, ExpressionComparisonTrees.BitsComparer(), shards)), "at " + shards + " shards");
        }

        [TestMethod, TestCategory("Gated")]
        public void JoinOfSignedZerosUnderTwoCopiesOfOneComparerFindsNoMatch()
        {
            foreach (var shards in new[] { 1, 2, 8 })
                Assert.AreEqual(0, Joined(OnD(0.0, ExpressionComparisonTrees.BitsComparer(), shards), OnD(-0.0, ExpressionComparisonTrees.BitsComparer(), shards)), "at " + shards + " shards");
        }

        // With the default comparer on both inputs, keys equal under it are placed on one shard, so the join finds the
        // match at every shard count.
        [TestMethod, TestCategory("Gated")]
        public void JoinUnderTheDefaultComparerFindsEqualKeysAtEveryShardCount()
        {
            foreach (var shards in new[] { 1, 2, 8 })
            {
                Assert.AreEqual(1, Joined(OnD(NaN0, shards), OnD(NaN1, shards)), "NaNs at " + shards + " shards");
                Assert.AreEqual(1, Joined(OnD(0.0, shards), OnD(-0.0, shards)), "zeros at " + shards + " shards");
            }
        }

        // Two copies of one comparer factory, equal when the query is built; the right copy's salt is then assigned
        // again. The stored hashes come from the live comparers, so the join and WhereNotExists must still rehash.
        private static IShardedStreamable<long, ExpressionComparisonEvent> SaltInput(out Action<int> setSalt)
            => ExpressionComparisonTestsBase.Input(k: 4).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.SaltComparer(out setSalt)).Shard(1).ReKey(e => e.K);

        [TestMethod, TestCategory("Gated")]
        public void JoinAfterACapturedSaltIsAssignedAgainFindsTheMatch()
        {
            var left = SaltInput(out _);
            var right = SaltInput(out var setRightSalt);
            var result = left.Query(right, (l, r) => l.Join(r, (a, b) => a.X + b.X)).Unshuffle().Unshard();
            setRightSalt(1);
            Assert.AreEqual(1, result.ToStreamEventObservable().ToEnumerable().Count(e => e.IsData));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereNotExistsAfterACapturedSaltIsAssignedAgainRemovesTheEvent()
        {
            var left = SaltInput(out _);
            var right = SaltInput(out var setRightSalt);
            var result = left.Query(right, (l, r) => l.WhereNotExists(r)).Unshuffle().Unshard();
            setRightSalt(1);
            Assert.AreEqual(0, result.ToStreamEventObservable().ToEnumerable().Count(e => e.IsData));
        }

        // A hash with a block-local variable. Each copy declares its own variable in the same place, so the two hashes
        // are the same. Comparing them used to throw.
        private static Expression<Func<long, int>> BlockLocalHash()
        {
            var key = Expression.Parameter(typeof(long), "key");
            var local = Expression.Variable(typeof(int), "local");
            return Expression.Lambda<Func<long, int>>(Expression.Block(new[] { local }, Expression.Assign(local, Expression.Convert(key, typeof(int))), local), key);
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereNotExistsWithBlockVariablesInBothHashesRemovesTheEvent()
        {
            var left = ExpressionComparisonTestsBase.WithK(ExpressionComparisonTestsBase.Input(k: 4), (a, b) => a == b, BlockLocalHash());
            var right = ExpressionComparisonTestsBase.WithK(ExpressionComparisonTestsBase.Input(k: 4), (a, b) => a == b, BlockLocalHash());
            var result = left.WhereNotExists(right, e => e.K, e => e.K);
            Assert.AreEqual(0, result.ToStreamEventObservable().ToEnumerable().Count(e => e.IsData));
        }

        // Two copies of one parity comparer whose hash has a list initializer or a block variable in a receiver: the join
        // keeps their equality, so 1 and 3 match.
        [TestMethod, TestCategory("Gated")]
        public void JoinOfTwoCopiesOfAComparerWithAListInitializerInAReceiverFindsTheMatch()
            => Assert.AreEqual(1, ExpressionComparisonTestsBase.Join(
                ExpressionComparisonTestsBase.Input(k: 1).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.ParityViaListInit()),
                ExpressionComparisonTestsBase.Input(k: 3).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.ParityViaListInit()), e => e.K));

        [TestMethod, TestCategory("Gated")]
        public void JoinOfTwoCopiesOfAComparerWithABlockVariableInAReceiverFindsTheMatch()
            => Assert.AreEqual(1, ExpressionComparisonTestsBase.Join(
                ExpressionComparisonTestsBase.Input(k: 1).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.ParityViaBlockVariable()),
                ExpressionComparisonTestsBase.Input(k: 3).SetProperty().EqualityComparer(e => e.K, ExpressionComparisonTrees.ParityViaBlockVariable()), e => e.K));

        [TestMethod, TestCategory("Gated")]
        public void JoinWithHashesThatDifferInAConversionMethodFindsTheMatch()
            => Assert.AreEqual(1, ExpressionComparisonTestsBase.Join(
                ExpressionComparisonTestsBase.WithK(ExpressionComparisonTestsBase.Input(k: 3), (a, b) => a == b, ExpressionComparisonTrees.HashVia("Low")),
                ExpressionComparisonTestsBase.WithK(ExpressionComparisonTestsBase.Input(k: 3), (a, b) => a == b, ExpressionComparisonTrees.HashVia("Parity")), e => e.K));

        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForHashesThatDifferInAConversionMethod()
            => ExpressionComparisonTestsBase.GuardThrows(
                n => ExpressionComparisonTestsBase.Redistributed(ExpressionComparisonTestsBase.WithK(ExpressionComparisonTestsBase.Input(k: 3), (a, b) => a == b, ExpressionComparisonTrees.HashVia("Low")), e => e.K, n),
                n => ExpressionComparisonTestsBase.Redistributed(ExpressionComparisonTestsBase.WithK(ExpressionComparisonTestsBase.Input(k: 3), (a, b) => a == b, ExpressionComparisonTrees.HashVia("Parity")), e => e.K, n));

        [TestMethod, TestCategory("Gated")]
        public void GuardThrowsForCapturedNaNTagsWithDifferentBits()
            => ExpressionComparisonTestsBase.GuardThrows(
                n => ExpressionComparisonTestsBase.Redistributed(ExpressionComparisonTestsBase.WithK(ExpressionComparisonTestsBase.Input(k: 1), (a, b) => a == b, ExpressionComparisonTrees.TagHash(BitConverter.Int64BitsToDouble(0x7FF8000000000001))), e => e.K, n),
                n => ExpressionComparisonTestsBase.Redistributed(ExpressionComparisonTestsBase.WithK(ExpressionComparisonTestsBase.Input(k: 1), (a, b) => a == b, ExpressionComparisonTrees.TagHash(BitConverter.Int64BitsToDouble(0x7FF8000000000002))), e => e.K, n));
    }

    [TestClass]
    public class ExpressionComparisonFallbackTestsRow : ExpressionComparisonFallbackTestsBase
    {
        public ExpressionComparisonFallbackTestsRow() : base(false) { }
    }

    [TestClass]
    public class ExpressionComparisonFallbackTestsColumnar : ExpressionComparisonFallbackTestsBase
    {
        public ExpressionComparisonFallbackTestsColumnar() : base(true) { }
    }

    // ComparerDifference.MayDiffer and EqualityComparer.IsEqual on pairs that differ in exactly one part of the tree.
    [TestClass]
    public class ExpressionComparisonUnitTests
    {
        private static IEqualityComparerExpression<long> Comparer(Expression<Func<long, long, bool>> equals, Expression<Func<long, int>> hash)
            => new EqualityComparerExpression<long>(equals, hash);

        private static void Differ(Expression<Func<long, long, bool>> equals1, Expression<Func<long, int>> hash1, Expression<Func<long, long, bool>> equals2, Expression<Func<long, int>> hash2)
            => Assert.IsTrue(ComparerDifference.MayDiffer(Comparer(equals1, hash1), Comparer(equals2, hash2)));

        [TestMethod, TestCategory("Gated")]
        public void ReceiverAloneDiffers()
            => Differ((a, b) => a == b, a => (int)ExpressionComparisonScale.Two.Apply(a), (a, b) => a == b, a => (int)ExpressionComparisonScale.Three.Apply(a));

        [TestMethod, TestCategory("Gated")]
        public void BinaryMethodAloneDiffers()
            => Differ(ExpressionComparisonTrees.EqualVia("SameParity"), a => (int)(a & 1), ExpressionComparisonTrees.EqualVia("Same"), a => (int)(a & 1));

        [TestMethod, TestCategory("Gated")]
        public void UnaryMethodAloneDiffers()
            => Differ((a, b) => a == b, ExpressionComparisonTrees.HashVia("Low"), (a, b) => a == b, ExpressionComparisonTrees.HashVia("Parity"));

        [TestMethod, TestCategory("Gated")]
        public void ConversionTypeAloneDiffers()
            => Differ((a, b) => (float)a == (float)b, a => 0, (a, b) => (double)a == (double)b, a => 0);

        [TestMethod, TestCategory("Gated")]
        public void NaNConstantBitsAloneDiffer()
            => Differ((a, b) => a == b, ExpressionComparisonTrees.CopySignHash(ExpressionComparisonTrees.NegativeNaN), (a, b) => a == b, ExpressionComparisonTrees.CopySignHash(ExpressionComparisonTrees.PositiveNaN));

        [TestMethod, TestCategory("Gated")]
        public void SignedZeroConstantsAloneDiffer()
            => Differ((a, b) => a == b, a => (int)Math.CopySign(a, 0.0), (a, b) => a == b, a => (int)Math.CopySign(a, -0.0));

        [TestMethod, TestCategory("Gated")]
        public void CapturedNaNTagsDiffer()
            => Differ((a, b) => a == b, ExpressionComparisonTrees.TagHash(BitConverter.Int64BitsToDouble(0x7FF8000000000001)), (a, b) => a == b, ExpressionComparisonTrees.TagHash(BitConverter.Int64BitsToDouble(0x7FF8000000000002)));

        [TestMethod, TestCategory("Gated")]
        public void FloatConstantBitsAloneDiffer()
        {
            Expression<Func<long, int>> plus = a => (int)MathF.CopySign(1f, 0f);
            Expression<Func<long, int>> minus = a => (int)MathF.CopySign(1f, -0f);
            Assert.IsFalse(EqualityComparer.IsEqual(plus, minus));
        }

        [TestMethod, TestCategory("Gated")]
        public void SameNaNBitsAreTheSame()
            => Assert.IsFalse(ComparerDifference.MayDiffer(
                Comparer((a, b) => a == b, ExpressionComparisonTrees.CopySignHash(ExpressionComparisonTrees.PositiveNaN)),
                Comparer((a, b) => a == b, ExpressionComparisonTrees.CopySignHash(ExpressionComparisonTrees.PositiveNaN))));

        // A receiver whose inner lambda declares the outer lambda's parameter again: two copies, each with its own
        // parameter, are the same. The outer parameter is used again after the inner lambda.
        [TestMethod, TestCategory("Gated")]
        public void ShadowedParametersAreTheSame()
            => Assert.IsTrue(EqualityComparer.IsEqual(
                ExpressionComparisonTrees.ShadowReceiver(Expression.Parameter(typeof(long), "p")),
                ExpressionComparisonTrees.ShadowReceiver(Expression.Parameter(typeof(long), "q"))));

        // Parameters that no lambda or block declares cannot be compared: they count as different, without an exception.
        [TestMethod, TestCategory("Gated")]
        public void FreeParametersCountAsDifferent()
            => Assert.IsFalse(EqualityComparer.IsEqual(Expression.Negate(Expression.Parameter(typeof(long), "p")), Expression.Negate(Expression.Parameter(typeof(long), "q"))));

        [TestMethod, TestCategory("Gated")]
        public void TwoCopiesFromOneFactoryAreTheSame()
            => Assert.IsFalse(ComparerDifference.MayDiffer(ExpressionComparisonTrees.BitsComparer(), ExpressionComparisonTrees.BitsComparer()));

        // A coalesce with different conversion lambdas, and lifted comparisons that differ in IsLiftedToNull (and so in
        // their type, bool? against bool).
        [TestMethod, TestCategory("Gated")]
        public void CoalesceConversionAloneDiffers()
        {
            var a = Expression.Parameter(typeof(long?), "a");
            var b = Expression.Parameter(typeof(long), "b");
            var x = Expression.Parameter(typeof(long), "x");
            var plusOne = Expression.Lambda<Func<long, long>>(Expression.Add(x, Expression.Constant(1L)), x);
            var timesTwo = Expression.Lambda<Func<long, long>>(Expression.Multiply(x, Expression.Constant(2L)), x);
            var first = Expression.Lambda<Func<long?, long, long>>(Expression.Coalesce(a, b, plusOne), a, b);
            var second = Expression.Lambda<Func<long?, long, long>>(Expression.Coalesce(a, b, timesTwo), a, b);
            Assert.IsFalse(EqualityComparer.IsEqual(first, second));
        }

        // The same difference as LiftToNullAloneDiffers, under a conversion to bool? that gives both trees the same
        // types above the comparison.
        [TestMethod, TestCategory("Gated")]
        public void LiftToNullUnderAConversionAloneDiffers()
        {
            var first = ExpressionComparisonTrees.LiftedHash(false);
            var second = ExpressionComparisonTrees.LiftedHash(true);
            Assert.AreEqual(0, first.Compile()(null));
            Assert.AreEqual(1, second.Compile()(null));
            Assert.IsFalse(EqualityComparer.IsEqual(first, second));
            Assert.IsTrue(ComparerDifference.MayDiffer(
                new EqualityComparerExpression<long?>((a, b) => a == b, first), new EqualityComparerExpression<long?>((a, b) => a == b, second)));
        }

        // Constants whose Equals ignores something code can read: a decimal's scale, a DateTime's kind and a
        // DateTimeOffset's offset.
        [TestMethod, TestCategory("Gated")]
        public void DecimalScaleAloneDiffers()
        {
            Expression<Func<long, int>> one = a => (1.0m).ToString().Length;
            Expression<Func<long, int>> other = a => (1.00m).ToString().Length;
            Assert.AreNotEqual(one.Compile()(0), other.Compile()(0));
            Assert.IsFalse(EqualityComparer.IsEqual(one, other));
        }

        [TestMethod, TestCategory("Gated")]
        public void DateTimeKindAloneDiffers()
        {
            var a = Expression.Parameter(typeof(long), "a");
            Expression<Func<long, int>> Hash(DateTimeKind kind)
                => Expression.Lambda<Func<long, int>>(Expression.Convert(Expression.Property(Expression.Constant(new DateTime(2026, 1, 1, 0, 0, 0, kind)), "Kind"), typeof(int)), a);
            Assert.IsFalse(EqualityComparer.IsEqual(Hash(DateTimeKind.Utc), Hash(DateTimeKind.Local)));
        }

        [TestMethod, TestCategory("Gated")]
        public void DateTimeOffsetOffsetAloneDiffers()
        {
            var a = Expression.Parameter(typeof(long), "a");
            var instant = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
            Expression<Func<long, int>> Hash(DateTimeOffset value)
                => Expression.Lambda<Func<long, int>>(Expression.Property(Expression.Constant(value), "Hour"), a);
            Assert.IsFalse(EqualityComparer.IsEqual(Hash(instant), Hash(instant.ToOffset(TimeSpan.FromHours(2)))));
        }

        // The claimed case for coalesce conversions: a non-null key 0 hashes to 0 and to 1.
        [TestMethod, TestCategory("Gated")]
        public void CoalesceConversionUnderAHashAloneDiffers()
        {
            Assert.AreEqual(0, ExpressionComparisonTrees.CoalesceHash(0).Compile()(0));
            Assert.AreEqual(1, ExpressionComparisonTrees.CoalesceHash(1).Compile()(0));
            Assert.IsFalse(EqualityComparer.IsEqual(ExpressionComparisonTrees.CoalesceHash(0), ExpressionComparisonTrees.CoalesceHash(1)));
        }

        // Every public property of BinaryExpression and UnaryExpression is compared, or is decided by what is compared.
        // A property added by a later .NET fails this test until it is classified.
        [TestMethod, TestCategory("Gated")]
        public void BinaryAndUnaryPropertiesAreAllAccountedFor()
        {
            // Compared: Left, Right, Method, Conversion (binary); Operand, Method (unary); Type and NodeType (every node).
            // Decided by those: IsLifted and IsLiftedToNull (by the node type, the operand types, the method and Type),
            // and CanReduce (by the node type).
            var accounted = new[] { "CanReduce", "Conversion", "IsLifted", "IsLiftedToNull", "Left", "Method", "NodeType", "Operand", "Right", "Type" };
            foreach (var type in new[] { typeof(BinaryExpression), typeof(UnaryExpression) })
            {
                var unknown = type.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                    .Select(p => p.Name).Where(n => !accounted.Contains(n)).ToArray();
                Assert.AreEqual(0, unknown.Length, type.Name + " has properties the comparison does not account for: " + string.Join(", ", unknown));
            }
        }

        // One set of parameter objects declared in another order. Separately built bodies: (x, y, z) => x + 0 against
        // (y, z, x) => z + 0, and (x, y) => x + 0 against (y, x) => x + 0. Each pair computes different functions.
        [TestMethod, TestCategory("Gated")]
        public void SharedParametersDeclaredInAnotherOrderDiffer()
        {
            ExpressionComparisonTrees.CyclicHashes(out var first, out var second);
            Assert.AreEqual(1, first.Compile()(1));
            Assert.AreEqual(0, second.Compile()(1));
            Assert.IsFalse(EqualityComparer.IsEqual(first, second));
            var x = Expression.Parameter(typeof(long), "x");
            var y = Expression.Parameter(typeof(long), "y");
            Assert.IsFalse(EqualityComparer.IsEqual(
                Expression.Lambda<Func<long, long, long>>(Expression.Add(x, Expression.Constant(0L)), x, y),
                Expression.Lambda<Func<long, long, long>>(Expression.Add(x, Expression.Constant(0L)), y, x)));
        }

        // (x, y) => x against (y, x) => y compute the same function, but one parameter object declared in different
        // places counts as different: the comparison does not try to match such trees.
        [TestMethod, TestCategory("Gated")]
        public void SharedParametersDeclaredInAnotherOrderCountAsDifferentEvenIfEquivalent()
        {
            var x = Expression.Parameter(typeof(long), "x");
            var y = Expression.Parameter(typeof(long), "y");
            Assert.IsFalse(EqualityComparer.IsEqual(Expression.Lambda<Func<long, long, long>>(x, x, y), Expression.Lambda<Func<long, long, long>>(y, y, x)));
        }

        // x => y => body against y => x => body, with one body object x + 0: the first returns its outer argument and
        // the second its inner one. The shared body must not count as the same.
        [TestMethod, TestCategory("Gated")]
        public void SharedSubtreeUnderOtherBindingsDiffers()
        {
            var x = Expression.Parameter(typeof(long), "x");
            var y = Expression.Parameter(typeof(long), "y");
            var body = Expression.Add(x, Expression.Constant(0L));
            var first = Expression.Lambda<Func<long, Func<long, long>>>(Expression.Lambda<Func<long, long>>(body, y), x);
            var second = Expression.Lambda<Func<long, Func<long, long>>>(Expression.Lambda<Func<long, long>>(body, x), y);
            Assert.AreEqual(1, first.Compile()(1)(2));
            Assert.AreEqual(2, second.Compile()(1)(2));
            Assert.IsFalse(EqualityComparer.IsEqual(first, second));
        }

        // Index nodes that differ only in the indexer they call: explicit interface indexers, and a hiding indexer.
        [TestMethod, TestCategory("Gated")]
        public void ExplicitInterfaceIndexersAloneDiffer()
        {
            var c = Expression.Parameter(typeof(ExpressionComparisonTwoIndexers), "c");
            Expression<Func<ExpressionComparisonTwoIndexers, int>> Via(Type face)
                => Expression.Lambda<Func<ExpressionComparisonTwoIndexers, int>>(Expression.MakeIndex(c, face.GetProperty("Item"), new[] { Expression.Constant(0) }), c);
            var first = Via(typeof(IExpressionComparisonFirstIndex));
            var second = Via(typeof(IExpressionComparisonSecondIndex));
            Assert.AreEqual(1, first.Compile()(new ExpressionComparisonTwoIndexers()));
            Assert.AreEqual(2, second.Compile()(new ExpressionComparisonTwoIndexers()));
            Assert.IsFalse(EqualityComparer.IsEqual(first, second));
        }

        [TestMethod, TestCategory("Gated")]
        public void HidingIndexerAloneDiffers()
        {
            var d = Expression.Parameter(typeof(ExpressionComparisonHidingIndexer), "d");
            Expression<Func<ExpressionComparisonHidingIndexer, int>> Via(Type declaring)
                => Expression.Lambda<Func<ExpressionComparisonHidingIndexer, int>>(
                    Expression.MakeIndex(d, declaring.GetProperty("Item", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.DeclaredOnly), new[] { Expression.Constant(0) }), d);
            var first = Via(typeof(ExpressionComparisonBaseIndexer));
            var second = Via(typeof(ExpressionComparisonHidingIndexer));
            Assert.AreEqual(1, first.Compile()(new ExpressionComparisonHidingIndexer()));
            Assert.AreEqual(2, second.Compile()(new ExpressionComparisonHidingIndexer()));
            Assert.IsFalse(EqualityComparer.IsEqual(first, second));
        }

        // Two labels with the same name, and one goto object that jumps to the first: the blocks give 0 and 1.
        [TestMethod, TestCategory("Gated")]
        public void LabelsWithTheSameNameDiffer()
        {
            var a = Expression.Parameter(typeof(long), "a");
            var targetA = Expression.Label(typeof(int), "t");
            var targetB = Expression.Label(typeof(int), "t");
            var go = Expression.Goto(targetA, Expression.Constant(1));
            var first = Expression.Lambda<Func<long, int>>(Expression.Block(go, Expression.Label(targetA, Expression.Constant(0)), Expression.Label(targetB, Expression.Constant(0))), a);
            var second = Expression.Lambda<Func<long, int>>(Expression.Block(go, Expression.Label(targetB, Expression.Constant(0)), Expression.Label(targetA, Expression.Constant(0))), a);
            Assert.AreEqual(0, first.Compile()(0));
            Assert.AreEqual(1, second.Compile()(0));
            Assert.IsFalse(EqualityComparer.IsEqual(first, second));
        }

        [TestMethod, TestCategory("Gated")]
        public void LiftToNullAloneDiffers()
        {
            var a = Expression.Parameter(typeof(long?), "a");
            var b = Expression.Parameter(typeof(long?), "b");
            var first = Expression.Lambda(Expression.Equal(a, b, true, null), a, b);
            var second = Expression.Lambda(Expression.Equal(a, b, false, null), a, b);
            Assert.IsFalse(EqualityComparer.IsEqual(first, second));
        }
    }
}
