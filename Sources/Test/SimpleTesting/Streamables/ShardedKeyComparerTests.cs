using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Sharding;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct ShardedKeyComparerEvent
    {
        public string Key;
    }

    // A comparer's hash can call an instance method of an object it captures.
    public sealed class ShardedKeyComparerModulo
    {
        private readonly int modulus;

        public ShardedKeyComparerModulo(int modulus) => this.modulus = modulus;

        public int Hash(string s) => s.Length % this.modulus;
    }

    // The binary Query of a sharded streamable throws ShardedKeyComparerMismatchException when its inputs were
    // redistributed with different comparers, because equal keys can then be on different shards.
    // The comparer compares string lengths (hash: the length). Each input holds key "a" from time 1 to 11 unless a
    // test says otherwise, so a WhereNotExists of one input against the other returns no events. Generated grouping
    // code can be cached by key selector text, so each test uses key selector texts of its own.
    public abstract class ShardedKeyComparerTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal ShardedKeyComparerTestsBase(bool columnar, bool strict)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(strict)) { }

        private static IStreamable<Empty, ShardedKeyComparerEvent> Input(string key = "a")
            => new[] { StreamEvent.CreateInterval(1, 11, new ShardedKeyComparerEvent { Key = key }) }.ToObservable().ToStreamable();

        private static IStreamable<Empty, ShardedKeyComparerEvent> WithComparer(
            IStreamable<Empty, ShardedKeyComparerEvent> input, Expression<Func<ShardedKeyComparerEvent, string>> keySelector)
            => input.SetProperty().EqualityComparer(keySelector, (a, b) => a.Length == b.Length, a => a.Length);

        // Groups the input by the key selector and redistributes it to the given number of shards. The Select gives
        // the grouped stream a new payload before it is redistributed.
        private static IShardedStreamable<string, ShardedKeyComparerEvent> Redistributed(
            IStreamable<Empty, ShardedKeyComparerEvent> input, Expression<Func<ShardedKeyComparerEvent, string>> keySelector, int shards = 2)
            => input.Shard(1).ReKey(keySelector).Query(s => s.Select(e => new ShardedKeyComparerEvent { Key = e.Key }))
                .ReDistribute(new LocalLocationDescriptor(shards));

        private static string Keys(IShardedStreamable<string, ShardedKeyComparerEvent> result)
            => string.Join(", ", result.Unshuffle().Unshard().ToStreamEventObservable().ToEnumerable()
                .Where(e => e.IsData).Select(e => e.Payload.Key).OrderBy(k => k, StringComparer.Ordinal));

        // The case from fork #131's review: the comparer is on the left input only.
        [TestMethod, TestCategory("Gated")]
        public void ComparerOnTheLeftOnlyThrows()
        {
            var left = Redistributed(WithComparer(Input(), e => e.Key + "L"), e => e.Key + "L");
            var right = Redistributed(Input(), e => e.Key + "L");
            var e = Assert.ThrowsException<ShardedKeyComparerMismatchException>(() => left.Query(right, (l, r) => l.WhereNotExists(r)));
            StringAssert.Contains(e.Message, "binary Query of a sharded streamable");
            StringAssert.Contains(e.Message, "a => a.Length");
            StringAssert.Contains(e.Message, "ReDistribute");
        }

        [TestMethod, TestCategory("Gated")]
        public void ComparerOnTheRightOnlyThrows()
        {
            var left = Redistributed(Input(), e => e.Key + "R");
            var right = Redistributed(WithComparer(Input(), e => e.Key + "R"), e => e.Key + "R");
            Assert.ThrowsException<ShardedKeyComparerMismatchException>(() => left.Query(right, (l, r) => l.WhereNotExists(r)));
        }

        [TestMethod, TestCategory("Gated")]
        public void SameComparerOnBothInputsRuns()
        {
            var left = Redistributed(WithComparer(Input(), e => e.Key + "1"), e => e.Key + "1");
            var right = Redistributed(WithComparer(Input(), e => e.Key + "1"), e => e.Key + "1");
            Assert.AreEqual(string.Empty, Keys(left.Query(right, (l, r) => l.WhereNotExists(r))));
        }

        // "a" and "b" have the same length, so the comparer on both inputs matches them, and both are on one shard.
        [TestMethod, TestCategory("Gated")]
        public void SameComparerOnBothInputsJoins()
        {
            var left = Redistributed(WithComparer(Input("a"), e => e.Key + "2"), e => e.Key + "2");
            var right = Redistributed(WithComparer(Input("b"), e => e.Key + "2"), e => e.Key + "2");
            var result = left.Query(right, (l, r) => l.Join(r, (x, y) => new ShardedKeyComparerEvent { Key = x.Key + y.Key }));
            Assert.AreEqual("ab", Keys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void NoComparerRuns()
        {
            var left = Redistributed(Input(), e => e.Key + "3");
            var right = Redistributed(Input(), e => e.Key + "3");
            Assert.AreEqual(string.Empty, Keys(left.Query(right, (l, r) => l.WhereNotExists(r))));
        }

        // With one shard, every key is on it, so different comparers cannot separate equal keys. A Union does not
        // compare keys, so its result does not depend on how each shard compares them.
        [TestMethod, TestCategory("Gated")]
        public void OneShardWithDifferentComparersRuns()
        {
            var left = Redistributed(WithComparer(Input(), e => e.Key + "4"), e => e.Key + "4", 1);
            var right = Redistributed(Input("b"), e => e.Key + "4", 1);
            Assert.AreEqual("a, b", Keys(left.Query(right, (l, r) => l.Union(r))));
        }

        // The remedy in the exception message, on a source with two shards: the comparer is set before Shard, and the
        // Select after ReKey gives the stream a new payload before ReDistribute.
        [TestMethod, TestCategory("Gated")]
        public void RemedyOnTwoSourceShardsRuns()
        {
            IShardedStreamable<string, ShardedKeyComparerEvent> TwoSourceShards()
                => WithComparer(Input(), e => e.Key + "10").Shard(2).ReKey(e => e.Key + "10")
                    .Query(s => s.Select(e => new ShardedKeyComparerEvent { Key = e.Key })).ReDistribute(new LocalLocationDescriptor(2));
            Assert.AreEqual(string.Empty, Keys(TwoSourceShards().Query(TwoSourceShards(), (l, r) => l.WhereNotExists(r))));
        }

        // Neither input was redistributed: the guard knows nothing about how their shards were chosen.
        [TestMethod, TestCategory("Gated")]
        public void InputsNotRedistributedAreNotChecked()
        {
            var left = WithComparer(Input(), e => e.Key + "5").Shard(1).ReKey(e => e.Key + "5");
            var right = Input("b").Shard(1).ReKey(e => e.Key + "5");
            Assert.AreEqual("a, b", Keys(left.Query(right, (l, r) => l.Union(r))));
        }

        // Only the left input was redistributed. The right input's two shards were filled by Shard, not by key hash.
        [TestMethod, TestCategory("Gated")]
        public void OnlyTheLeftInputRedistributedIsNotChecked()
        {
            var left = Redistributed(WithComparer(Input(), e => e.Key + "8"), e => e.Key + "8");
            var right = Input("b").Shard(2).ReKey(e => e.Key + "8");
            Assert.AreEqual("a, b", Keys(left.Query(right, (l, r) => l.Union(r))));
        }

        // The right input was redistributed with the default comparer and then broadcast, so every shard holds its event
        // and the left input's length comparer cannot separate equal keys. The join compares keys with default equality.
        [TestMethod, TestCategory("Gated")]
        public void BroadcastInputIsNotChecked()
        {
            var left = Redistributed(WithComparer(Input(), e => e.Key + "9"), e => e.Key + "9");
            var right = Redistributed(Input(), e => e.Key + "9").Broadcast();
            var result = left.Query(right, (l, r) => l.Join(r, (x, y) => new ShardedKeyComparerEvent { Key = x.Key + y.Key }));
            Assert.AreEqual("aa", Keys(result));
        }

        // A unary Query after ReDistribute keeps every event on its shard, so the check still applies after it.
        [TestMethod, TestCategory("Gated")]
        public void CheckedAfterAUnaryQuery()
        {
            var left = Redistributed(WithComparer(Input(), e => e.Key + "U"), e => e.Key + "U").Query(s => s.Where(e => e.Key != null));
            var right = Redistributed(Input(), e => e.Key + "U");
            Assert.ThrowsException<ShardedKeyComparerMismatchException>(() => left.Query(right, (l, r) => l.WhereNotExists(r)));
        }

        // SelectKey keeps every event on its shard, so the check still applies after it.
        [TestMethod, TestCategory("Gated")]
        public void CheckedAfterSelectKey()
        {
            var left = Redistributed(WithComparer(Input(), e => e.Key + "S"), e => e.Key + "S").SelectKey((k, e) => new ShardedKeyComparerEvent { Key = k });
            var right = Redistributed(Input(), e => e.Key + "S");
            Assert.ThrowsException<ShardedKeyComparerMismatchException>(() => left.Query(right, (l, r) => l.WhereNotExists(r)));
        }

        // The output of a binary Query of two inputs with the same comparer is checked against a third input.
        [TestMethod, TestCategory("Gated")]
        public void CheckedAfterABinaryQuery()
        {
            var first = Redistributed(WithComparer(Input(), e => e.Key + "B"), e => e.Key + "B");
            var second = Redistributed(WithComparer(Input(), e => e.Key + "B"), e => e.Key + "B");
            var third = Redistributed(Input(), e => e.Key + "B");
            var union = first.Query(second, (l, r) => l.Union(r));
            Assert.ThrowsException<ShardedKeyComparerMismatchException>(() => union.Query(third, (l, r) => l.WhereNotExists(r)));
        }

        // The left input's two shards have different comparers, so it has no record, and the check does not apply,
        // although the right input has a record of the default comparer.
        [TestMethod, TestCategory("Gated")]
        public void InputWhoseShardsDisagreeIsNotChecked()
        {
            var shards = new[]
            {
                WithComparer(Input(), e => e.Key + "7").Shard(1).ReKey(e => e.Key + "7"),
                Input("b").Shard(1).ReKey(e => e.Key + "7"),
            };
            var left = new ShardedStreamable<string, ShardedKeyComparerEvent>(shards.Select(s => ((ShardedStreamable<string, ShardedKeyComparerEvent>)s).Streamables[0]).ToArray())
                .Query(s => s.Select(e => new ShardedKeyComparerEvent { Key = e.Key })).ReDistribute(new LocalLocationDescriptor(2));
            var right = Redistributed(Input("c"), e => e.Key + "7");
            Assert.AreEqual("a, b, c", Keys(left.Query(right, (l, r) => l.Union(r))));
        }

    }

    [TestClass]
    public class ShardedKeyComparerTestsRow : ShardedKeyComparerTestsBase
    {
        public ShardedKeyComparerTestsRow() : base(false, false) { }
    }

    [TestClass]
    public class ShardedKeyComparerTestsColumnar : ShardedKeyComparerTestsBase
    {
        public ShardedKeyComparerTestsColumnar() : base(true, false) { }
    }

    [TestClass]
    public class ShardedKeyComparerTestsColumnarStrict : ShardedKeyComparerTestsBase
    {
        public ShardedKeyComparerTestsColumnarStrict() : base(true, true) { }
    }

    // ComparerDifference decides whether two comparers might differ. It compares the Equals and the hash
    // expressions, as trees, up to parameter names and after inlining captured values.
    [TestClass]
    public class ComparerDifferenceTests
    {
        private static IEqualityComparerExpression<string> Comparer(
            Expression<Func<string, string, bool>> equals, Expression<Func<string, int>> hash)
            => new EqualityComparerExpression<string>(equals, hash);

        private static IEqualityComparerExpression<string> Modulo(int modulus)
            => Comparer((a, b) => a.Length % modulus == b.Length % modulus, a => a.Length % modulus);

        [TestMethod, TestCategory("Gated")]
        public void SameHashUnderOtherParameterNamesIsTheSame()
            => Assert.IsFalse(ComparerDifference.MayDiffer(Comparer((a, b) => a.Length == b.Length, a => a.Length), Comparer((x, y) => x.Length == y.Length, s => s.Length)));

        [TestMethod, TestCategory("Gated")]
        public void DifferentHashIsDifferent()
            => Assert.IsTrue(ComparerDifference.MayDiffer(Comparer((a, b) => a.Length == b.Length, a => a.Length), EqualityComparerExpression<string>.Default));

        // The two hashes compute the same function, but as trees they differ, so they count as different.
        [TestMethod, TestCategory("Gated")]
        public void SameFunctionWrittenDifferentlyIsDifferent()
            => Assert.IsTrue(ComparerDifference.MayDiffer(Comparer((a, b) => a.Length == b.Length, a => a.Length), Comparer((a, b) => a.Length == b.Length, a => a.Length + 0)));

        // With different Equals, a join compares keys with default equality, which the shared hash need not respect.
        [TestMethod, TestCategory("Gated")]
        public void DifferentEqualsWithTheSameHashIsDifferent()
            => Assert.IsTrue(ComparerDifference.MayDiffer(Comparer((a, b) => a.Length == b.Length, a => a.Length), Comparer((a, b) => a == b, a => a.Length)));

        [TestMethod, TestCategory("Gated")]
        public void SameCapturedValueInTwoClosuresIsTheSame()
            => Assert.IsFalse(ComparerDifference.MayDiffer(Modulo(3), Modulo(3)));

        [TestMethod, TestCategory("Gated")]
        public void DifferentCapturedValuesAreDifferent()
            => Assert.IsTrue(ComparerDifference.MayDiffer(Modulo(3), Modulo(4)));

        private static IEqualityComparerExpression<string> ViaObject(ShardedKeyComparerModulo modulo)
            => Comparer((a, b) => a.Length == b.Length, a => modulo.Hash(a));

        // Calls on different objects differ: EqualityComparer.IsEqual and ComparerDifference both compare receivers.
        [TestMethod, TestCategory("Gated")]
        public void InstanceCallsOnDifferentObjectsAreDifferent()
            => Assert.IsTrue(ComparerDifference.MayDiffer(ViaObject(new ShardedKeyComparerModulo(3)), ViaObject(new ShardedKeyComparerModulo(4))));

        // Both lambdas read the same local, so their receivers are the same field of the same closure.
        [TestMethod, TestCategory("Gated")]
        public void InstanceCallsOnTheSameObjectAreTheSame()
        {
            var modulo = new ShardedKeyComparerModulo(3);
            var first = Comparer((a, b) => a.Length == b.Length, a => modulo.Hash(a));
            var second = Comparer((a, b) => a.Length == b.Length, a => modulo.Hash(a));
            Assert.IsFalse(ComparerDifference.MayDiffer(first, second));
        }

        private static IEqualityComparerExpression<string> ViaConvertAll()
            => Comparer((a, b) => a.Length == b.Length, a => new System.Collections.Generic.List<int> { 1 }.ConvertAll(i => i).IndexOf(a.Length));

        // The receiver of IndexOf is a list initializer followed by a call that declares the lambda i => i. Two comparers
        // written the same way are the same.
        [TestMethod, TestCategory("Gated")]
        public void ReceiverThatDeclaresALambdaIsTheSame()
            => Assert.IsFalse(ComparerDifference.MayDiffer(ViaConvertAll(), ViaConvertAll()));

        // A hash with a block-local variable: each copy declares its own variable in the same place, so the two are the same.
        private static IEqualityComparerExpression<string> WithBlockLocal()
        {
            var key = Expression.Parameter(typeof(string), "key");
            var local = Expression.Variable(typeof(int), "local");
            var hash = Expression.Lambda<Func<string, int>>(
                Expression.Block(new[] { local }, Expression.Assign(local, Expression.Property(key, "Length")), local), key);
            return Comparer((a, b) => a.Length == b.Length, hash);
        }

        [TestMethod, TestCategory("Gated")]
        public void BlockLocalVariablesInTheSamePlaceAreTheSame()
            => Assert.IsFalse(ComparerDifference.MayDiffer(WithBlockLocal(), WithBlockLocal()));

        // Two loops that share their body, a break with no continue label: the missing labels match.
        private static IEqualityComparerExpression<string> WithLoop(LabelTarget done, Expression body)
        {
            var key = Expression.Parameter(typeof(string), "key");
            var hash = Expression.Lambda<Func<string, int>>(Expression.Block(Expression.Loop(body, done), Expression.Property(key, "Length")), key);
            return Comparer((a, b) => a.Length == b.Length, hash);
        }

        [TestMethod, TestCategory("Gated")]
        public void LoopsWithoutAContinueLabelAreTheSame()
        {
            var done = Expression.Label("done");
            var body = Expression.Break(done);
            Assert.IsFalse(ComparerDifference.MayDiffer(WithLoop(done, body), WithLoop(done, body)));
        }

        [TestMethod, TestCategory("Gated")]
        public void InstanceCallsOnTheKeyAreTheSame()
            => Assert.IsFalse(ComparerDifference.MayDiffer(Comparer((a, b) => a.Equals(b), a => a.GetHashCode()), Comparer((x, y) => x.Equals(y), s => s.GetHashCode())));

        [TestMethod, TestCategory("Gated")]
        public void SharedReturnsTheFirstWhenAllHashesAreTheSame()
        {
            var first = Comparer((a, b) => a.Length == b.Length, a => a.Length);
            Assert.AreSame(first, ComparerDifference.Shared(new[] { first, Comparer((a, b) => a.Length == b.Length, a => a.Length), Comparer((a, b) => a.Length == b.Length, a => a.Length) }));
        }

        [TestMethod, TestCategory("Gated")]
        public void SharedReturnsNullWhenAnyHashDiffers()
            => Assert.IsNull(ComparerDifference.Shared(new[] { Modulo(3), Modulo(3), Modulo(4) }));

        [TestMethod, TestCategory("Gated")]
        public void SharedReturnsNullForNoComparers()
            => Assert.IsNull(ComparerDifference.Shared(new IEqualityComparerExpression<string>[0]));
    }
}
