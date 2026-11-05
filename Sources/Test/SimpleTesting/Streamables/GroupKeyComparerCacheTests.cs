using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Sharding;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct GroupKeyComparerEvent
    {
        public int Key;
    }

    // Two grouping queries in one process with the same key selector text, one over a stream with an equality
    // comparer for that selector and one without. The comparer (a % 2 == b % 2, hash a % 2) agrees with default
    // equality on keys 1 and 2, so each query must return the same events as it does alone. Each test uses key
    // selector texts of its own, so that generated code cached by an earlier test cannot affect it.
    public abstract class GroupKeyComparerCacheTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const string Counts = "1:11, 1:11, 2:21, 2:21, 4:12, 4:12, 11:11, 11:11";
        private const string Keys = "1:1, 1:1, 2:2, 2:2, 4:1, 4:1, 11:1, 11:1";

        private static readonly long[] Times = { 1, 2, 4 };

        private readonly bool strict;

        internal GroupKeyComparerCacheTestsBase(bool columnar, bool strict)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(strict))
            => this.strict = strict;

        // Keys 1, 2, 1 at times 1, 2 and 4, each lasting 10 ticks.
        private static IStreamable<Empty, GroupKeyComparerEvent> Input()
            => new[] { 1, 2, 1 }.Select((k, i) => StreamEvent.CreateInterval(Times[i], Times[i] + 10, new GroupKeyComparerEvent { Key = k }))
                .ToObservable().ToStreamable();

        // Each data event as time:value, ordered by time and then value.
        private static string Events<T>(IStreamable<Empty, T> query, Func<T, int> value)
            => string.Join(", ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData)
                .OrderBy(e => e.StartTime).ThenBy(e => value(e.Payload)).Select(e => e.StartTime + ":" + value(e.Payload)));

        [TestMethod, TestCategory("Gated")]
        public void GroupApplyWithAComparerThenWithout()
        {
            var withComparer = Input().SetProperty().EqualityComparer(e => e.Key, (a, b) => a % 2 == b % 2, a => a % 2)
                .GroupApply(e => e.Key, g => g.Count(), (g, c) => g.Key * 10 + (int)c);
            Assert.AreEqual(Counts, Events(withComparer, x => x));

            var without = Input().GroupApply(e => e.Key, g => g.Count(), (g, c) => g.Key * 10 + (int)c);
            Assert.AreEqual(Counts, Events(without, x => x));
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupApplyWithoutAComparerThenWith()
        {
            var without = Input().GroupApply(e => e.Key + 0, g => g.Count(), (g, c) => g.Key * 10 + (int)c);
            Assert.AreEqual(Counts, Events(without, x => x));

            var withComparer = Input().SetProperty().EqualityComparer(e => e.Key + 0, (a, b) => a % 2 == b % 2, a => a % 2)
                .GroupApply(e => e.Key + 0, g => g.Count(), (g, c) => g.Key * 10 + (int)c);
            Assert.AreEqual(Counts, Events(withComparer, x => x));
        }

        // The second query's key selector has different text, so the two never shared generated code.
        [TestMethod, TestCategory("Gated")]
        public void GroupApplyWithADifferentKeySelector()
        {
            var withComparer = Input().SetProperty().EqualityComparer(e => e.Key - 0, (a, b) => a % 2 == b % 2, a => a % 2)
                .GroupApply(e => e.Key - 0, g => g.Count(), (g, c) => g.Key * 10 + (int)c);
            Assert.AreEqual(Counts, Events(withComparer, x => x));

            var without = Input().GroupApply(e => e.Key - 0 + 0, g => g.Count(), (g, c) => g.Key * 10 + (int)c);
            Assert.AreEqual(Counts, Events(without, x => x));
        }

        [TestMethod, TestCategory("Gated")]
        public void DistinctInsideGroupApplyWithAComparerThenWithout()
        {
            var withComparer = Input().SetProperty().EqualityComparer(e => e.Key * 1, (a, b) => a % 2 == b % 2, a => a % 2)
                .GroupApply(e => e.Key * 1, g => g.Distinct(), (g, p) => p);
            Assert.AreEqual(Keys, Events(withComparer, p => p.Key));

            var without = Input().GroupApply(e => e.Key * 1, g => g.Distinct(), (g, p) => p);
            Assert.AreEqual(Keys, Events(without, p => p.Key));
        }

        // A sharded ReKey groups with GroupStreamable, which caches its generated code in the same way.
        [TestMethod, TestCategory("Gated")]
        public void ReKeyWithAComparerThenWithout()
        {
            var withComparer = Input().SetProperty().EqualityComparer(e => e.Key / 1, (a, b) => a % 2 == b % 2, a => a % 2)
                .Shard(1).ReKey(e => e.Key / 1).ReDistribute().Query(s => s.Count()).SelectKey((g, c) => g * 10 + (int)c).Unshuffle().Unshard();
            Assert.AreEqual(Counts, Events(withComparer, x => x));

            var without = Input().Shard(1).ReKey(e => e.Key / 1).ReDistribute().Query(s => s.Count()).SelectKey((g, c) => g * 10 + (int)c).Unshuffle().Unshard();
            Assert.AreEqual(Counts, Events(without, x => x));
        }

        [TestMethod, TestCategory("Gated")]
        public void ReKeyWithoutAComparerThenWith()
        {
            var without = Input().Shard(1).ReKey(e => e.Key | 0).ReDistribute().Query(s => s.Count()).SelectKey((g, c) => g * 10 + (int)c).Unshuffle().Unshard();
            Assert.AreEqual(Counts, Events(without, x => x));

            var withComparer = Input().SetProperty().EqualityComparer(e => e.Key | 0, (a, b) => a % 2 == b % 2, a => a % 2)
                .Shard(1).ReKey(e => e.Key | 0).ReDistribute().Query(s => s.Count()).SelectKey((g, c) => g * 10 + (int)c).Unshuffle().Unshard();
            Assert.AreEqual(Counts, Events(withComparer, x => x));
        }

        // A hash that compiles but cannot be written as C# source (a block with a return). Code generation for the
        // grouping fails, so the query runs in row mode, or throws when fallback is disabled.
        [TestMethod, TestCategory("Gated")]
        public void GroupApplyWithAHashCodeGenerationCannotWrite()
            => CannotWrite(() => Events(
                Input().SetProperty().EqualityComparer(e => e.Key + 3 - 3, (x, y) => x == y, UnwritableHash())
                    .GroupApply(e => e.Key + 3 - 3, g => g.Count(), (g, c) => g.Key * 10 + (int)c),
                x => x));

        // The same after a query whose code generation succeeded with the same key selector text: that cached
        // success must not stand for the second query's grouping.
        [TestMethod, TestCategory("Gated")]
        public void GroupApplyThenAHashCodeGenerationCannotWrite()
        {
            Assert.AreEqual(Counts, Events(Input().GroupApply(e => e.Key + 4 - 4, g => g.Count(), (g, c) => g.Key * 10 + (int)c), x => x));
            CannotWrite(() => Events(
                Input().SetProperty().EqualityComparer(e => e.Key + 4 - 4, (x, y) => x == y, UnwritableHash())
                    .GroupApply(e => e.Key + 4 - 4, g => g.Count(), (g, c) => g.Key * 10 + (int)c),
                x => x));
        }

        [TestMethod, TestCategory("Gated")]
        public void ReKeyThenAHashCodeGenerationCannotWrite()
        {
            Assert.AreEqual(Counts, Events(Input().Shard(1).ReKey(e => e.Key + 5 - 5).ReDistribute().Query(s => s.Count()).SelectKey((g, c) => g * 10 + (int)c).Unshuffle().Unshard(), x => x));
            CannotWrite(() => Events(
                Input().SetProperty().EqualityComparer(e => e.Key + 5 - 5, (x, y) => x == y, UnwritableHash())
                    .Shard(1).ReKey(e => e.Key + 5 - 5).ReDistribute().Query(s => s.Count()).SelectKey((g, c) => g * 10 + (int)c).Unshuffle().Unshard(),
                x => x));
        }

        private static Expression<Func<int, int>> UnwritableHash()
        {
            var a = Expression.Parameter(typeof(int), "a");
            var ret = Expression.Label(typeof(int), "ret");
            return Expression.Lambda<Func<int, int>>(Expression.Block(Expression.Return(ret, a), Expression.Label(ret, Expression.Constant(0))), a);
        }

        private void CannotWrite(Func<string> run)
        {
            if (!this.strict)
            {
                Assert.AreEqual(Counts, run());
                return;
            }

            var e = Assert.ThrowsException<InvalidOperationException>(run);
            StringAssert.Contains(e.Message, "Code Generation failed");
        }
    }

    [TestClass]
    public class GroupKeyComparerCacheTestsRow : GroupKeyComparerCacheTestsBase
    {
        public GroupKeyComparerCacheTestsRow() : base(false, false) { }
    }

    [TestClass]
    public class GroupKeyComparerCacheTestsColumnar : GroupKeyComparerCacheTestsBase
    {
        public GroupKeyComparerCacheTestsColumnar() : base(true, false) { }
    }

    [TestClass]
    public class GroupKeyComparerCacheTestsColumnarStrict : GroupKeyComparerCacheTestsBase
    {
        public GroupKeyComparerCacheTestsColumnarStrict() : base(true, true) { }
    }
}
