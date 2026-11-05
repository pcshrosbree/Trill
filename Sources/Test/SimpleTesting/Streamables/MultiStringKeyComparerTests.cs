using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Sharding;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct MultiStringKeyComparerEvent
    {
        public string Key;
    }

    // String keys with a key comparer (equal lengths are equal keys, hash = length) while columnar batches store
    // strings as MultiString columns. Generated grouping and shuffling code must hash such keys with the comparer,
    // not with the MultiString hash vector, which computes the default string hash.
    public abstract class MultiStringKeyComparerTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal MultiStringKeyComparerTestsBase(bool columnar, bool strict)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(strict).UseMultiString(true)) { }

        // One event per key, all from time 1 to time 11.
        private static IStreamable<Empty, MultiStringKeyComparerEvent> Input(params string[] keys)
            => keys.Select(k => StreamEvent.CreateInterval(1, 11, new MultiStringKeyComparerEvent { Key = k })).ToObservable().ToStreamable();

        private static IStreamable<Empty, MultiStringKeyComparerEvent> WithComparer(IStreamable<Empty, MultiStringKeyComparerEvent> input)
            => input.SetProperty().EqualityComparer(e => e.Key, (a, b) => a.Length == b.Length, a => a.Length);

        // Both inputs hold the key "a", so the comparer on the left agrees with default equality on every key.
        [TestMethod, TestCategory("Gated")]
        public void ReKeyThenWhereNotExistsWithAComparerOnTheLeft()
        {
            var left = WithComparer(Input("a")).Shard(1).ReKey(e => e.Key);
            var right = Input("a").Shard(1).ReKey(e => e.Key);
            var result = left.Query(right, (l, r) => l.WhereNotExists(r)).Unshuffle().Unshard()
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.Key);
            Assert.AreEqual(string.Empty, string.Join(", ", result));
        }

        // "a" and "b" have the same length, so they are equal under the comparer on both inputs.
        [TestMethod, TestCategory("Gated")]
        public void ReKeyThenJoinWithTheComparerOnBothSides()
        {
            var left = WithComparer(Input("a")).Shard(1).ReKey(e => e.Key);
            var right = WithComparer(Input("b")).Shard(1).ReKey(e => e.Key);
            var result = left.Query(right, (l, r) => l.Join(r, (x, y) => x.Key + y.Key)).Unshuffle().Unshard()
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload);
            Assert.AreEqual("ab", string.Join(", ", result));
        }

        // All four keys have length 1, so they form one group, and every count is 4.
        [TestMethod, TestCategory("Gated")]
        public void GroupApplyWithAComparer()
        {
            var result = WithComparer(Input("a", "b", "c", "d")).GroupApply(e => e.Key, s => s.Count(), (g, c) => c)
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload).Distinct();
            Assert.AreEqual("4", string.Join(", ", result));
        }
    }

    [TestClass]
    public class MultiStringKeyComparerTestsRow : MultiStringKeyComparerTestsBase
    {
        public MultiStringKeyComparerTestsRow() : base(false, false) { }
    }

    [TestClass]
    public class MultiStringKeyComparerTestsColumnar : MultiStringKeyComparerTestsBase
    {
        public MultiStringKeyComparerTestsColumnar() : base(true, false) { }
    }

    [TestClass]
    public class MultiStringKeyComparerTestsColumnarStrict : MultiStringKeyComparerTestsBase
    {
        public MultiStringKeyComparerTestsColumnarStrict() : base(true, true) { }
    }
}
