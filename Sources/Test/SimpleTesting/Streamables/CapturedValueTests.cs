using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct CapturedInput
    {
        public int Key;
    }

    public abstract class CapturedValueTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // Keys 1, 2 and 1 fall in the window that ends at 5; keys 2 and 1 in the one that ends at 10.
        private static readonly int[] Keys = { 1, 2, 1, 2, 1 };
        private static readonly long[] Times = { 1, 2, 4, 7, 9 };

        internal CapturedValueTestsBase(ConfigModifier modifier) : base(modifier) { }

        // The key selector reads a local, so the expression reads a field of a compiler-generated closure class.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInGroupKey()
        {
            int mult = 10;
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * mult, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });

            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        // The same key selector in a two-input map and reduce, which builds a Map2ReduceStreamable.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInTwoInputReduce()
        {
            int mult = 10;
            var result = Input().Map(s => s, e => e.Key * mult).Reduce(
                Input().Map(s => s, e => e.Key * mult),
                (l, r) => l.Union(r).TumblingWindowLifetime(5).Count(),
                (g, c) => new KeyCount { Key = g.Key, Count = c });

            CollectionAssert.AreEquivalent(new[] { "5:10=4", "5:20=2", "10:10=2", "10:20=2" }, Collect(result));
        }

        // MapReduceStreamable and Map2ReduceStreamable each have a second class for streams without an outer
        // key. No public operator builds it, so these tests construct it directly.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInEmptyKeyMapReduce()
        {
            int mult = 10;
            var result = new MapReduceStreamable<CapturedInput, CapturedInput, int, CapturedInput, ulong, KeyCount>(
                Input(),
                null,
                (l, r) => l,
                e => e.Key * mult,
                s => s.TumblingWindowLifetime(5).Count(),
                (k, c) => new KeyCount { Key = k, Count = c });

            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInEmptyKeyTwoInputReduce()
        {
            int mult = 10;
            var result = new Map2ReduceStreamable<CapturedInput, CapturedInput, CapturedInput, CapturedInput, int, CapturedInput, CapturedInput, ulong, KeyCount>(
                Input(),
                null,
                (l, r) => l,
                e => e.Key * mult,
                Input(),
                null,
                (l, r) => l,
                e => e.Key * mult,
                (l, r) => l.Union(r).TumblingWindowLifetime(5).Count(),
                (k, c) => new KeyCount { Key = k, Count = c },
                false,
                false,
                OperationalHint.None);

            CollectionAssert.AreEquivalent(new[] { "5:10=4", "5:20=2", "10:10=2", "10:20=2" }, Collect(result));
        }

        // Without a captured value every operator can run columnar, so the query must stay columnar.
        [TestMethod, TestCategory("Gated")]
        public void ConstantKeyStaysColumnar()
        {
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * 10, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });

            Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);
            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ConstantKeyTwoInputReduceStaysColumnar()
        {
            var result = Input().Map(s => s, e => e.Key * 10).Reduce(
                Input().Map(s => s, e => e.Key * 10),
                (l, r) => l.Union(r).TumblingWindowLifetime(5).Count(),
                (g, c) => new KeyCount { Key = g.Key, Count = c });

            Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);
            CollectionAssert.AreEquivalent(new[] { "5:10=4", "5:20=2", "10:10=2", "10:20=2" }, Collect(result));
        }

        private static IStreamable<Empty, CapturedInput> Input()
            => Enumerable.Range(0, Keys.Length)
                .Select(i => StreamEvent.CreatePoint(Times[i], new CapturedInput { Key = Keys[i] }))
                .ToObservable()
                .ToStreamable();

        private static List<string> Collect(IStreamable<Empty, KeyCount> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.Payload.Key + "=" + e.Payload.Count)
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class CapturedValueTestsRow : CapturedValueTestsBase
    {
        public CapturedValueTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // The default settings, where a failed code generation falls back to row mode.
    [TestClass]
    public class CapturedValueTestsColumnarFallback : CapturedValueTestsBase
    {
        public CapturedValueTestsColumnarFallback()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }
}
