using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // Members that generated code cannot reach without an access check waiver for this assembly.
    internal static class NonPublicKnobs
    {
        private static readonly int PrivateMult = 10;

        // An internal property: generated code calls its getter, an internal method.
        internal static int Mult => 10;

        internal static bool IsOdd(int key) => key % 2 == 1;

        // Built here because only this class can name the private field.
        internal static readonly Expression<Func<CapturedInput, int>> PrivateFieldKey = e => e.Key * PrivateMult;
    }

    public abstract class NonPublicMemberTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // Keys 1, 2 and 1 fall in the window that ends at 5; keys 2 and 1 in the one that ends at 10.
        private static readonly int[] Keys = { 1, 2, 1, 2, 1 };
        private static readonly long[] Times = { 1, 2, 4, 7, 9 };

        internal NonPublicMemberTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestMethod, TestCategory("Gated")]
        public void InternalPropertyInGroupKey()
        {
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * NonPublicKnobs.Mult, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });

            Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);
            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void PrivateFieldInGroupKey()
        {
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(NonPublicKnobs.PrivateFieldKey, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });

            Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);
            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void InternalMethodInWhere()
        {
            var result = Input()
                .Where(e => NonPublicKnobs.IsOdd(e.Key))
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * 10, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });

            Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);
            CollectionAssert.AreEquivalent(new[] { "5:10=2", "10:10=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void InternalPropertyInSelect()
        {
            var result = Input()
                .Select(e => new CapturedInput { Key = e.Key * NonPublicKnobs.Mult })
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });

            Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);
            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
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
    public class NonPublicMemberTestsRow : NonPublicMemberTestsBase
    {
        public NonPublicMemberTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // The default settings, where a failed code generation falls back to row mode.
    [TestClass]
    public class NonPublicMemberTestsColumnarFallback : NonPublicMemberTestsBase
    {
        public NonPublicMemberTestsColumnarFallback()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }

    // Columnar with fallback disabled: generated code must reach the non-public members itself.
    [TestClass]
    public class NonPublicMemberTestsColumnar : NonPublicMemberTestsBase
    {
        public NonPublicMemberTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
