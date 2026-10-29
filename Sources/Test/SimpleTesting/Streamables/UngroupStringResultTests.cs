using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class UngroupInput
    {
        public string Key;
    }

    public abstract class UngroupStringResultTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // a@1, b@2 and a@4 fall in the window that ends at 5; b@7 and a@9 in the one that ends at 10.
        private static readonly string[] Expected = { "5:a=2", "5:b=1", "10:a=1", "10:b=1" };

        internal UngroupStringResultTestsBase(ConfigModifier modifier) : base(modifier) { }

        // A string result cannot be columnar, so Ungroup uses its row pipe even when the input is columnar.
        [TestMethod, TestCategory("Gated")]
        public void GroupApplyWithStringResult()
        {
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupApply(e => e.Key, g => g.Count(), (g, c) => g.Key + "=" + c);

            CollectionAssert.AreEquivalent(Expected, Collect(result));
        }

        // The Ungroup form for a stream without an outer key. No public operator builds it.
        [TestMethod, TestCategory("Gated")]
        public void UngroupWithStringResult()
        {
            var counts = new GroupStreamable<Empty, UngroupInput, string>(Input(), e => e.Key)
                .TumblingWindowLifetime(5)
                .Count();
            var result = new UngroupStreamable<string, ulong, string>(counts, (k, c) => k + "=" + c);

            CollectionAssert.AreEquivalent(Expected, Collect(result));
        }

        private static IStreamable<Empty, UngroupInput> Input()
            => new[] { Point(1, "a"), Point(2, "b"), Point(4, "a"), Point(7, "b"), Point(9, "a") }
                .ToObservable()
                .ToStreamable();

        private static StreamEvent<UngroupInput> Point(long time, string key)
            => StreamEvent.CreatePoint(time, new UngroupInput { Key = key });

        private static List<string> Collect(IStreamable<Empty, string> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.Payload)
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class UngroupStringResultTestsRow : UngroupStringResultTestsBase
    {
        public UngroupStringResultTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class UngroupStringResultTestsColumnar : UngroupStringResultTestsBase
    {
        public UngroupStringResultTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
