using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class UntransformedResult
    {
        public int Key;
        public int Count;
    }

    // Each query has a selector that the columnar code generator cannot transform, so in columnar mode the
    // operator must fall back to row mode.
    public abstract class UntransformedSelectorTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal UntransformedSelectorTestsBase(ConfigModifier modifier) : base(modifier.MapArity(1)) { }

        // With start edges only and MapArity 1, GroupAggregate builds a GroupedWindowStreamable. The result
        // selector contains a lambda.
        [TestMethod, TestCategory("Gated")]
        public void GroupAggregateResultSelectorWithALambda()
        {
            var query = Enumerable.Range(1, 3)
                .Select(i => StreamEvent.CreateStart(i, i))
                .ToObservable()
                .ToStreamable()
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .GroupAggregate(k => k, w => w.Count(), (g, c) => new UntransformedResult { Key = g.Key, Count = new[] { (int)c }.Count(x => x > 0) });

            StringAssert.StartsWith(query.GetType().Name, "GroupedWindowStreamable");
            Check(query, "1:1 2:1 3:1");
        }

        // SelectMany over Enumerable.Repeat transforms the repeated element, here a captured object.
        [TestMethod, TestCategory("Gated")]
        public void SelectManyRepeatsACapturedObject()
        {
            var captured = new UntransformedResult { Key = 5, Count = 6 };
            var query = Enumerable.Range(1, 3)
                .Select(i => StreamEvent.CreatePoint(i, i))
                .ToObservable()
                .ToStreamable()
                .Union(Array.Empty<StreamEvent<int>>().ToObservable().ToStreamable())
                .SelectMany(i => Enumerable.Repeat(captured, 1));

            Check(query, "5:6 5:6 5:6");
        }

        private static void Check(IStreamable<Empty, UntransformedResult> query, string expected)
        {
            var values = query.ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload.Key + ":" + e.Payload.Count)
                .ToEnumerable()
                .OrderBy(v => v, StringComparer.Ordinal);
            Assert.AreEqual(expected, string.Join(" ", values));
            Assert.IsFalse(query.Properties.IsColumnar, "columnar");
        }
    }

    [TestClass]
    public class UntransformedSelectorTestsRow : UntransformedSelectorTestsBase
    {
        public UntransformedSelectorTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class UntransformedSelectorTestsColumnar : UntransformedSelectorTestsBase
    {
        public UntransformedSelectorTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }
}
