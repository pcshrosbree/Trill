using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [InlineArray(2)]
    public struct InlinePair
    {
        public int Element;

        public static InlinePair Of(int first, int second)
        {
            var pair = default(InlinePair);
            pair[0] = first;
            pair[1] = second;
            return pair;
        }

        public static int Second(InlinePair pair) => pair[1];

        public static string Show(InlinePair pair) => "[" + pair[0] + ", " + pair[1] + "]";

        public int Bump() => ++this.Element;
    }

    public struct InlinePairHolder
    {
        public int A;
        public InlinePair Pair;
    }

    // Columnar mode would keep only the first element of an inline array, so such a payload runs in row mode.
    public abstract class InlineArrayPayloadTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private readonly bool columnar;

        internal InlineArrayPayloadTestsBase(bool columnar, bool strict)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(strict))
            => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void IngressToEgress()
            => Assert.AreEqual("[3, 7]", Run(Ingress(InlinePair.Of(3, 7)), InlinePair.Show, false));

        [TestMethod, TestCategory("Gated")]
        public void WhereReadsTheSecondElement()
            => Assert.AreEqual("[3, 7]", Run(Separate(InlinePair.Of(3, 7), InlinePair.Of(3, 8)).Where(p => InlinePair.Second(p) == 7), InlinePair.Show, false));

        [TestMethod, TestCategory("Gated")]
        public void WhereReadsTheElementField()
            => Assert.AreEqual("[3, 7]", Run(Separate(InlinePair.Of(3, 7), InlinePair.Of(4, 7)).Where(p => p.Element == 3), InlinePair.Show, false));

        [TestMethod, TestCategory("Gated")]
        public void WhereCallsAMethodThatChangesThePayload()
            => Assert.AreEqual("[3, 7]", Run(Separate(InlinePair.Of(3, 7)).Where(p => p.Bump() == 4), InlinePair.Show, false));

        [TestMethod, TestCategory("Gated")]
        public void SelectIntoAnInlineArray()
            => Assert.AreEqual("[3, 7]", Run(Separate(new InlinePairHolder { A = 1, Pair = InlinePair.Of(3, 7) }).Select(h => h.Pair), InlinePair.Show, false));

        [TestMethod, TestCategory("Gated")]
        public void GroupBySecondElement()
        {
            // All three events start at the same time, so one snapshot counts them.
            var query = new[] { InlinePair.Of(3, 7), InlinePair.Of(4, 7), InlinePair.Of(3, 8) }
                .Select(p => StreamEvent.CreatePoint(1, p)).ToObservable().ToStreamable()
                .GroupAggregate(p => InlinePair.Second(p), w => w.Count(), (g, c) => g.Key * 10 + (int)c);
            Assert.AreEqual("72 81", Run(query, x => x.ToString(), false, sorted: true));
        }

        [TestMethod, TestCategory("Gated")]
        public void StructHoldingAnInlineArrayStaysColumnarControl()
            => Assert.AreEqual("1 [3, 7]", Run(
                Separate(new InlinePairHolder { A = 1, Pair = InlinePair.Of(3, 7) }).Where(h => InlinePair.Second(h.Pair) == 7),
                h => h.A + " " + InlinePair.Show(h.Pair),
                this.columnar));

        private static IStreamable<Empty, T> Ingress<T>(params T[] values)
            => values.Select((v, i) => StreamEvent.CreatePoint(i + 1, v)).ToObservable().ToStreamable();

        // The Union keeps the next operator out of the ingress, so it runs as a separate operator.
        private static IStreamable<Empty, T> Separate<T>(params T[] values)
            => Ingress(values).Union(Array.Empty<StreamEvent<T>>().ToObservable().ToStreamable());

        private static string Run<T>(IStreamable<Empty, T> query, Func<T, string> show, bool expectColumnar, bool sorted = false)
        {
            Assert.AreEqual(expectColumnar, query.Properties.IsColumnar);
            var values = query.ToStreamEventObservable(ReshapingPolicy.CoalesceEndEdges).ToEnumerable()
                .Where(e => e.IsData).Select(e => show(e.Payload));
            return string.Join(" ", sorted ? values.OrderBy(v => v, StringComparer.Ordinal) : values);
        }
    }

    [TestClass]
    public class InlineArrayPayloadTestsRow : InlineArrayPayloadTestsBase
    {
        public InlineArrayPayloadTestsRow() : base(false, false) { }
    }

    [TestClass]
    public class InlineArrayPayloadTestsColumnar : InlineArrayPayloadTestsBase
    {
        public InlineArrayPayloadTestsColumnar() : base(true, false) { }
    }

    [TestClass]
    public class InlineArrayPayloadTestsColumnarStrict : InlineArrayPayloadTestsBase
    {
        public InlineArrayPayloadTestsColumnarStrict() : base(true, true) { }
    }

    [TestClass]
    public class InlineArrayRefusalTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public InlineArrayRefusalTests() : base(new ConfigModifier().ForceRowBasedExecution(false)) { }

        [TestMethod, TestCategory("Gated")]
        public void TheTypeCheckRefusesAnInlineArray()
        {
            Assert.IsFalse(typeof(InlinePair).CanRepresentAsColumnar());
            Assert.IsTrue(typeof(InlinePairHolder).CanRepresentAsColumnar());
        }

        [TestMethod, TestCategory("Gated")]
        public void TheReportNamesTheReason()
        {
            var reports = new List<RowModeReport>();
            Config.RowModeReporter = reports.Add;
            try
            {
                var query = new[] { StreamEvent.CreatePoint(1, InlinePair.Of(3, 7)) }.ToObservable().ToStreamable();
                query.ToStreamEventObservable().ToEnumerable().ToList();
            }
            finally
            {
                Config.RowModeReporter = null;
            }

            var report = reports.First(r => r.Reason == RowModeReason.TypeNotColumnar);
            Assert.AreEqual(typeof(InlinePair), report.NonColumnarType);
            Assert.AreEqual("the type is an inline array", report.Detail);
        }
    }
}
