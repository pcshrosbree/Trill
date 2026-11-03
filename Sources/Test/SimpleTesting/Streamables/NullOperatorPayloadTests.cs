using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // operator == says that a reading whose Value is 0 equals null.
    public sealed class NullOperatorReading
    {
        public NullOperatorReading() { }

        public NullOperatorReading(int value) => this.Value = value;

        public int Value;
        public long Time;

        public static bool operator ==(NullOperatorReading x, NullOperatorReading y)
            => ReferenceEquals(x, y) || (ReferenceEquals(x, null) ? y.Value == 0 : ReferenceEquals(y, null) ? x.Value == 0 : x.Value == y.Value);

        public static bool operator !=(NullOperatorReading x, NullOperatorReading y) => !(x == y);

        public override bool Equals(object obj) => obj is NullOperatorReading other && other.Value == this.Value;

        public override int GetHashCode() => this.Value;
    }

    public sealed class NullOperatorPlain
    {
        public int Value;
    }

    // Each test sends a reading with Value 0 and one with Value 5 through a different path that stores payloads in
    // columns. Both must come back as objects, not null.
    public abstract class NullOperatorPayloadTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const string Expected = "Value 0, Value 5";

        internal NullOperatorPayloadTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestMethod, TestCategory("Gated")]
        public void TemporalIngress()
            => Check(Readings().Select((r, i) => StreamEvent.CreatePoint(r.Time, r)).ToObservable().ToStreamable());

        [TestMethod, TestCategory("Gated")]
        public void AtemporalIngress()
            => Check(Readings().ToObservable().ToAtemporalStreamable(TimelinePolicy.Sequence(5)));

        [TestMethod, TestCategory("Gated")]
        public void ArrayIngressOfStreamEvents()
            => Check(Segment(Readings().Select(r => StreamEvent.CreatePoint(r.Time, r))).ToStreamable());

        [TestMethod, TestCategory("Gated")]
        public void ArrayIngressWithAStartEdge()
            => Check(Segment(Readings()).ToTemporalArrayStreamable(r => r.Time));

        [TestMethod, TestCategory("Gated")]
        public void AtemporalArrayIngressOnTheWallClock()
            => Check(Segment(Readings()).ToAtemporalArrayStreamable(TimelinePolicy.WallClock(TimeSpan.FromSeconds(1)), OnCompletedPolicy.EndOfStream));

        [TestMethod, TestCategory("Gated")]
        public void AtemporalArrayIngressInSequence()
            => Check(Segment(Readings()).ToAtemporalArrayStreamable(TimelinePolicy.Sequence(5), OnCompletedPolicy.EndOfStream));

        // A Select whose body is a constructor call stores each result through the batch indexer.
        [TestMethod, TestCategory("Gated")]
        public void SelectThroughTheBatchIndexer()
        {
            var values = new[] { StreamEvent.CreatePoint(1, 0), StreamEvent.CreatePoint(2, 5) }
                .ToObservable()
                .ToStreamable()
                .Union(Array.Empty<StreamEvent<int>>().ToObservable().ToStreamable());
            Check(values.Select(v => new NullOperatorReading(v)));
        }

        // Control: the same class without the operator.
        [TestMethod, TestCategory("Gated")]
        public void TemporalIngressWithoutTheOperator()
        {
            var query = new[] { StreamEvent.CreatePoint(1, new NullOperatorPlain { Value = 0 }), StreamEvent.CreatePoint(2, new NullOperatorPlain { Value = 5 }) }
                .ToObservable()
                .ToStreamable();
            Assert.AreEqual(!Config.ForceRowBasedExecution, query.Properties.IsColumnar, "columnar");
            var payloads = query.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload).ToEnumerable()
                .Select(p => ReferenceEquals(p, null) ? "null" : "Value " + p.Value);
            Assert.AreEqual(Expected, string.Join(", ", payloads));
        }

        internal static NullOperatorReading[] Readings()
            => new[] { new NullOperatorReading { Value = 0, Time = 1 }, new NullOperatorReading { Value = 5, Time = 2 } };

        internal static IObservable<ArraySegment<T>> Segment<T>(System.Collections.Generic.IEnumerable<T> items)
            => new[] { new ArraySegment<T>(items.ToArray()) }.ToObservable();

        private static void Check(IStreamable<Empty, NullOperatorReading> query)
            => Check(query.Properties.IsColumnar, query.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload));

        internal static void Check(bool columnar, IObservable<NullOperatorReading> payloads)
        {
            var shown = payloads.ToEnumerable()
                .Select(p => ReferenceEquals(p, null) ? "null" : "Value " + p.Value)
                .OrderBy(s => s, StringComparer.Ordinal);
            Assert.AreEqual(Expected, string.Join(", ", shown));
            Assert.AreEqual(!Config.ForceRowBasedExecution, columnar, "columnar");
        }
    }

    [TestClass]
    public class NullOperatorPayloadTestsRow : NullOperatorPayloadTestsBase
    {
        public NullOperatorPayloadTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class NullOperatorPayloadTestsColumnar : NullOperatorPayloadTestsBase
    {
        public NullOperatorPayloadTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }

    // Partitioned array ingress, columnar mode only. Fallback is allowed because the columnar egress of a partitioned
    // stream does not compile, so only the egress falls back; the ingress under test stays columnar.
    [TestClass]
    public class NullOperatorPartitionedPayloadTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public NullOperatorPartitionedPayloadTests()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedArrayIngressWithAStartEdge()
            => Check(NullOperatorPayloadTestsBase.Segment(NullOperatorPayloadTestsBase.Readings())
                .ToPartitionedArrayStreamable(r => r.Value % 2, r => r.Time));

        [TestMethod, TestCategory("Gated")]
        public void PartitionedArrayIngressWithStartAndEndEdges()
            => Check(NullOperatorPayloadTestsBase.Segment(NullOperatorPayloadTestsBase.Readings())
                .ToPartitionedArrayStreamable(r => r.Value % 2, r => r.Time, r => r.Time + 10));

        [TestMethod, TestCategory("Gated")]
        public void PartitionedArrayIngressOfStreamEvents()
            => Check(NullOperatorPayloadTestsBase.Segment(NullOperatorPayloadTestsBase.Readings()
                .Select(r => PartitionedStreamEvent.CreatePoint(0, r.Time, r))).ToStreamable());

        private static void Check(IStreamable<PartitionKey<int>, NullOperatorReading> query)
            => NullOperatorPayloadTestsBase.Check(
                query.Properties.IsColumnar, query.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload));
    }
}
