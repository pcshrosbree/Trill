using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct PartitionedArrayReading
    {
        public int Key;
        public long Start;
        public long End;
        public int Value;
    }

    // The repro from fork #98: the low watermark that ends ToPartitionedArrayStreamable must not be read as data.
    public abstract class PartitionedPayloadArrayIngressTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private static readonly PartitionedArrayReading[] Readings =
        {
            new PartitionedArrayReading { Key = 10, Start = 10, End = 15, Value = 10 },
            new PartitionedArrayReading { Key = 0, Start = 20, End = 25, Value = 20 },
            new PartitionedArrayReading { Key = 10, Start = 30, End = 35, Value = 30 },
            new PartitionedArrayReading { Key = 0, Start = 40, End = 45, Value = 40 },
        };

        private static readonly string EndPoints = "10:[15,16)=10 0:[25,26)=20 0:[45,46)=40 10:[35,36)=30 LW@" + StreamEvent.InfinitySyncTime;

        private readonly bool columnar;

        internal PartitionedPayloadArrayIngressTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar)) => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void IntervalsThroughPointAtEnd()
        {
            var events = Readings.Select(r => PartitionedStreamEvent.CreateInterval(r.Key, r.Start, r.End, r));
            Assert.AreEqual(EndPoints, Format(events.ToObservable().ToStreamable().PointAtEnd()));
            Assert.AreEqual(EndPoints, Format(Intervals().PointAtEnd()));
        }

        [TestMethod, TestCategory("Gated")]
        public void LowWatermarkStartsABatch()
        {
            using (new ConfigModifier().DataBatchSize(4).Modify())
            {
                Assert.AreEqual(EndPoints, Format(Intervals().PointAtEnd()));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void LowWatermarkRowIsMarkedAsNotData()
            => Assert.AreEqual("15:False 25:False 35:False 45:False -1:True", this.MarkedRows(Intervals()));

        [TestMethod, TestCategory("Gated")]
        public void StartEdgeOverloadMarksTheLowWatermark()
        {
            var query = Segment().ToPartitionedArrayStreamable(r => r.Key, r => r.Start);
            var other = StreamEvent.InfinitySyncTime;
            Assert.AreEqual($"{other}:False {other}:False {other}:False {other}:False -1:True", this.MarkedRows(query));
        }

        [TestMethod, TestCategory("Gated")]
        public void StraightToEgressControl()
        {
            var query = Intervals();
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            Assert.AreEqual("10:[10,15)=10 0:[20,25)=20 10:[30,35)=30 0:[40,45)=40 LW@" + StreamEvent.InfinitySyncTime, Format(query));
        }

        private static IObservable<ArraySegment<PartitionedArrayReading>> Segment()
            => new[] { new ArraySegment<PartitionedArrayReading>(Readings) }.ToObservable();

        private static IStreamable<PartitionKey<int>, PartitionedArrayReading> Intervals()
            => Segment().ToPartitionedArrayStreamable(r => r.Key, r => r.Start, r => r.End);

        private static string Format(IStreamable<PartitionKey<int>, PartitionedArrayReading> query)
            => string.Join(" ", query.ToStreamEventObservable().ToEnumerable()
                .Where(e => e.IsData || e.IsLowWatermark)
                .Select(e => e.IsLowWatermark ? "LW@" + e.SyncTime : $"{e.PartitionKey}:[{e.StartTime},{e.EndTime})={e.Payload.Value}"));

        // Each row of the ingress batches as its OtherTime and whether its bit is set.
        private string MarkedRows(IStreamable<PartitionKey<int>, PartitionedArrayReading> query)
        {
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            var batches = query.ToStreamMessageObservable().ToEnumerable().ToArray();
            var rows = batches.SelectMany(b => Enumerable.Range(0, b.Count).Select(i =>
                b.vother.col[i] + ":" + ((b.bitvector.col[i >> 6] & (1L << (i & 0x3f))) != 0))).ToArray();
            foreach (var b in batches) b.Free();
            return string.Join(" ", rows);
        }
    }

    [TestClass]
    public class PartitionedPayloadArrayIngressTestsRow : PartitionedPayloadArrayIngressTestsBase
    {
        public PartitionedPayloadArrayIngressTestsRow() : base(false) { }
    }

    [TestClass]
    public class PartitionedPayloadArrayIngressTestsColumnar : PartitionedPayloadArrayIngressTestsBase
    {
        public PartitionedPayloadArrayIngressTestsColumnar() : base(true) { }
    }
}
