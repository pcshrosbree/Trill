using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public abstract class PartitionedArrayIngressTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const string Data = "0:[1,5)=10 1:[2,6)=20 0:[3,7)=30 1:[4,8)=40";

        private static readonly PartitionedStreamEvent<int, int>[] Events =
        {
            PartitionedStreamEvent.CreateInterval(0, 1, 5, 10),
            PartitionedStreamEvent.CreateInterval(1, 2, 6, 20),
            PartitionedStreamEvent.CreateInterval(0, 3, 7, 30),
            PartitionedStreamEvent.CreateInterval(1, 4, 8, 40),
        };

        private static readonly string EndOfStream = Data + " LW@" + StreamEvent.InfinitySyncTime;

        private readonly bool columnar;

        internal PartitionedArrayIngressTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar)) => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void DefaultBatchSize() => Assert.AreEqual(EndOfStream, Show(OneSegment().ToStreamable()));

        [TestMethod, TestCategory("Gated")]
        public void PartialLastBatch()
        {
            using (new ConfigModifier().DataBatchSize(3).Modify())
            {
                Assert.AreEqual(EndOfStream, Show(OneSegment().ToStreamable()));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void FullBatchesThenLowWatermark()
        {
            using (new ConfigModifier().DataBatchSize(2).Modify())
            {
                Assert.AreEqual(EndOfStream, Show(OneSegment().ToStreamable()));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void LowWatermarkFillsLastBatch()
        {
            using (new ConfigModifier().DataBatchSize(5).Modify())
            {
                Assert.AreEqual(EndOfStream, Show(OneSegment().ToStreamable()));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void TwoSegmentsWithOffset()
        {
            var padded = new[] { PartitionedStreamEvent.CreateInterval(0, 0, 9, -1) }.Concat(Events.Skip(1)).ToArray();
            var segments = new[]
            {
                new ArraySegment<PartitionedStreamEvent<int, int>>(Events, 0, 1),
                new ArraySegment<PartitionedStreamEvent<int, int>>(padded, 1, 3),
            };
            Assert.AreEqual(EndOfStream, Show(segments.ToObservable().ToStreamable()));
        }

        [TestMethod, TestCategory("Gated")]
        public void FlushPolicy()
            => Assert.AreEqual(Data + " LW@4", Show(OneSegment().ToStreamable(OnCompletedPolicy.Flush)));

        [TestMethod, TestCategory("Gated")]
        public void QueryContainerInput()
        {
            var container = new QueryContainer();
            var input = container.RegisterInput(OneSegment());
            Assert.AreEqual(this.columnar, input.Properties.IsColumnar);
            var output = new List<PartitionedStreamEvent<int, int>>();
            using (container.RegisterOutput(input).Subscribe(output.Add))
            {
                container.Restore();
            }

            Assert.AreEqual(EndOfStream, Format(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void LowWatermarkIsNotData()
        {
            const string expected = "0:[5,6)=10 0:[7,8)=30 1:[6,7)=20 1:[8,9)=40 LW@3155378975999999999";
            Assert.AreEqual(expected, Format(Events.ToObservable().ToStreamable().PointAtEnd().ToStreamEventObservable().ToEnumerable()));
            Assert.AreEqual(expected, Format(OneSegment().ToStreamable().PointAtEnd().ToStreamEventObservable().ToEnumerable()));
        }

        [TestMethod, TestCategory("Gated")]
        public void LowWatermarkRowIsMarkedAsNotData()
        {
            var batches = OneSegment().ToStreamable().ToStreamMessageObservable().ToEnumerable().ToArray();
            var rows = batches.SelectMany(b => Enumerable.Range(0, b.Count).Select(i =>
                (other: b.vother.col[i], marked: (b.bitvector.col[i >> 6] & (1L << (i & 0x3f))) != 0))).ToArray();
            foreach (var b in batches) b.Free();
            Assert.AreEqual(5, rows.Length);
            Assert.IsTrue(rows.Take(4).All(r => !r.marked));
            Assert.AreEqual((PartitionedStreamEvent.LowWatermarkOtherTime, true), rows[4]);
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupSpansFullAndLastBatch()
        {
            var events = new[] { 10, 10, 10, 10, 20 }.Select(v => PartitionedStreamEvent.CreateStart(1, 1, v)).ToArray();
            using (new ConfigModifier().DataBatchSize(3).Modify())
            {
                Assert.AreEqual("1 4", GroupCounts(events.ToObservable().ToStreamable()));
                Assert.AreEqual("1 4", GroupCounts(new[] { new ArraySegment<PartitionedStreamEvent<int, int>>(events) }.ToObservable().ToStreamable()));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void ObservableOfEventsControl()
            => Assert.AreEqual(EndOfStream, Format(Events.ToObservable().ToStreamable().ToStreamEventObservable().ToEnumerable()));

        private static IObservable<ArraySegment<PartitionedStreamEvent<int, int>>> OneSegment()
            => new[] { new ArraySegment<PartitionedStreamEvent<int, int>>(Events) }.ToObservable();

        private static string Format(IEnumerable<PartitionedStreamEvent<int, int>> output)
            => string.Join(" ", output.Select(e => e.IsLowWatermark
                ? "LW@" + e.SyncTime
                : e.IsData ? $"{e.PartitionKey}:[{e.StartTime},{e.EndTime})={e.Payload}" : "P" + e.PartitionKey + "@" + e.SyncTime));

        private static string GroupCounts(IStreamable<PartitionKey<int>, int> query)
            => string.Join(" ", query.GroupAggregate(v => v, w => w.Count(), (key, count) => count)
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload).OrderBy(c => c));

        private string Show(IStreamable<PartitionKey<int>, int> query)
        {
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            return Format(query.ToStreamEventObservable().ToEnumerable());
        }
    }

    [TestClass]
    public class PartitionedArrayIngressTestsRow : PartitionedArrayIngressTestsBase
    {
        public PartitionedArrayIngressTestsRow() : base(false) { }
    }

    [TestClass]
    public class PartitionedArrayIngressTestsColumnar : PartitionedArrayIngressTestsBase
    {
        public PartitionedArrayIngressTestsColumnar() : base(true) { }
    }
}
