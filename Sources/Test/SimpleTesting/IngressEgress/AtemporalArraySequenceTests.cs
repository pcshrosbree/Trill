using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public abstract class AtemporalArraySequenceTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const string OnePerSample = "1@0 2@1 3@2 4@3 5@4 6@5";
        private const string TwoPerSample = "1@0 2@0 3@1 4@1 5@2 6@2";
        private const string ThreePerSample = "1@0 2@0 3@0 4@1 5@1 6@1";

        private readonly bool columnar;

        internal AtemporalArraySequenceTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar)) => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void OneSegmentOnePerSample() => Assert.AreEqual(OnePerSample, Show(1, Segment(1, 2, 3, 4, 5, 6)));

        [TestMethod, TestCategory("Gated")]
        public void OneSegmentTwoPerSample() => Assert.AreEqual(TwoPerSample, Show(2, Segment(1, 2, 3, 4, 5, 6)));

        [TestMethod, TestCategory("Gated")]
        public void OneSegmentThreePerSample() => Assert.AreEqual(ThreePerSample, Show(3, Segment(1, 2, 3, 4, 5, 6)));

        [TestMethod, TestCategory("Gated")]
        public void SampleSpansTwoSegments() => Assert.AreEqual(TwoPerSample, Show(2, Segment(1, 2, 3), Segment(4, 5, 6)));

        [TestMethod, TestCategory("Gated")]
        public void SegmentWithOffset()
            => Assert.AreEqual(TwoPerSample, Show(2, new ArraySegment<int>(new[] { -1, 1, 2, 3, 4, 5, 6, -1 }, 1, 6)));

        [TestMethod, TestCategory("Gated")]
        public void SampleFillsBatch()
        {
            using (new ConfigModifier().DataBatchSize(2).Modify())
            {
                Assert.AreEqual(TwoPerSample, Show(2, Segment(1, 2, 3, 4, 5, 6)));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void SampleSpansTwoBatches()
        {
            using (new ConfigModifier().DataBatchSize(2).Modify())
            {
                Assert.AreEqual(ThreePerSample, Show(3, Segment(1, 2, 3, 4, 5, 6)));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void PunctuationSharesTheSampleBatch()
        {
            var batches = new[] { Segment(1, 2, 3, 4, 5, 6) }.ToObservable()
                .ToAtemporalArrayStreamable(TimelinePolicy.Sequence(2)).ToStreamMessageObservable().ToEnumerable().ToArray();
            var rows = string.Join(" | ", batches.Select(b => string.Join(" ", Enumerable.Range(0, b.Count).Select(i =>
                b.vother.col[i] == StreamEvent.PunctuationOtherTime ? "P" + b.vsync.col[i] : b[i] + "@" + b.vsync.col[i]))));
            foreach (var b in batches) b.Free();
            Assert.AreEqual("1@0 2@0 P0 | 3@1 4@1 P1 | 5@2 6@2 P2", rows);
        }

        [TestMethod, TestCategory("Gated")]
        public void OneValuePerSegmentControl()
            => Assert.AreEqual(TwoPerSample, Show(2, Enumerable.Range(1, 6).Select(i => Segment(i)).ToArray()));

        [TestMethod, TestCategory("Gated")]
        public void AtemporalStreamableControl()
        {
            Assert.AreEqual(OnePerSample, Data(Enumerable.Range(1, 6).ToObservable().ToAtemporalStreamable(TimelinePolicy.Sequence(1), onCompletedPolicy: OnCompletedPolicy.EndOfStream)));
            Assert.AreEqual(TwoPerSample, Data(Enumerable.Range(1, 6).ToObservable().ToAtemporalStreamable(TimelinePolicy.Sequence(2), onCompletedPolicy: OnCompletedPolicy.EndOfStream)));
            Assert.AreEqual(ThreePerSample, Data(Enumerable.Range(1, 6).ToObservable().ToAtemporalStreamable(TimelinePolicy.Sequence(3), onCompletedPolicy: OnCompletedPolicy.EndOfStream)));
        }

        private static ArraySegment<int> Segment(params int[] values) => new ArraySegment<int>(values);

        private static string Data(IStreamable<Empty, int> query)
            => string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => $"{e.Payload}@{e.StartTime}"));

        private string Show(int sampleSize, params ArraySegment<int>[] segments)
        {
            var query = segments.ToObservable().ToAtemporalArrayStreamable(TimelinePolicy.Sequence(sampleSize), OnCompletedPolicy.EndOfStream);
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            return Data(query);
        }
    }

    [TestClass]
    public class AtemporalArraySequenceTestsRow : AtemporalArraySequenceTestsBase
    {
        public AtemporalArraySequenceTestsRow() : base(false) { }
    }

    [TestClass]
    public class AtemporalArraySequenceTestsColumnar : AtemporalArraySequenceTestsBase
    {
        public AtemporalArraySequenceTestsColumnar() : base(true) { }
    }
}
