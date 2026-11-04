using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public abstract class AtemporalArrayWallClockTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private readonly bool columnar;

        internal AtemporalArrayWallClockTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar)) => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void NoPolicy() => Check(Values().ToAtemporalArrayStreamable(onCompletedPolicy: OnCompletedPolicy.EndOfStream));

        [TestMethod, TestCategory("Gated")]
        public void WallClock() => Check(Values().ToAtemporalArrayStreamable(TimelinePolicy.WallClock(), OnCompletedPolicy.EndOfStream));

        [TestMethod, TestCategory("Gated")]
        public void QueryContainerWithNoPolicy()
        {
            var container = new QueryContainer();
            var input = container.RegisterAtemporalArrayInput(Values(), onCompletedPolicy: OnCompletedPolicy.EndOfStream);
            Assert.AreEqual(this.columnar, input.Properties.IsColumnar);
            var output = new List<StreamEvent<int>>();
            using (container.RegisterOutput(input).Subscribe(output.Add))
            {
                container.Restore();
            }

            CheckEvents(output);
        }

        [TestMethod, TestCategory("Gated")]
        public void WallClockWithAnInterval()
        {
            var policy = TimelinePolicy.WallClock(TimeSpan.FromSeconds(1));
            Assert.ThrowsException<NotSupportedException>(() => Values().ToAtemporalArrayStreamable(policy, OnCompletedPolicy.EndOfStream));
            Assert.ThrowsException<NotSupportedException>(() => new QueryContainer().RegisterAtemporalArrayInput(Values(), policy));
        }

        [TestMethod, TestCategory("Gated")]
        public void SequenceControl()
        {
            var query = Values().ToAtemporalArrayStreamable(TimelinePolicy.Sequence(1), OnCompletedPolicy.EndOfStream);
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            var data = query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData);
            Assert.AreEqual("1@0 2@1 3@2", string.Join(" ", data.Select(e => e.Payload + "@" + e.StartTime)));
        }

        private static IObservable<ArraySegment<int>> Values() => new[] { new ArraySegment<int>(new[] { 1, 2, 3 }) }.ToObservable();

        private static void CheckEvents(IList<StreamEvent<int>> output)
        {
            var data = output.Where(e => e.IsData).ToList();
            Assert.AreEqual("1 2 3", string.Join(" ", data.Select(e => e.Payload)));
            for (int i = 1; i < data.Count; i++) Assert.IsTrue(data[i - 1].StartTime <= data[i].StartTime);
        }

        private void Check(IStreamable<Empty, int> query)
        {
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            CheckEvents(query.ToStreamEventObservable().ToEnumerable().ToList());
        }
    }

    [TestClass]
    public class AtemporalArrayWallClockTestsRow : AtemporalArrayWallClockTestsBase
    {
        public AtemporalArrayWallClockTestsRow() : base(false) { }
    }

    [TestClass]
    public class AtemporalArrayWallClockTestsColumnar : AtemporalArrayWallClockTestsBase
    {
        public AtemporalArrayWallClockTestsColumnar() : base(true) { }
    }
}
