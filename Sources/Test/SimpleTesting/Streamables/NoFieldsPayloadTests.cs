// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // No instance fields anywhere in its class hierarchy, and no auto-properties: Trill stores it whole, in one column.
    public sealed class NoFieldsPayload
    {
        public int Two => 2;
    }

    // Controls: a struct with no fields is stored whole too, and a class with one public field gets a column for it.
    public struct NoFieldsStructPayload
    {
        public int Two => 2;
    }

    public sealed class OneFieldPayload
    {
        public int Value;
    }

    // The generated columnar ingress for a class stored whole used to write an extra closing brace, so it did not
    // compile: with fallback disabled it threw, and with fallback allowed it ran in row mode.
    public abstract class NoFieldsPayloadTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private ConcurrentQueue<RowModeReport> reports;

        internal NoFieldsPayloadTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestInitialize]
        public void StartCollecting()
        {
            var queue = new ConcurrentQueue<RowModeReport>();
            this.reports = queue;
            Config.RowModeReporter = queue.Enqueue;
        }

        [TestCleanup]
        public void StopCollecting() => Config.RowModeReporter = null;

        [TestMethod, TestCategory("Gated")]
        public void IngressOfAClassWithNoFieldsRuns()
        {
            var payloads = new[] { new NoFieldsPayload(), new NoFieldsPayload() };
            var query = Ingress(payloads);

            var output = RoundTrip(query);

            Assert.AreEqual(2, output.Length);
            Assert.AreSame(payloads[0], output[0], "a payload stored whole comes back as the same object");
            Assert.AreSame(payloads[1], output[1]);
            this.AssertColumnarWithNoReport(query);
        }

        // The batch that the ingress builds must also work in the operators after it. The predicate uses the payload
        // itself, not one of its members.
        [TestMethod, TestCategory("Gated")]
        public void AClassWithNoFieldsRunsThroughOperators()
        {
            var first = new NoFieldsPayload();
            var second = new NoFieldsPayload();
            var query = Ingress(new[] { first }).Union(Ingress(new[] { second })).Where(p => p != null);

            var output = RoundTrip(query);

            Assert.AreEqual(2, output.Length);
            Assert.IsTrue(output.Contains(first) && output.Contains(second));
            this.AssertColumnarWithNoReport(query);
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressOfAStructWithNoFieldsRuns()
        {
            var query = Ingress(new[] { default(NoFieldsStructPayload) });

            Assert.AreEqual(1, RoundTrip(query).Length);
            this.AssertColumnarWithNoReport(query);
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressOfAClassWithOneFieldRuns()
        {
            var query = Ingress(new[] { new OneFieldPayload { Value = 7 } });

            var output = RoundTrip(query);

            Assert.AreEqual(1, output.Length);
            Assert.AreEqual(7, output[0].Value);
            this.AssertColumnarWithNoReport(query);
        }

        private static IStreamable<Empty, T> Ingress<T>(T[] payloads)
            => payloads.Select((p, i) => StreamEvent.CreateInterval(i + 1, i + 5, p)).ToObservable().ToStreamable();

        private static T[] RoundTrip<T>(IStreamable<Empty, T> query)
            => query.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload).ToEnumerable().ToArray();

        private void AssertColumnarWithNoReport<T>(IStreamable<Empty, T> query)
        {
            Assert.AreEqual(!Config.ForceRowBasedExecution, query.Properties.IsColumnar);
            Assert.AreEqual(0, this.reports.Count, string.Join("; ", this.reports.Select(r => r.ToString())));
        }
    }

    [TestClass]
    public class NoFieldsPayloadTestsRow : NoFieldsPayloadTestsBase
    {
        public NoFieldsPayloadTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // Columnar, with fallback disabled, so that a failed code generation throws.
    [TestClass]
    public class NoFieldsPayloadTestsColumnar : NoFieldsPayloadTestsBase
    {
        public NoFieldsPayloadTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
