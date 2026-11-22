// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class WindowLifetimeArgumentTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // A window size and a period must be positive. The checks run when the operator is called, before any
        // subscription, and name the parameter. No test here subscribes to a refused query: a period of 0 used to
        // divide by zero at the call, and a negative period in RepetitiveHoppingWindowLifetime used to give a query
        // that never returned.
        [TestMethod, TestCategory("Gated")]
        public void TumblingDurationOfZeroIsRefused()
            => Refused("tumbleDuration", () => Ticks().TumblingWindowLifetime(0));

        [TestMethod, TestCategory("Gated")]
        public void TumblingNegativeDurationIsRefused()
            => Refused("tumbleDuration", () => Ticks().TumblingWindowLifetime(-3));

        [TestMethod, TestCategory("Gated")]
        public void HoppingWindowSizeOfZeroIsRefused()
            => Refused("windowSize", () => Ticks().HoppingWindowLifetime(0, 2));

        [TestMethod, TestCategory("Gated")]
        public void HoppingNegativeWindowSizeIsRefused()
            => Refused("windowSize", () => Ticks().HoppingWindowLifetime(-1, 2));

        [TestMethod, TestCategory("Gated")]
        public void HoppingPeriodOfZeroIsRefused()
            => Refused("period", () => Ticks().HoppingWindowLifetime(4, 0));

        [TestMethod, TestCategory("Gated")]
        public void HoppingNegativePeriodIsRefused()
            => Refused("period", () => Ticks().HoppingWindowLifetime(4, -3));

        [TestMethod, TestCategory("Gated")]
        public void RepetitiveHoppingPeriodOfZeroIsRefused()
            => Refused("period", () => Ticks().RepetitiveHoppingWindowLifetime(4, 0));

        [TestMethod, TestCategory("Gated")]
        public void RepetitiveHoppingNegativePeriodIsRefused()
            => Refused("period", () => Ticks().RepetitiveHoppingWindowLifetime(4, -3));

        // A null source is refused, and before any other argument is looked at.
        [TestMethod, TestCategory("Gated")]
        public void HoppingNullSourceIsRefused()
            => RefusedNull("source", () => NoStream().HoppingWindowLifetime(4, 2));

        [TestMethod, TestCategory("Gated")]
        public void HoppingNullSourceIsRefusedBeforeThePeriod()
            => RefusedNull("source", () => NoStream().HoppingWindowLifetime(4, 0));

        // Valid calls give the lifetimes they gave before: each start moves to the first multiple of the period,
        // plus the offset, at or after it, and the event lasts the window size. The first two are the
        // documentation catalogue's results for the same input.
        [TestMethod, TestCategory("Gated")]
        public void TumblingWindowIsUnchanged()
            => CollectionAssert.AreEquivalent(
                new[] { "a1:4-8", "b2:4-8", "a3:4-8", "b4:8-12", "a5:12-16" },
                Lifetimes(Ticks().TumblingWindowLifetime(4)));

        [TestMethod, TestCategory("Gated")]
        public void HoppingWindowIsUnchanged()
            => CollectionAssert.AreEquivalent(
                new[] { "a1:2-6", "b2:2-6", "a3:4-8", "b4:8-12", "a5:10-14" },
                Lifetimes(Ticks().HoppingWindowLifetime(4, 2)));

        [TestMethod, TestCategory("Gated")]
        public void HoppingSmallestWindowSizeAndPeriodAreAccepted()
            => CollectionAssert.AreEquivalent(
                new[] { "a1:1-2", "b2:2-3", "a3:4-5", "b4:7-8", "a5:9-10" },
                Lifetimes(Ticks().HoppingWindowLifetime(1, 1)));

        [TestMethod, TestCategory("Gated")]
        public void HoppingNegativeOffsetIsAccepted()
            => CollectionAssert.AreEquivalent(
                new[] { "a1:1-5", "b2:3-7", "a3:5-9", "b4:7-11", "a5:9-13" },
                Lifetimes(Ticks().HoppingWindowLifetime(4, 2, -1)));

        [TestMethod, TestCategory("Gated")]
        public void RepetitiveHoppingWindowIsUnchanged()
            => CollectionAssert.AreEquivalent(
                new[] { "a1:2-4", "a1:4-6", "b2:2-4", "b2:4-6", "a3:4-6", "a3:6-8", "b4:8-10", "b4:10-12", "a5:10-12", "a5:12-14" },
                Lifetimes(Ticks().RepetitiveHoppingWindowLifetime(4, 2)));

        private static void Refused(string parameter, Func<IStreamable<Empty, string>> build)
        {
            var e = Assert.ThrowsException<ArgumentException>(() => build());
            Assert.AreEqual(parameter, e.ParamName);
        }

        private static void RefusedNull(string parameter, Func<IStreamable<Empty, string>> build)
        {
            var e = Assert.ThrowsException<ArgumentNullException>(() => build());
            Assert.AreEqual(parameter, e.ParamName);
        }

        private static IStreamable<Empty, string> NoStream() => null;

        // Point events at 1, 2, 4, 7 and 9, as in the documentation's Ticks input.
        private static IStreamable<Empty, string> Ticks()
        {
            var input = new[]
            {
                StreamEvent.CreateInterval(1, 2, "a1"),
                StreamEvent.CreateInterval(2, 3, "b2"),
                StreamEvent.CreateInterval(4, 5, "a3"),
                StreamEvent.CreateInterval(7, 8, "b4"),
                StreamEvent.CreateInterval(9, 10, "a5"),
            };

            return input.ToObservable().ToStreamable();
        }

        // Each data event as payload:start-end.
        private static string[] Lifetimes(IStreamable<Empty, string> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload + ":" + e.StartTime + "-" + e.EndTime)
                .ToEnumerable()
                .ToArray();
    }
}
