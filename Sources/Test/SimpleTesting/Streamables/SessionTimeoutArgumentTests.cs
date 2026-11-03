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
    public sealed class SessionTimeoutArgumentInput
    {
        public string Key;
        public int Value;
    }

    public abstract class SessionTimeoutArgumentTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal SessionTimeoutArgumentTestsBase(ConfigModifier modifier) : base(modifier) { }

        // A timeout must be positive. The check runs when SessionTimeoutWindow is called, before any
        // subscription, and names the parameter.
        [TestMethod, TestCategory("Gated")]
        public void SessionTimeoutOfZeroIsRefused()
            => Refused("timeout", () => Input().SessionTimeoutWindow(0));

        [TestMethod, TestCategory("Gated")]
        public void SessionNegativeTimeoutIsRefused()
            => Refused("timeout", () => Input().SessionTimeoutWindow(-1));

        // A positive maxDuration must be greater than the timeout.
        [TestMethod, TestCategory("Gated")]
        public void SessionMaxDurationEqualToTheTimeoutIsRefused()
            => Refused("maxDuration", () => Input().SessionTimeoutWindow(3, 3));

        [TestMethod, TestCategory("Gated")]
        public void SessionMaxDurationBelowTheTimeoutIsRefused()
            => Refused("maxDuration", () => Input().SessionTimeoutWindow(3, 2));

        // No limit stands for StreamEvent.InfinitySyncTime, which the timeout must also be below.
        [TestMethod, TestCategory("Gated")]
        public void SessionTimeoutAtTheNoLimitValueIsRefused()
            => Refused("maxDuration", () => Input().SessionTimeoutWindow(StreamEvent.InfinitySyncTime));

        [TestMethod, TestCategory("Gated")]
        public void SessionTimeoutBelowTheNoLimitValueIsAccepted()
            => Assert.IsNotNull(Input().SessionTimeoutWindow(StreamEvent.InfinitySyncTime - 1));

        // The smallest accepted values still give sessions: a timeout of 1, and a maxDuration one more than
        // the timeout. A maxDuration of 0 or less still means no limit.
        [TestMethod, TestCategory("Gated")]
        public void SessionSmallestTimeoutIsAccepted()
            => CollectionAssert.AreEqual(new[] { "1-2", "2-3", "4-5" }, Lifetimes(Input().SessionTimeoutWindow(1)));

        [TestMethod, TestCategory("Gated")]
        public void SessionMaxDurationOneAboveTheTimeoutIsAccepted()
            => CollectionAssert.AreEqual(new[] { "1-4", "2-4", "4-6" }, Lifetimes(Input().SessionTimeoutWindow(2, 3)));

        [TestMethod, TestCategory("Gated")]
        public void SessionMaxDurationOfZeroOrLessMeansNoLimit()
        {
            var unlimited = new[] { "1-7", "2-7", "4-7" };
            CollectionAssert.AreEqual(unlimited, Lifetimes(Input().SessionTimeoutWindow(3)));
            CollectionAssert.AreEqual(unlimited, Lifetimes(Input().SessionTimeoutWindow(3, 0)));
            CollectionAssert.AreEqual(unlimited, Lifetimes(Input().SessionTimeoutWindow(3, -5)));
        }

        private static void Refused(string parameter, Func<IStreamable<Empty, SessionTimeoutArgumentInput>> build)
        {
            var e = Assert.ThrowsException<ArgumentException>(() => build());
            Assert.AreEqual(parameter, e.ParamName);
        }

        // Events at 1, 2 and 4, one tick long, with no grouping key.
        private static IStreamable<Empty, SessionTimeoutArgumentInput> Input()
            => new long[] { 1, 2, 4 }
                .Select(t => StreamEvent.CreateInterval(t, t + 1, new SessionTimeoutArgumentInput { Key = "a", Value = (int)t }))
                .ToObservable()
                .ToStreamable();

        // Each event's lifetime as start-end, from intervals and end edges, in ordinal order.
        private static string[] Lifetimes(IStreamable<Empty, SessionTimeoutArgumentInput> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsInterval || e.IsEnd)
                .Select(e => e.StartTime + "-" + e.EndTime)
                .ToEnumerable()
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToArray();
    }

    [TestClass]
    public class SessionTimeoutArgumentTestsRow : SessionTimeoutArgumentTestsBase
    {
        public SessionTimeoutArgumentTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class SessionTimeoutArgumentTestsColumnar : SessionTimeoutArgumentTestsBase
    {
        public SessionTimeoutArgumentTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
