// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class SessionOutputOrderInput
    {
        public string Key;
        public int Value;
    }

    public abstract class SessionOutputOrderTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // One-tick events with a timeout of 5 and a maxDuration of 6. a's first session is cut at 12, and b's event
        // at 13 closes it there. b's session, from 13, ends at its timeout, 19. a's event at 14 continues a's session
        // in a new one, cut at 18: earlier than b's end. a's events must end at 18, before b's end at 19.
        private static readonly long[] Times = { 1, 5, 9, 11, 13, 14, 14, 18, 19, 30 };
        private static readonly string[] Keys = { "a", "a", "a", "a", "b", "b", "a", "c", "c", "c" };
        private static readonly string[] Expected =
        {
            "a:1-12", "a:5-12", "a:9-12", "a:11-12", "a:14-18", "b:13-19", "b:14-19", "c:18-24", "c:19-24", "c:30-35",
        };

        // As above, but a's own event at 12 continues its session, cut at 18, which would end at its timeout, 17,
        // before b's. a's event at 14 then moves a's end to its cut point, 18, which is still before b's end at 19.
        private static readonly long[] TimesWhenASessionGrows = { 1, 5, 9, 11, 12, 13, 14, 14, 18, 19, 30 };
        private static readonly string[] KeysWhenASessionGrows = { "a", "a", "a", "a", "a", "b", "b", "a", "c", "c", "c" };
        private static readonly string[] ExpectedWhenASessionGrows =
        {
            "a:1-12", "a:5-12", "a:9-12", "a:11-12", "a:12-18", "a:14-18", "b:13-19", "b:14-19", "c:18-24", "c:19-24", "c:30-35",
        };

        internal SessionOutputOrderTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestMethod, TestCategory("Gated")]
        public void SessionOutputInTimeOrderInsideGroupApply()
        {
            var output = Collect(Input(Times, Keys).GroupApply(e => e.Key, g => g.SessionTimeoutWindow(5, 6), (g, e) => e));
            InTimeOrder(output);
            Same(Expected, Lifetimes(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionOutputInTimeOrderInsideGroupApplyWhenASessionGrows()
        {
            var output = Collect(Input(TimesWhenASessionGrows, KeysWhenASessionGrows).GroupApply(e => e.Key, g => g.SessionTimeoutWindow(5, 6), (g, e) => e));
            InTimeOrder(output);
            Same(ExpectedWhenASessionGrows, Lifetimes(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionOutputInTimeOrderOnAPartitionedStream()
        {
            var output = Partitioned(Times, Keys);
            InTimeOrder(output);
            Same(Expected, Lifetimes(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void SessionOutputInTimeOrderOnAPartitionedStreamWhenASessionGrows()
        {
            var output = Partitioned(TimesWhenASessionGrows, KeysWhenASessionGrows);
            InTimeOrder(output);
            Same(ExpectedWhenASessionGrows, Lifetimes(output));
        }

        // One partition, grouped by key inside it, so the partitioned session pipe sees several keys.
        private static List<Event> Partitioned(long[] times, string[] keys)
            => times
                .Select((t, i) => PartitionedStreamEvent.CreateInterval(0, t, t + 1, new SessionOutputOrderInput { Key = keys[i], Value = (int)t }))
                .ToObservable()
                .ToStreamable()
                .GroupApply(e => e.Key, g => g.SessionTimeoutWindow(5, 6), (g, e) => e)
                .ToStreamEventObservable()
                .ToEnumerable()
                .Select(e => new Event { Sync = e.SyncTime, IsData = e.IsData, IsEnd = e.IsEnd || e.IsInterval, Start = e.StartTime, End = e.EndTime, Key = e.IsData ? e.Payload.Key : null })
                .ToList();

        private sealed class Event
        {
            public long Sync;
            public bool IsData;
            public bool IsEnd;
            public long Start;
            public long End;
            public string Key;
        }

        private static List<Event> Collect(IStreamable<Empty, SessionOutputOrderInput> result)
            => result
                .ToStreamEventObservable()
                .ToEnumerable()
                .Select(e => new Event { Sync = e.SyncTime, IsData = e.IsData, IsEnd = e.IsEnd || e.IsInterval, Start = e.StartTime, End = e.EndTime, Key = e.IsData ? e.Payload.Key : null })
                .ToList();

        // Trill requires each operator's output to be in sync-time order.
        private static void InTimeOrder(List<Event> output)
        {
            for (int i = 1; i < output.Count; i++)
            {
                var message = "sync time goes back from " + output[i - 1].Sync + " to " + output[i].Sync + " at output event " + i + ": "
                    + string.Join(" ", output.Select(e => e.Sync + (e.Key ?? "p")));
                Assert.IsTrue(output[i - 1].Sync <= output[i].Sync, message);
            }
        }

        private static string[] Lifetimes(List<Event> output)
            => output
                .Where(e => e.IsData && e.IsEnd)
                .OrderBy(e => e.Key).ThenBy(e => e.Start)
                .Select(e => e.Key + ":" + e.Start + "-" + e.End)
                .ToArray();

        private static void Same(string[] expected, string[] actual)
            => Assert.AreEqual(string.Join(" ", expected), string.Join(" ", actual));

        private static IStreamable<Empty, SessionOutputOrderInput> Input(long[] times, string[] keys)
            => times
                .Select((t, i) => StreamEvent.CreateInterval(t, t + 1, new SessionOutputOrderInput { Key = keys[i], Value = (int)t }))
                .ToObservable()
                .ToStreamable();
    }

    [TestClass]
    public class SessionOutputOrderTestsRow : SessionOutputOrderTestsBase
    {
        public SessionOutputOrderTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class SessionOutputOrderTestsColumnar : SessionOutputOrderTestsBase
    {
        public SessionOutputOrderTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
