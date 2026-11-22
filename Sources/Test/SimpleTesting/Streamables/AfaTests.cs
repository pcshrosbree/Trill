// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Internal;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [DataContract]
    public class FList<T> : List<T>
    {
        public FList()
            : base()
        { }

        public FList(int capacity)
            : base(capacity)
        { }

        public FList(IEnumerable<T> collection)
            : base(collection)
        { }

        public new FList<T> Add(T t)
        {
            base.Add(t);
            return this;
        }

        public FList<T> Clone() => new FList<T>(this);

        public new FList<T> Clear()
        {
            base.Clear();
            return this;
        }

        public override string ToString() => $"{{ {string.Join(", ", this.Select(e => $"[{e}]"))} }}";
    }

    public struct AfaPayload
    {
        public string Field1;
        public int Field2;

        public override string ToString() => new { this.Field1, this.Field2 }.ToString();
    }

    public class Payload2 : IComparable<Payload2>
    {
        private static long nextTick = 0;

        public long Tick;
        public string Session;
        public long SessionLong;
        public string Field1;
        public bool IsStart = false;
        public bool IsEnd = false;

        public Payload2() => this.Tick = nextTick++;

        public override string ToString() => new { this.Field1 }.ToString();

        public int CompareTo(Payload2 other) => other != null ? this.Tick.CompareTo(other.Tick) : -1;
    }

    public class Register
    {
        public bool IsNegative { get; set; }
        public FList<Payload2> MatchedPayloads { get; set; }

        public static Register AggregateRegister(Register reg, Payload2 payload, bool isNegativeMatch) =>
            new Register
            {
                IsNegative = (reg?.IsNegative ?? default) || isNegativeMatch,
                MatchedPayloads = (reg?.MatchedPayloads.Clone() ?? new FList<Payload2>()).Add(payload)
            };
    }

    public abstract class AfaTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal AfaTests(ConfigModifier modifer) : base(modifer)
        { }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaList01()
        {
            var pat1 = Afa.Create<long>();
            pat1.AddListElementArc(0, 1, fence: (ts, ev, reg) => ev.Contains(0));
            pat1.AddListElementArc(1, 1, fence: (ts, ev, reg) => !ev.Contains(1));
            pat1.AddListElementArc(1, 2, fence: (ts, ev, reg) => ev.Contains(1));

            var result =
                new long[] { 0, 0, 1 }
                .Select((e, i) => StreamEvent.CreatePoint<long>(i, e))
                .ToObservable()
                .ToStreamable()
                .SetProperty().IsSyncTimeSimultaneityFree(true)
                .AlterEventDuration(10)
                .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable();
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(2, 10, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaList02()
        {
            var pat1 = Afa.Create<AfaPayload>();
            pat1.AddListElementArc(0, 1, fence: (ts, events, reg) => events.Any(p => p.Field2 == 0));
            pat1.AddListElementArc(1, 1, fence: (ts, events, reg) => !events.Any(p => p.Field2 == 1));
            pat1.AddListElementArc(1, 2, fence: (ts, events, reg) => events.Any(p => p.Field2 == 1));

            var input = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 0 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "C", Field2 = 0 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 1 }),
            };
            var result = input
                .ToObservable()
                .ToStreamable()
                .SetProperty().IsSyncTimeSimultaneityFree(true)
                .AlterEventDuration(1000)
                .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable();
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(140, 1100, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEvent01()
        {
            var pat1 = new Afa<long, Empty, bool>();
            pat1.AddMultiElementArc(0, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 0), // Any element is 0
                fence: (ts, acc, reg) => acc);

            pat1.AddMultiElementArc(1, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => !acc);

            pat1.AddMultiElementArc(1, 2,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => acc);

            var result =
                new long[] { 0, 0, 1 }
                .Select((e, i) => StreamEvent.CreatePoint<long>(i, e))
                    .ToObservable()
                    .ToStreamable()
                    .SetProperty().IsSyncTimeSimultaneityFree(true)
                    .AlterEventDuration(10)
                    .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ToEnumerable()
                    .ToArray();
            var expected = new StreamEvent<Empty>[]
            {
                StreamEvent.CreateInterval(2, 10, Empty.Default),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEvent02()
        {
            var pat1 = new Afa<long, Empty, bool>();
            pat1.AddMultiElementArc(0, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 0), // Any element is 0
                fence: (ts, acc, reg) => acc);

            pat1.AddMultiElementArc(1, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => !acc);

            pat1.AddMultiElementArc(1, 2,
                    initialize: (ts, reg) => false,
                    accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                    fence: (ts, acc, reg) => acc);

            var result =
                new Tuple<int, int>[] { Tuple.Create(0, 0), Tuple.Create(0, 0), Tuple.Create(1, 1) }
                .Select(e => StreamEvent.CreatePoint<long>(e.Item1, e.Item2))
                    .ToObservable()
                    .ToStreamable()
                    .SetProperty().IsSyncTimeSimultaneityFree(true)
                    .AlterEventDuration(10)
                    .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ToEnumerable();
            var x = result.ToArray();
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(1, 10, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEvent03()
        {
            var pat1 = new Afa<long, Empty, bool>();
            pat1.AddSingleElementArc(0, 1,
                fence: (ts, ev, reg) => ev == 0);

            pat1.AddMultiElementArc(1, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => !acc);

            pat1.AddMultiElementArc(1, 2,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => acc);

            var result =
                new StreamEvent<long>[]
                {
                    StreamEvent.CreateStart<long>(0, 0),
                    StreamEvent.CreateStart<long>(0, 0),
                    StreamEvent.CreateStart<long>(1, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(3, 1),
                    StreamEvent.CreateStart<long>(3, 0),
                }
                    .ToObservable()
                    .ToStreamable()
                    .SetProperty().IsSyncTimeSimultaneityFree(true)
                    .AlterEventDuration(10)
                    .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ToEnumerable()
                    .ToArray();
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(3, 11, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void CoreAfaMultiEvent04()
        {
            var pat1 = new Afa<long, Empty, bool>();
            pat1.AddSingleElementArc(0, 1,
                fence: (ts, ev, reg) => ev == 0);

            pat1.AddMultiElementArc(1, 1,
                initialize: (ts, reg) => false,
                accumulate: (ts, ev, reg, acc) => acc ? acc : (ev == 1), // Any element is 1
                fence: (ts, acc, reg) => !acc);

            pat1.AddListElementArc(1, 2,
                fence: (ts, listev, reg) => listev.Contains(1));

            var result =
                new StreamEvent<long>[]
                {
                    StreamEvent.CreateStart<long>(0, 0),
                    StreamEvent.CreateStart<long>(0, 0),
                    StreamEvent.CreateStart<long>(1, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(2, 0),
                    StreamEvent.CreateStart<long>(3, 1),
                    StreamEvent.CreateStart<long>(3, 0),
                }
                    .ToObservable()
                    .ToStreamable()
                    .SetProperty().IsSyncTimeSimultaneityFree(true)
                    .AlterEventDuration(10)
                    .Detect(pat1, isDeterministic: true, allowOverlappingInstances: false)
                    .ToStreamEventObservable()
                    .Where(e => e.IsData)
                    .ToEnumerable()
                    .ToArray();
            var expected = new StreamEvent<Empty>[] { StreamEvent.CreateInterval(3, 11, Empty.Default) };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePattern01()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "C", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "C", Field2 = 7 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .Detect(
                    0,
                    p => p
                        .SingleElement(e => e.Field1 == "A")
                        .KleeneStar(r => r.SingleElement(e => e.Field1 == "B", (ev, d) => d + ev.Field2))
                        .SingleElement(e => e.Field1 == "C"))
                        .ToStreamEventObservable()
                        .Where(e => e.IsData)
                        .ToEnumerable()
                        .ToArray();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(110, 1100, 0),
                StreamEvent.CreateInterval(150, 1120, 14),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePattern02()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "C", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "C", Field2 = 7 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .DefinePattern(0)
                .SingleElement(e => e.Field1 == "A")
                .KleeneStar(r => r.SingleElement(e => e.Field1 == "B", (ev, d) => d + ev.Field2))
                .SingleElement(e => e.Field1 == "C")
                .Detect()
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable()
                .ToArray();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(110, 1100, 0),
                StreamEvent.CreateInterval(150, 1120, 14),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaDefinePattern03()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "C", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "C", Field2 = 7 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .DefinePattern(10)
                .SingleElement(e => e.Field1 == "A", (l, p, i) => i + p.Field2)
                .SingleElement(e => e.Field1 == "C", (l, p, i) => i + p.Field2)
                .Detect()
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable()
                .ToArray();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(110, 1100, 17),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaPatternAiBi01()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "A", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "B", Field2 = 9 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .Detect(0, p => p
                    .KleeneStar(x => x.SingleElement((ts, e, r) => e.Field1 == "A", (ev, r) => r + 1))
                    .KleeneStar(x => x.SingleElement((ts, e, r) => e.Field1 == "B" && r > 1, (ev, r) => r - 1))
                    .SingleElement((ts, e, r) => e.Field1 == "B" && r == 1))
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(130, 1120, 1),
                StreamEvent.CreateInterval(140, 1110, 1),
                StreamEvent.CreateInterval(150, 1100, 1),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaPatternAiBi02()
        {
            var source1 = new StreamEvent<AfaPayload>[]
            {
                StreamEvent.CreatePoint(100, new AfaPayload { Field1 = "A", Field2 = 4 }),
                StreamEvent.CreatePoint(110, new AfaPayload { Field1 = "A", Field2 = 3 }),
                StreamEvent.CreatePoint(120, new AfaPayload { Field1 = "A", Field2 = 1 }),
                StreamEvent.CreatePoint(130, new AfaPayload { Field1 = "B", Field2 = 6 }),
                StreamEvent.CreatePoint(140, new AfaPayload { Field1 = "B", Field2 = 8 }),
                StreamEvent.CreatePoint(150, new AfaPayload { Field1 = "B", Field2 = 9 }),
                StreamEvent.CreatePoint(160, new AfaPayload { Field1 = "B", Field2 = 9 }),
            }.ToObservable().ToStreamable().AlterEventDuration(1000);
            var result =
                source1
                .Detect(0, p => p
                    .KleeneStar(x => x.SingleElement((ts, e, r) => e.Field1 == "A", (ev, r) => r + 1))
                    .KleeneStar(x => x.SingleElement((ts, e, r) => e.Field1 == "B" && r > 1, (ev, r) => r - 1))
                    .SingleElement((ts, e, r) => e.Field1 == "B" && r == 1),
                    allowOverlappingInstances: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateInterval(150, 1100, 1),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void DAfa01()
        {
            var count = 0;
            var source = new List<string>() { "A", "B", "C", "A", "A", "A", "B" }
                .Select(a => new Payload2() { Session = "1", Field1 = a, Tick = count++ })
                .OrderBy(e => e.Tick)
                .ToList()
                .Select(e => StreamEvent.CreateStart(e.Tick, e))
                .ToObservable().ToStreamable(DisorderPolicy.Drop())
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var pattern3 = ARegex.SingleElement<Payload2, Register>(
                (t, ev, reg) => ev.Field1 == "C",
                (t, ev, reg) => reg);
            var registers =
                source
                .Detect(pattern3, StreamEvent.InfinitySyncTime, false, false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .Select(evt => evt.Payload)
                .ToEnumerable()
                .ToList();
            Assert.IsTrue(registers.Count == 1 && registers[0] == null); // really just want to test that code gen didn't fail.
        }

        [TestMethod, TestCategory("Gated")]
        public void DAfa02()
        {
            var count = 0;
            var source = new List<string>() { "A", "B", "C", "A", "A", "A", "B" }
                .Select(a => new Payload2() { Session = "1", Field1 = a, Tick = count++ })
                .OrderBy(e => e.Tick)
                .Select(e => StreamEvent.CreateStart(e.Tick, e))
                .ToList()
                .ToObservable().ToStreamable(DisorderPolicy.Drop())
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);
            var pattern3 = ARegex.SingleElement<Payload2, Register>(
                (t, ev, reg) => ev.Field1 == "C",
                (t, ev, reg) => Register.AggregateRegister(reg, ev, false));
            var registers =
                source
                .Detect(pattern3, StreamEvent.InfinitySyncTime, false, false)
                .Where(register => register.MatchedPayloads.Count > 0)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .Select(evt => evt.Payload)
                .ToEnumerable()
                .ToList();
            Assert.IsTrue(registers.Count == 1 && registers[0].MatchedPayloads.Count == 1);
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaZeroOrOne()
        {
            var source = new StreamEvent<string>[]
            {
                StreamEvent.CreateStart(0, "A"),
                StreamEvent.CreateStart(1, "C"),
                StreamEvent.CreateStart(2, "B"),
                StreamEvent.CreateStart(3, "B"),
                StreamEvent.CreateStart(4, "C"),
                StreamEvent.CreateStart(5, "A"),
                StreamEvent.CreateStart(6, "B"),
                StreamEvent.CreateStart(7, "B"),
                StreamEvent.CreateStart(8, "B"),
                StreamEvent.CreateStart(9, "A"),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(2);

            var afa = ARegex.Concat(
                ARegex.SingleElement<string, string>(
                    (time, @event, state) => @event == "A",
                    (time, @event, state) => @event),
                ARegex.ZeroOrOne(
                    ARegex.SingleElement<string, string>(
                        (time, @event, state) => @event == "B",
                        (time, @event, state) => state + @event)));

            var result = source
                .Detect(
                    afa,
                    allowOverlappingInstances: false,
                    isDeterministic: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(0, 2, "A"),
                StreamEvent.CreateInterval(5, 7, "A"),
                StreamEvent.CreateInterval(6, 7, "AB"),
                StreamEvent.CreateInterval(9, 11, "A"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaZeroOrOneInsideOr()
        {
            var source = new StreamEvent<string>[]
            {
                StreamEvent.CreateStart(0, "A"),
                StreamEvent.CreateStart(1, "C"),
                StreamEvent.CreateStart(2, "B"),
                StreamEvent.CreateStart(3, "B"),
                StreamEvent.CreateStart(4, "C"),
                StreamEvent.CreateStart(5, "A"),
                StreamEvent.CreateStart(6, "B"),
                StreamEvent.CreateStart(7, "B"),
                StreamEvent.CreateStart(8, "B"),
                StreamEvent.CreateStart(9, "A"),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(3);

            // Create Regex for "A (C? | BB)
            var afa = ARegex.Concat(
                ARegex.SingleElement<string, string>(
                    (time, @event, state) => @event == "A",
                    (time, @event, state) => @event),
                ARegex.Or(
                    ARegex.ZeroOrOne(
                        ARegex.SingleElement<string, string>(
                            (time, @event, state) => @event == "C",
                            (time, @event, state) => state + @event)),
                    ARegex.Concat(
                        ARegex.SingleElement<string, string>(
                            (time, @event, state) => @event == "B",
                            (time, @event, state) => state + @event),
                        ARegex.SingleElement<string, string>(
                            (time, @event, state) => @event == "B",
                            (time, @event, state) => state + @event))));

            var result = source
                .Detect(
                    afa,
                    allowOverlappingInstances: false,
                    isDeterministic: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(0, 3, "A"),
                StreamEvent.CreateInterval(1, 3, "AC"),
                StreamEvent.CreateInterval(5, 8, "A"),
                StreamEvent.CreateInterval(7, 8, "ABB"),
                StreamEvent.CreateInterval(9, 12, "A"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaZeroOrOneGroup()
        {
            var source = new StreamEvent<string>[]
            {
                StreamEvent.CreateStart(0, "O"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(2, "A"),
                StreamEvent.CreateStart(3, "F"),
                StreamEvent.CreateStart(4, "O"),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(10);

            // O(A*F)?
            var afa = ARegex.Concat(
                        ARegex.SingleElement<string, string>(
                            (time, @event, state) => @event == "O",
                            (time, @event, state) => state + @event),
                        ARegex.ZeroOrOne(
                            ARegex.Concat(
                                ARegex.KleeneStar(
                                    ARegex.SingleElement<string, string>(
                                        (time, @event, state) => @event == "A",
                                        (time, @event, state) => state + @event)),
                                ARegex.SingleElement<string, string>(
                                    (time, @event, state) => @event == "F",
                                    (time, @event, state) => state + @event))));

            var result = source
                .Detect(
                    afa,
                    allowOverlappingInstances: false,
                    isDeterministic: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(0, 10, "O"),
                StreamEvent.CreateInterval(3, 10, "OAAF"),
                StreamEvent.CreateInterval(4, 14, "O"),
            };

            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaPunctuationAtBatchBoundary()
        {
            using (new ConfigModifier().DataBatchSize(3).Modify())
            {
                // Add some columns to the pool so we don't just keep reusing the same one
                var longPool = MemoryManager.GetColumnPool<long>();
                var columns = new ColumnBatch<long>[100];
                for (int i = 0; i < columns.Length; i++)
                {
                    Assert.IsTrue(longPool.Get(out columns[i]));
                }
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].Return();
                }

                var source = new StreamEvent<string>[]
                {
                    StreamEvent.CreateStart(0, "A"),
                    StreamEvent.CreateStart(1, "B"), // Match
                    StreamEvent.CreateStart(2, "C"),

                    StreamEvent.CreateStart(3, "A"),
                    StreamEvent.CreateStart(4, "B"), // Match
                    StreamEvent.CreateStart(5, "C"),

                    StreamEvent.CreateStart(6, "A"),
                    StreamEvent.CreatePunctuation<string>(6), // End of output batch, middle of input batch
                    StreamEvent.CreateStart(7, "B"),

                    StreamEvent.CreateStart(8, "C"),
                }.ToObservable()
                    .ToStreamable(flushPolicy: FlushPolicy.None)
                    .AlterEventDuration(3);

                // Create Regex for "ABC"
                var afa = ARegex.Concat(
                    ARegex.SingleElement<string, string>(
                        (time, @event, state) => @event == "A",
                        (time, @event, state) => state + @event),
                    ARegex.SingleElement<string, string>(
                        (time, @event, state) => @event == "B",
                        (time, @event, state) => state + @event));
                var afaStreamable = source.Detect(
                        afa,
                        allowOverlappingInstances: false,
                        isDeterministic: false);
                afaStreamable.Properties.IsSyncTimeSimultaneityFree = true;

                var result = afaStreamable
                    .ToStreamEventObservable()
                    .ToEnumerable()
                    .ToArray();
                var expected = new StreamEvent<string>[]
                {
                    StreamEvent.CreateInterval(1, 3, "AB"),
                    StreamEvent.CreateInterval(4, 6, "AB"),
                    StreamEvent.CreatePunctuation<string>(6),
                    StreamEvent.CreateInterval(7, 9, "AB"),
                    StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime),
                };
                Assert.IsTrue(result.SequenceEqual(expected));

                longPool.Free();
            }
        }
    }

    /// <summary>
    /// Since columnar is not supported for partitioned queries, put partitioned test cases here.
    /// </summary>
    [TestClass]
    public class AfaTestsWithoutCodegen : AfaTests
    {
        public AfaTestsWithoutCodegen() : base(new ConfigModifier().CodeGenAfa(false))
        { }

        [TestMethod, TestCategory("Gated")]
        public void DAfa_IsSyncTimeSimultaneityFree()
        {
            var source = new StreamEvent<string>[]
            {
                StreamEvent.CreateStart(0, "A"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(1, "B"),
                StreamEvent.CreateStart(3, "B"),
                StreamEvent.CreateStart(4, "C"),
                StreamEvent.CreateStart(5, "A"),
                StreamEvent.CreateStart(5, "B"),
                StreamEvent.CreateStart(5, "B"),
                StreamEvent.CreateStart(5, "B"),
                StreamEvent.CreateStart(9, "A"),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(2);

            // Pattern: A B
            var afa = ARegex.Concat(
                ARegex.SingleElement<string, string>(
                    (time, @event, state) => @event == "A",
                    (time, @event, state) => state + @event),
                ARegex.SingleElement<string, string>(
                    (time, @event, state) => @event == "B",
                    (time, @event, state) => state + @event));

            var afa_compiled = source
                .Detect(afa);
            afa_compiled.Properties.IsSyncTimeSimultaneityFree = true;

            var result = afa_compiled
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(1, 3, "AB"),
                StreamEvent.CreateInterval(5, 7, "AB"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void Afa_IsSyncTimeSimultaneityFree()
        {
            var source = new StreamEvent<string>[]
            {
                StreamEvent.CreateStart(0, "A"),
                StreamEvent.CreateStart(1, "A"),
                StreamEvent.CreateStart(1, "B"),
                StreamEvent.CreateStart(3, "B"),
                StreamEvent.CreateStart(4, "C"),
                StreamEvent.CreateStart(5, "A"),
                StreamEvent.CreateStart(5, "B"),
                StreamEvent.CreateStart(5, "B"),
                StreamEvent.CreateStart(5, "B"),
                StreamEvent.CreateStart(9, "A"),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(5);

            // Pattern: A+ B+
            var afa = ARegex.Concat(
                ARegex.KleenePlus(
                    ARegex.SingleElement<string, string>(
                        (time, @event, state) => @event == "A",
                        (time, @event, state) => state + @event)),
                ARegex.KleenePlus(
                    ARegex.SingleElement<string, string>(
                        (time, @event, state) => @event == "B",
                        (time, @event, state) => state + @event)));

            var afa_compiled = source
                .Detect(afa);
            afa_compiled.Properties.IsSyncTimeSimultaneityFree = true;

            var result = afa_compiled
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<string>[]
            {
                StreamEvent.CreateInterval(1, 6, "AB"),
                StreamEvent.CreateInterval(1, 5, "AAB"),
                StreamEvent.CreateInterval(3, 6, "ABB"),
                StreamEvent.CreateInterval(3, 5, "AABB"),
                StreamEvent.CreateInterval(5, 10, "AB"),
                StreamEvent.CreateInterval(5, 10, "ABB"),
                StreamEvent.CreateInterval(5, 10, "ABBB"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupedAfa()
        {
            var source = new StreamEvent<Tuple<string, int>>[]
            {
                StreamEvent.CreateStart(0, new Tuple<string, int>("A", 1)),
                    StreamEvent.CreateStart(1, new Tuple<string, int>("A", 2)),
                    StreamEvent.CreateStart(2, new Tuple<string, int>("B", 2)),
                StreamEvent.CreateStart(3, new Tuple<string, int>("A", 1)),
                StreamEvent.CreateStart(4, new Tuple<string, int>("B", 1)),
                    StreamEvent.CreateStart(5, new Tuple<string, int>("B", 2)),
                StreamEvent.CreateStart(6, new Tuple<string, int>("B", 1)),
                StreamEvent.CreateStart(7, new Tuple<string, int>("C", 1)),
                    StreamEvent.CreateStart(8, new Tuple<string, int>("B", 2)),
                    StreamEvent.CreateStart(9, new Tuple<string, int>("A", 2)),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(7);

            // Pattern: A+ B+
            var afa = ARegex.Concat(
                ARegex.KleenePlus(
                    ARegex.SingleElement<Tuple<string, int>, Tuple<string, int>>(
                        (time, @event, state) => @event.Item1 == "A",
                        (time, @event, state) => new Tuple<string, int>(state == null ? @event.Item1 : state.Item1 + @event.Item1, @event.Item2))),
                ARegex.KleenePlus(
                    ARegex.SingleElement<Tuple<string, int>, Tuple<string, int>>(
                        (time, @event, state) => @event.Item1 == "B",
                        (time, @event, state) => new Tuple<string, int>(state == null ? @event.Item1 : state.Item1 + @event.Item1, @event.Item2))));

            var result = source
                .GroupApply((input) => input.Item2, group => group.Detect(afa, maxDuration: 7), (group, bind) => bind)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<Tuple<string, int>>[]
            {
                StreamEvent.CreateInterval(2, 8, new Tuple<string, int>("AB", 2)),
                StreamEvent.CreateInterval(4, 10, new Tuple<string, int>("AB", 1)),
                StreamEvent.CreateInterval(4, 7, new Tuple<string, int>("AAB", 1)),
                StreamEvent.CreateInterval(5, 8, new Tuple<string, int>("ABB", 2)),
                StreamEvent.CreateInterval(6, 10, new Tuple<string, int>("ABB", 1)),
                StreamEvent.CreateInterval(6, 7, new Tuple<string, int>("AABB", 1)),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupedAfa_IsSyncTimeSimultaneityFree()
        {
            var source = new StreamEvent<Tuple<string, int>>[]
            {
                StreamEvent.CreateStart(0, new Tuple<string, int>("A", 1)),
                    StreamEvent.CreateStart(1, new Tuple<string, int>("A", 2)),
                    StreamEvent.CreateStart(1, new Tuple<string, int>("B", 2)),
                StreamEvent.CreateStart(3, new Tuple<string, int>("A", 1)),

                StreamEvent.CreatePunctuation<Tuple<string, int>>(4),

                StreamEvent.CreateStart(4, new Tuple<string, int>("B", 1)),
                    StreamEvent.CreateStart(4, new Tuple<string, int>("B", 2)),
                StreamEvent.CreateStart(5, new Tuple<string, int>("B", 1)),
                StreamEvent.CreateStart(5, new Tuple<string, int>("C", 1)),
                    StreamEvent.CreateStart(6, new Tuple<string, int>("B", 2)),
                    StreamEvent.CreateStart(7, new Tuple<string, int>("A", 2)),

                StreamEvent.CreatePunctuation<Tuple<string, int>>(7),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(10);

            // Pattern: A+ B+
            var afa = ARegex.Concat(
                ARegex.KleenePlus(
                    ARegex.SingleElement<Tuple<string, int>, Tuple<string, int>>(
                        (time, @event, state) => @event.Item1 == "A",
                        (time, @event, state) => new Tuple<string, int>(state == null ? @event.Item1 : state.Item1 + @event.Item1, @event.Item2))),
                ARegex.KleenePlus(
                    ARegex.SingleElement<Tuple<string, int>, Tuple<string, int>>(
                        (time, @event, state) => @event.Item1 == "B",
                        (time, @event, state) => new Tuple<string, int>(state == null ? @event.Item1 : state.Item1 + @event.Item1, @event.Item2))));

            var afa_compiled = source.GroupApply(
                (input) => input.Item2,
                group =>
                {
                    var afaGroup = group.Detect(afa, maxDuration: 10);
                    afaGroup.Properties.IsSyncTimeSimultaneityFree = true;
                    return afaGroup;
                },
                (group, bind) => bind);

            var result = afa_compiled
                .ToStreamEventObservable()
                .ToEnumerable()
                .ToArray();
            var expected = new StreamEvent<Tuple<string, int>>[]
            {
                StreamEvent.CreateInterval(1, 11, new Tuple<string, int>("AB", 2)),
                StreamEvent.CreatePunctuation<Tuple<string, int>>(4),
                StreamEvent.CreateInterval(4, 13, new Tuple<string, int>("AB", 1)),
                StreamEvent.CreateInterval(4, 10, new Tuple<string, int>("AAB", 1)),
                StreamEvent.CreateInterval(4, 11, new Tuple<string, int>("ABB", 2)),
                StreamEvent.CreateInterval(5, 13, new Tuple<string, int>("ABB", 1)),
                StreamEvent.CreateInterval(5, 10, new Tuple<string, int>("AABB", 1)),
                StreamEvent.CreateInterval(6, 11, new Tuple<string, int>("ABBB", 2)),
                StreamEvent.CreatePunctuation<Tuple<string, int>>(7),

                StreamEvent.CreatePunctuation<Tuple<string, int>>(StreamEvent.InfinitySyncTime),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedAfa()
        {
            var source = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateStart(1, 0, "A"),
                            PartitionedStreamEvent.CreateStart(2, 0, "A"),
                PartitionedStreamEvent.CreateStart(1, 1, "B"),
                PartitionedStreamEvent.CreateStart(1, 2, "B"),
                            PartitionedStreamEvent.CreateStart(2, 2, "B"),
                PartitionedStreamEvent.CreateStart(1, 3, "A"),
                PartitionedStreamEvent.CreateStart(1, 4, "C"),
                            PartitionedStreamEvent.CreateStart(2, 3, "C"),
                            PartitionedStreamEvent.CreateStart(2, 4, "A"),
                PartitionedStreamEvent.CreateStart(1, 5, "A"),
                PartitionedStreamEvent.CreateStart(1, 6, "B"),
                            PartitionedStreamEvent.CreateStart(2, 5, "B"),
                PartitionedStreamEvent.CreateStart(1, 7, "B"),
                            PartitionedStreamEvent.CreateStart(2, 6, "B"),
                PartitionedStreamEvent.CreateStart(1, 8, "B"),
                PartitionedStreamEvent.CreateStart(1, 9, "A"),
                            PartitionedStreamEvent.CreateStart(2, 7, "A"),
                            PartitionedStreamEvent.CreateStart(2, 8, "B")
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(7);

            // Pattern: A B
            var afa = ARegex.Concat(
                            ARegex.SingleElement<string, string>(
                                (time, @event, state) => @event == "A",
                                (time, @event, state) => state + @event),
                            ARegex.SingleElement<string, string>(
                                (time, @event, state) => @event == "B",
                                (time, @event, state) => state + @event));

            var result = source
                .Detect(
                    afa,
                    maxDuration: 7,
                    allowOverlappingInstances: false,
                    isDeterministic: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateInterval(1, 1, 7, "AB"),
                PartitionedStreamEvent.CreateInterval(2, 2, 7, "AB"),
                PartitionedStreamEvent.CreateInterval(1, 6, 12, "AB"),
                PartitionedStreamEvent.CreateInterval(2, 5, 11, "AB"),
                PartitionedStreamEvent.CreateInterval(2, 8, 14, "AB"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedAfa_IsSyncTimeSimultaneityFree()
        {
            var source = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateStart(1, 0, "A"),
                            PartitionedStreamEvent.CreateStart(2, 0, "A"),
                PartitionedStreamEvent.CreateStart(1, 1, "B"),
                PartitionedStreamEvent.CreateStart(1, 1, "B"),
                            PartitionedStreamEvent.CreateStart(2, 0, "B"),
                PartitionedStreamEvent.CreateStart(1, 3, "A"),
                PartitionedStreamEvent.CreateStart(1, 4, "C"),
                            PartitionedStreamEvent.CreateStart(2, 3, "C"),
                            PartitionedStreamEvent.CreateStart(2, 4, "A"),
                PartitionedStreamEvent.CreateStart(1, 5, "A"),
                PartitionedStreamEvent.CreateStart(1, 6, "B"),
                            PartitionedStreamEvent.CreateStart(2, 4, "B"),
                PartitionedStreamEvent.CreateStart(1, 7, "B"),
                            PartitionedStreamEvent.CreateStart(2, 6, "B"),
                PartitionedStreamEvent.CreateStart(1, 8, "B"),
                PartitionedStreamEvent.CreateStart(1, 9, "A"),
                            PartitionedStreamEvent.CreateStart(2, 7, "A"),
                            PartitionedStreamEvent.CreateStart(2, 8, "B")
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(7);

            // Pattern: A B
            var afa = ARegex.Concat(
                            ARegex.SingleElement<string, string>(
                                (time, @event, state) => @event == "A",
                                (time, @event, state) => state + @event),
                            ARegex.SingleElement<string, string>(
                                (time, @event, state) => @event == "B",
                                (time, @event, state) => state + @event));

            var afa_compiled = source
                .Detect(afa, maxDuration: 7);

            afa_compiled.Properties.IsSyncTimeSimultaneityFree = true;

            var result = afa_compiled
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateInterval(1, 1, 7, "AB"),
                PartitionedStreamEvent.CreateInterval(2, 0, 7, "AB"),
                PartitionedStreamEvent.CreateInterval(1, 6, 12, "AB"),
                PartitionedStreamEvent.CreateInterval(2, 4, 11, "AB"),
                PartitionedStreamEvent.CreateInterval(2, 8, 14, "AB"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedAfa_LowWatermarkAboveMaxDuration_Emit()
        {
            var source = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateStart(1, 0, "A"),
                PartitionedStreamEvent.CreateStart(1, 1, "B"),
                PartitionedStreamEvent.CreateStart(1, 2, "B"),
                PartitionedStreamEvent.CreateStart(1, 3, "B"),
                PartitionedStreamEvent.CreateLowWatermark<int, string>(9),
                PartitionedStreamEvent.CreateStart(1, 10, "B")
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(4);

            // Pattern: A B+
            var afa = ARegex.Concat(
                            ARegex.SingleElement<string, string>(
                                (time, @event, state) => @event == "A",
                                (time, @event, state) => state + @event),
                            ARegex.KleenePlus(
                                ARegex.SingleElement<string, string>(
                                    (time, @event, state) => @event == "B",
                                    (time, @event, state) => state + @event)));

            var result = source
                .Detect(afa, maxDuration: 4)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateInterval(1, 1, 4, "AB"),
                PartitionedStreamEvent.CreateInterval(1, 2, 4, "ABB"),
                PartitionedStreamEvent.CreateInterval(1, 3, 4, "ABBB"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedAfa_LowWatermarkBelowMaxDuration_PunctuationEmit()
        {
            var source = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateStart(1, 0, "A"),
                PartitionedStreamEvent.CreateStart(1, 1, "B"),
                PartitionedStreamEvent.CreateStart(1, 2, "B"),
                PartitionedStreamEvent.CreateStart(1, 3, "B"),
                PartitionedStreamEvent.CreateLowWatermark<int, string>(4),
                PartitionedStreamEvent.CreateStart(1, 5, "B"),
                PartitionedStreamEvent.CreateStart(1, 11, "A"),
                PartitionedStreamEvent.CreateStart(1, 12, "B"),
                PartitionedStreamEvent.CreateStart(1, 13, "B"),
                PartitionedStreamEvent.CreatePunctuation<int, string>(1, 15),
            }.ToObservable()
                .ToStreamable()
                .AlterEventDuration(7);

            // Pattern: A B+
            var afa = ARegex.Concat(
                            ARegex.SingleElement<string, string>(
                                (time, @event, state) => @event == "A",
                                (time, @event, state) => state + @event),
                            ARegex.KleenePlus(
                                ARegex.SingleElement<string, string>(
                                    (time, @event, state) => @event == "B",
                                    (time, @event, state) => state + @event)));

            var result = source
                .Detect(afa, maxDuration: 7)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();
            var expected = new PartitionedStreamEvent<int, string>[]
            {
                PartitionedStreamEvent.CreateInterval(1, 1, 7, "AB"),
                PartitionedStreamEvent.CreateInterval(1, 2, 7, "ABB"),
                PartitionedStreamEvent.CreateInterval(1, 3, 7, "ABBB"),
                PartitionedStreamEvent.CreateInterval(1, 5, 7, "ABBBB"),
                PartitionedStreamEvent.CreateInterval(1, 12, 18, "AB"),
                PartitionedStreamEvent.CreateInterval(1, 13, 18, "ABB"),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        internal class State
        {
            // This cannot be an enum because we want represent a concatinated state in terms of digits in int value
            public const int A = 1;
            public const int B = 2;
            public const int C = 3;
        }

        [TestMethod, TestCategory("Gated")]
        public void AfaZeroOrOneWithStartEvent()
        {
            var source = new StreamEvent<int>[]
            {
                StreamEvent.CreateStart(0, State.A),
                StreamEvent.CreateStart(1, State.C),
                StreamEvent.CreateStart(2, State.B),
                StreamEvent.CreateStart(3, State.B),
                StreamEvent.CreateStart(4, State.C),
                StreamEvent.CreateStart(5, State.A),
                StreamEvent.CreateStart(6, State.B),
                StreamEvent.CreateStart(7, State.B),
                StreamEvent.CreateStart(8, State.B),
                StreamEvent.CreateStart(9, State.A),
            }.ToObservable()
                .ToStreamable()
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime);

            // Assert we are actually testing columnar
            Assert.IsTrue(source.Properties.IsColumnar);

            var afa = ARegex.Concat(
                ARegex.SingleElement<int, int>(
                    (time, @event, state) => @event == State.A,
                    (time, @event, state) => @event),
                ARegex.ZeroOrOne(
                    ARegex.SingleElement<int, int>(
                        (time, @event, state) => @event == State.B,
                        (time, @event, state) => state * 10 + @event)));

            var result = source
                .Detect(
                    afa,
                    allowOverlappingInstances: false,
                    isDeterministic: false)
                .ToStreamEventObservable()
                .Where(evt => evt.IsData)
                .ToEnumerable()
                .ToArray();

            var expected = new StreamEvent<int>[]
            {
                StreamEvent.CreateStart(0, State.A),
                StreamEvent.CreateStart(5, State.A),
                StreamEvent.CreateStart(6, State.A * 10 + State.B),
                StreamEvent.CreateStart(9, State.A),
            };
            Assert.IsTrue(result.SequenceEqual(expected));
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedAfaPunctuationAtBatchBoundary() => PartitionedAfaBatchBoundary(isLowWatermark: false);

        [TestMethod, TestCategory("Gated")]
        public void PartitionedAfaLowWatermarkAtBatchBoundary() => PartitionedAfaBatchBoundary(isLowWatermark: true);

        private void PartitionedAfaBatchBoundary(bool isLowWatermark)
        {
            using (new ConfigModifier().DataBatchSize(3).Modify())
            {
                // Add some columns to the pool so we don't just keep reusing the same one
                var longPool = MemoryManager.GetColumnPool<long>();
                var columns = new ColumnBatch<long>[100];
                for (int i = 0; i < columns.Length; i++)
                {
                    Assert.IsTrue(longPool.Get(out columns[i]));
                }
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].Return();
                }

                var source = new PartitionedStreamEvent<int, string>[]
                {
                    PartitionedStreamEvent.CreateStart(0, 0, "A"),
                    PartitionedStreamEvent.CreateStart(0, 1, "B"), // Match
                    PartitionedStreamEvent.CreateStart(0, 2, "C"),

                    PartitionedStreamEvent.CreateStart(0, 3, "A"),
                    PartitionedStreamEvent.CreateStart(0, 4, "B"), // Match
                    PartitionedStreamEvent.CreateStart(0, 5, "C"),

                    PartitionedStreamEvent.CreateStart(0, 6, "A"),
                    isLowWatermark ? PartitionedStreamEvent.CreateLowWatermark<int, string>(6) : PartitionedStreamEvent.CreatePunctuation<int, string>(0, 6), // End of output batch, middle of input batch
                    PartitionedStreamEvent.CreateStart(0, 7, "B"),

                    PartitionedStreamEvent.CreateStart(0, 8, "C"),
                }.ToObservable()
                    .ToStreamable(flushPolicy: PartitionedFlushPolicy.None)
                    .AlterEventDuration(3);

                // Create Regex for "ABC"
                var afa = ARegex.Concat(
                    ARegex.SingleElement<string, string>(
                        (time, @event, state) => @event == "A",
                        (time, @event, state) => state + @event),
                    ARegex.SingleElement<string, string>(
                        (time, @event, state) => @event == "B",
                        (time, @event, state) => state + @event));
                var afaStreamable = source.Detect(
                        afa,
                        allowOverlappingInstances: false,
                        isDeterministic: false);
                afaStreamable.Properties.IsSyncTimeSimultaneityFree = true;

                var result = afaStreamable
                    .ToStreamEventObservable()
                    .ToEnumerable()
                    .ToArray();
                var expected = new PartitionedStreamEvent<int, string>[]
                {
                    PartitionedStreamEvent.CreateInterval(0, 1, 3, "AB"),
                    PartitionedStreamEvent.CreateInterval(0, 4, 6, "AB"),
                    isLowWatermark ? PartitionedStreamEvent.CreateLowWatermark<int, string>(6) : PartitionedStreamEvent.CreatePunctuation<int, string>(0, 6),
                    PartitionedStreamEvent.CreateInterval(0, 7, 9, "AB"),
                    PartitionedStreamEvent.CreateLowWatermark<int, string>(StreamEvent.InfinitySyncTime),
                };
                Assert.IsTrue(result.SequenceEqual(expected));

                longPool.Free();
            }
        }
    }

    [TestClass]
    public class AfaTestsWithCodegen : AfaTests
    {
        public AfaTestsWithCodegen() : base(new ConfigModifier()
            .CodeGenAfa(true)
            .ForceRowBasedExecution(false)
            .DontFallBackToRowBasedExecution(true))
        { }
    }

    // Afa.Clone() must give an automaton that matches like the original (fork issue #124). Each automaton here has a
    // multi-element arc without an initializer, so each match starts from the automaton's default accumulator.
    // Code generation cannot write an arc without an initializer, so in columnar mode the query falls back to row
    // execution. With fallback disabled it cannot be built, so there is no strict columnar class.
    public abstract class AfaCloneTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal AfaCloneTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(false)) { }

        private static IStreamable<Empty, int> Input(params int[] payloads)
            => payloads.Select((p, i) => StreamEvent.CreatePoint(i + 1, p)).ToObservable().ToStreamable();

        private static string Registers(IStreamable<Empty, int> output)
            => string.Join(", ", output.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload));

        // The automaton from the issue's repro.
        private static Afa<int, int, int> AddsToTen()
        {
            var afa = new Afa<int, int, int>(defaultRegister: 0, defaultAccumulator: 10);
            afa.AddMultiElementArc(0, 1,
                initialize: null,
                accumulate: (t, p, reg, acc) => acc + p,
                fence: (t, acc, reg) => true,
                transfer: (t, acc, reg) => acc);
            afa.AddFinalState(1);
            return afa;
        }

        // The issue's repro: one event with payload 1 gives the register 10 + 1 = 11. The original is the control.
        [TestMethod, TestCategory("Gated")]
        public void CloneKeepsDefaultAccumulator()
        {
            var afa = AddsToTen();
            Assert.AreEqual("11", Registers(Input(1).Detect(afa, maxDuration: 10)));
            Assert.AreEqual("11", Registers(Input(1).Detect(afa.Clone(), maxDuration: 10)));
        }

        // The pattern API clones each pattern it builds, in PatternMatcher's Concat.
        [TestMethod, TestCategory("Gated")]
        public void FluentPatternKeepsDefaultAccumulator()
        {
            var output = Input(1)
                .DefinePattern(0)
                .SetAccumulator(10)
                .MultiElement(null, (t, p, reg, acc) => acc + p, null, (t, acc, reg) => true, (t, acc, reg) => acc, null)
                .Detect(maxDuration: 10);
            Assert.AreEqual("11", Registers(output));
        }

        // ARegex.Concat clones its first pattern. Payload 1 at time 1 gives the register 10 + 1 = 11. Then the second
        // pattern's arc, which has an initializer of its own, adds payload 2 at time 2.
        [TestMethod, TestCategory("Gated")]
        public void RegexConcatKeepsDefaultAccumulator()
        {
            var second = new Afa<int, int, int>();
            second.AddMultiElementArc(0, 1,
                initialize: (t, reg) => 0,
                accumulate: (t, p, reg, acc) => acc + p,
                fence: (t, acc, reg) => true,
                transfer: (t, acc, reg) => reg + acc);
            second.AddFinalState(1);
            Assert.AreEqual("13", Registers(Input(1, 2).Detect(ARegex.Concat(AddsToTen(), second), maxDuration: 10)));
        }
    }

    [TestClass]
    public class AfaCloneTestsRow : AfaCloneTestsBase
    {
        public AfaCloneTestsRow() : base(false) { }
    }

    [TestClass]
    public class AfaCloneTestsColumnar : AfaCloneTestsBase
    {
        public AfaCloneTestsColumnar() : base(true) { }
    }

    // Clone() must copy every field of Afa<,,>. CloneCopiesEveryField fails when a field is added that Clone() does
    // not copy, or that Original() leaves at its default value, where copying it cannot be told from not copying it.
    [TestClass]
    public class AfaCloneFieldTests
    {
        private const BindingFlags InstanceFields = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        // The fields that Clone() does not copy by assignment, with the reason for each.
        private static readonly Dictionary<string, string> NotAssigned = new Dictionary<string, string>
        {
            { "isSealed", "A clone starts unsealed: Concat clones a sealed pattern and then adds arcs, which a sealed automaton refuses." },
            { "finalStates", "Clone() adds each final state to the clone's own list: see CloneHasTheSameFinalStatesInItsOwnList." },
            { "transitionInfo", "Clone() adds each arc to the clone's own dictionaries: see CloneHasTheSameArcsInItsOwnDictionaries." },
        };

        // Every field differs from its default value, and every expression of each arc is set.
        private static Afa<int, string, long> Original()
        {
            var afa = new Afa<int, string, long>(defaultRegister: "register", defaultAccumulator: 42);
            afa.AddSingleElementArc(1, 2, (t, p, reg) => p > 0, (t, p, reg) => reg + p);
            afa.AddMultiElementArc(2, 3,
                initialize: (t, reg) => 1,
                accumulate: (t, p, reg, acc) => acc + p,
                skipToEnd: (t, p, acc) => acc > 9,
                fence: (t, acc, reg) => acc > 0,
                transfer: (t, acc, reg) => reg + acc,
                dispose: acc => acc.ToString());
            afa.AddFinalState(3);
            afa.StartState = 1;
            afa.AllowOverlappingInstances = false;
            afa.IsDeterministic = true;
            afa.Seal();
            return afa;
        }

        private static string Arcs(Afa<int, string, long> afa)
            => string.Join("; ", afa.transitionInfo.OrderBy(source => source.Key).SelectMany(
                source => source.Value.OrderBy(target => target.Key).Select(target => source.Key + " -> " + target.Key + " " + target.Value.ArcType)));

        private static object[] Expressions(Arc<int, string> arc)
        {
            if (arc is SingleElementArc<int, string> single) return new object[] { single.Fence, single.Transfer };
            if (arc is MultiElementArc<int, string, long> multi) return new object[] { multi.Initialize, multi.Accumulate, multi.SkipToEnd, multi.Fence, multi.Transfer, multi.Dispose };
            throw new InvalidOperationException("Original() has only single-element and multi-element arcs.");
        }

        [TestMethod, TestCategory("Gated")]
        public void CloneCopiesEveryField()
        {
            var type = typeof(Afa<int, string, long>);
            foreach (var name in NotAssigned.Keys)
            {
                Assert.IsNotNull(type.GetField(name, InstanceFields), "NotAssigned lists " + name + ", which is not a field of Afa<,,>.");
            }

            var original = Original();
            var clone = original.Clone();
            var fresh = new Afa<int, string, long>();
            var compared = new List<string>();
            foreach (var field in type.GetFields(InstanceFields))
            {
                if (NotAssigned.ContainsKey(field.Name)) continue;
                var unset = "Original() leaves field " + field.Name + " at its default value. Give it another value there, or list it in NotAssigned with the reason.";
                Assert.AreNotEqual(field.GetValue(fresh), field.GetValue(original), unset);
                Assert.AreEqual(field.GetValue(original), field.GetValue(clone), "Clone() does not copy field " + field.Name + ".");
                compared.Add(field.Name);
            }

            CollectionAssert.Contains(compared, nameof(Afa<int, string, long>.DefaultAccumulator));
        }

        [TestMethod, TestCategory("Gated")]
        public void CloneStartsUnsealed()
            => Assert.IsFalse(Original().Clone().IsSealed());

        [TestMethod, TestCategory("Gated")]
        public void CloneHasTheSameFinalStatesInItsOwnList()
        {
            var original = Original();
            var clone = original.Clone();
            CollectionAssert.AreEqual(original.finalStates, clone.finalStates);
            Assert.AreNotSame(original.finalStates, clone.finalStates);
        }

        [TestMethod, TestCategory("Gated")]
        public void CloneHasTheSameArcsInItsOwnDictionaries()
        {
            var original = Original();
            var clone = original.Clone();
            Assert.AreEqual("1 -> 2 SingleElement; 2 -> 3 MultiElement", Arcs(clone));
            Assert.AreNotSame(original.transitionInfo, clone.transitionInfo);
            foreach (var state in original.transitionInfo.Keys)
            {
                Assert.AreNotSame(original.transitionInfo[state], clone.transitionInfo[state], "Clone() shares the arc dictionary of state " + state + ".");

                // Each arc's expressions are compared, not the arc objects: this test does not settle whether a clone shares them.
                foreach (var target in original.transitionInfo[state].Keys)
                {
                    var expected = Expressions(original.transitionInfo[state][target]);
                    var actual = Expressions(clone.transitionInfo[state][target]);
                    for (int i = 0; i < expected.Length; i++)
                    {
                        Assert.IsNotNull(expected[i]);
                        Assert.AreSame(expected[i], actual[i], "Clone() changes expression " + i + " of the arc " + state + " -> " + target + ".");
                    }
                }
            }
        }
    }

    // The pattern APIs must keep the default register they are given. The arcs here are single-element arcs without a
    // transfer, so each match outputs the default register. Every default register is 5, not the type's default 0.
    public abstract class AfaPatternDefaultRegisterTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal AfaPatternDefaultRegisterTestsBase(bool columnar, bool strict)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(strict)) { }

        private static IStreamable<Empty, int> Input(params int[] payloads)
            => payloads.Select((p, i) => StreamEvent.CreatePoint(i + 1, p)).ToObservable().ToStreamable();

        private static string Registers(IStreamable<Empty, int> output)
            => string.Join(", ", output.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload));

        // One arc from state 0 to the final state 1.
        private static Afa<int, int, int> Matching(Expression<Func<long, int, int, bool>> fence, int defaultRegister = 5, int defaultAccumulator = 10)
        {
            var afa = new Afa<int, int, int>(defaultRegister, defaultAccumulator);
            afa.AddSingleElementArc(0, 1, fence);
            afa.AddFinalState(1);
            return afa;
        }

        // The control: Detect of an automaton keeps its default register.
        [TestMethod, TestCategory("Gated")]
        public void DetectOfAnAutomatonKeepsDefaultRegister()
            => Assert.AreEqual("5", Registers(Input(1).Detect(Matching((t, e, r) => e == 1), maxDuration: 10)));

        [TestMethod, TestCategory("Gated")]
        public void DetectWithRegisterKeepsDefaultRegister()
            => Assert.AreEqual("5", Registers(Input(1).Detect(5, p => p.SingleElement(e => e == 1), maxDuration: 10)));

        [TestMethod, TestCategory("Gated")]
        public void DetectWithRegisterAndAccumulatorKeepsDefaultRegister()
            => Assert.AreEqual("5", Registers(Input(1).Detect(5, 10, p => p.SingleElement(e => e == 1), maxDuration: 10)));

        [TestMethod, TestCategory("Gated")]
        public void RegexOrKeepsDefaultRegister()
            => Assert.AreEqual("5", Registers(Input(1).Detect(ARegex.Or(Matching((t, e, r) => e == 1), Matching((t, e, r) => e == 2)), maxDuration: 10)));

        // Or takes its first pattern's defaults, as Concat does.
        [TestMethod, TestCategory("Gated")]
        public void RegexOrKeepsTheFirstPatternsDefaultRegister()
        {
            var either = ARegex.Or(Matching((t, e, r) => e == 1, 5, 10), Matching((t, e, r) => e == 2, 7, 20));
            Assert.AreEqual("5", Registers(Input(1).Detect(either, maxDuration: 10)));
        }

        [TestMethod, TestCategory("Gated")]
        public void RegexOrOfThreeKeepsTheFirstPatternsDefaultRegister()
        {
            var either = ARegex.Or(Matching((t, e, r) => e == 1, 5, 10), Matching((t, e, r) => e == 2, 7, 20), Matching((t, e, r) => e == 3, 9, 30));
            Assert.AreEqual("5", Registers(Input(1).Detect(either, maxDuration: 10)));
        }

        [TestMethod, TestCategory("Gated")]
        public void RegexKleeneStarKeepsDefaultRegister()
            => Assert.AreEqual("5", Registers(Input(1).Detect(ARegex.KleeneStar(Matching((t, e, r) => e == 1)), maxDuration: 10)));

        // An arc leads back to the start state, so ZeroOrOne puts an epsilon arc in front of the pattern.
        [TestMethod, TestCategory("Gated")]
        public void RegexZeroOrOneWithALoopKeepsDefaultRegister()
        {
            var afa = Matching((t, e, r) => e == 1);
            afa.AddSingleElementArc(1, 0, (t, e, r) => e == 2);
            Assert.AreEqual("5", Registers(Input(1).Detect(ARegex.ZeroOrOne(afa), maxDuration: 10)));
        }
    }

    [TestClass]
    public class AfaPatternDefaultRegisterTestsRow : AfaPatternDefaultRegisterTestsBase
    {
        public AfaPatternDefaultRegisterTestsRow() : base(false, false) { }
    }

    [TestClass]
    public class AfaPatternDefaultRegisterTestsColumnar : AfaPatternDefaultRegisterTestsBase
    {
        public AfaPatternDefaultRegisterTestsColumnar() : base(true, false) { }
    }

    [TestClass]
    public class AfaPatternDefaultRegisterTestsColumnarStrict : AfaPatternDefaultRegisterTestsBase
    {
        public AfaPatternDefaultRegisterTestsColumnarStrict() : base(true, true) { }
    }

    // The pattern APIs must keep the default accumulator they are given. Each automaton here has a multi-element arc
    // without an initializer, which starts each match from the default accumulator 10 and adds each payload to it, so
    // payload 1 gives 11. Code generation cannot write such an arc, so the columnar runs fall back to row execution
    // and there is no strict columnar class.
    public abstract class AfaPatternDefaultAccumulatorTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal AfaPatternDefaultAccumulatorTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(false)) { }

        private static IStreamable<Empty, int> Input(params int[] payloads)
            => payloads.Select((p, i) => StreamEvent.CreatePoint(i + 1, p)).ToObservable().ToStreamable();

        private static string Registers(IStreamable<Empty, int> output)
            => string.Join(", ", output.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload));

        private static Afa<int, int, int> AddsToTen(int defaultRegister = 0, int defaultAccumulator = 10)
        {
            var afa = new Afa<int, int, int>(defaultRegister, defaultAccumulator);
            afa.AddMultiElementArc(0, 1, initialize: null, accumulate: (t, e, r, a) => a + e, fence: (t, a, r) => true, transfer: (t, a, r) => a);
            afa.AddFinalState(1);
            return afa;
        }

        // Or and ZeroOrOne add a start state that has only epsilon arcs. The automata given to them here also have a
        // single-element arc, so the engine runs them in its pipe for automata with arcs of several kinds.
        private static Afa<int, int, int> NeverMatches(int defaultRegister = 0, int defaultAccumulator = 10)
        {
            var afa = new Afa<int, int, int>(defaultRegister, defaultAccumulator);
            afa.AddSingleElementArc(0, 1, (t, e, r) => e == 99);
            afa.AddFinalState(1);
            return afa;
        }

        // The control: Detect of an automaton keeps its default accumulator.
        [TestMethod, TestCategory("Gated")]
        public void DetectOfAnAutomatonKeepsDefaultAccumulator()
            => Assert.AreEqual("11", Registers(Input(1).Detect(AddsToTen(), maxDuration: 10)));

        [TestMethod, TestCategory("Gated")]
        public void DetectWithRegisterAndAccumulatorKeepsDefaultAccumulator()
        {
            var output = Input(1).Detect(0, 10, p => p.MultiElement(null, (t, e, r, a) => a + e, null, (t, a, r) => true, (t, a, r) => a, null), maxDuration: 10);
            Assert.AreEqual("11", Registers(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void DefinePatternKeepsDefaultAccumulator()
        {
            var output = Input(1)
                .DefinePattern(0, 10)
                .MultiElement(null, (t, e, r, a) => a + e, null, (t, a, r) => true, (t, a, r) => a, null)
                .Detect(maxDuration: 10);
            Assert.AreEqual("11", Registers(output));
        }

        [TestMethod, TestCategory("Gated")]
        public void RegexOrKeepsDefaultAccumulator()
            => Assert.AreEqual("11", Registers(Input(1).Detect(ARegex.Or(AddsToTen(), NeverMatches()), maxDuration: 10)));

        // Or takes its first pattern's defaults, as Concat does: 10 + 1, not 20 + 1.
        [TestMethod, TestCategory("Gated")]
        public void RegexOrKeepsTheFirstPatternsDefaultAccumulator()
            => Assert.AreEqual("11", Registers(Input(1).Detect(ARegex.Or(AddsToTen(5, 10), NeverMatches(7, 20)), maxDuration: 10)));

        [TestMethod, TestCategory("Gated")]
        public void RegexOrOfThreeKeepsTheFirstPatternsDefaultAccumulator()
            => Assert.AreEqual("11", Registers(Input(1).Detect(ARegex.Or(AddsToTen(5, 10), NeverMatches(7, 20), NeverMatches(9, 30)), maxDuration: 10)));

        [TestMethod, TestCategory("Gated")]
        public void RegexKleeneStarKeepsDefaultAccumulator()
            => Assert.AreEqual("11", Registers(Input(1).Detect(ARegex.KleeneStar(AddsToTen()), maxDuration: 10)));

        // An arc leads back to the start state, so ZeroOrOne puts an epsilon arc in front of the pattern.
        [TestMethod, TestCategory("Gated")]
        public void RegexZeroOrOneWithALoopKeepsDefaultAccumulator()
        {
            var afa = AddsToTen();
            afa.AddSingleElementArc(1, 0, (t, e, r) => e == 2);
            Assert.AreEqual("11", Registers(Input(1).Detect(ARegex.ZeroOrOne(afa), maxDuration: 10)));
        }
    }

    [TestClass]
    public class AfaPatternDefaultAccumulatorTestsRow : AfaPatternDefaultAccumulatorTestsBase
    {
        public AfaPatternDefaultAccumulatorTestsRow() : base(false) { }
    }

    [TestClass]
    public class AfaPatternDefaultAccumulatorTestsColumnar : AfaPatternDefaultAccumulatorTestsBase
    {
        public AfaPatternDefaultAccumulatorTestsColumnar() : base(true) { }
    }
}