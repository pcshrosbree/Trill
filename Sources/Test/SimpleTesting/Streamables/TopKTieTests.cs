// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // A payload with no comparison: neither IComparable nor IComparer.
    public sealed class TopKTieEvent
    {
        public string Key;
        public int Value;
    }

    // The same, as a struct and with properties.
    public struct TopKTieStruct
    {
        public string Key { get; set; }
        public int Value { get; set; }
    }

    // A payload with no comparison and two members before the ordering value.
    public sealed class TopKTieTwoKeys
    {
        public string Group;
        public string Name;
        public int Value;
    }

    // A payload with no comparison and a property whose type cannot be a type argument.
    public sealed class TopKTieSpanPayload
    {
        public string Key;
        public int Value;

        public ReadOnlySpan<char> Text => this.Key.AsSpan();
    }

    // A struct with no comparison, as a member of a payload with no comparison.
    public struct TopKTieTag
    {
        public int A;
    }

    public sealed class TopKTieTagged
    {
        public int Value;
        public TopKTieTag Tag;
    }

    // A base type with no comparison, and a derived type that orders by Key, descending.
    public class TopKTieBase
    {
        public string Key;
        public int Value;
    }

    public sealed class TopKTieDerived : TopKTieBase, IComparable
    {
        public int CompareTo(object obj) => string.CompareOrdinal(((TopKTieBase)obj).Key, this.Key);
    }

    // A payload with a comparison of its own: it orders by Key, descending.
    public sealed class TopKTieComparable : IComparable<TopKTieComparable>
    {
        public string Key;
        public int Value;

        public int CompareTo(TopKTieComparable other) => string.CompareOrdinal(other.Key, this.Key);
    }

    // The same with the non-generic IComparable only.
    public sealed class TopKTieNonGenericComparable : IComparable
    {
        public string Key;
        public int Value;

        public int CompareTo(object obj) => string.CompareOrdinal(((TopKTieNonGenericComparable)obj).Key, this.Key);
    }

    // The same through IComparer on the payload type itself.
    public sealed class TopKTieSelfComparer : IComparer<TopKTieSelfComparer>
    {
        public string Key;
        public int Value;

        public int Compare(TopKTieSelfComparer x, TopKTieSelfComparer y) => string.CompareOrdinal(y.Key, x.Key);
    }

    public abstract class TopKTieTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal TopKTieTestsBase(ConfigModifier modifier) : base(modifier) { }

        // The repro from the issue: two live events tie on Value; both get rank 1.
        [TestMethod, TestCategory("Gated")]
        public void TiesOnAPayloadWithoutAComparison()
        {
            var result = Input(Interval(1, 10, "a", 5), Interval(2, 10, "b", 5)).TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(new[] { "1:1:a", "2:1:a,1:b" }, Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // Ranks skip after a tie, every value tied for the last place is kept, and when a tied event ends
        // the other stays. b ends at 6; c (7) leads throughout.
        [TestMethod, TestCategory("Gated")]
        public void TiedEventsEndAndRanksSkip()
        {
            var result = Input(Interval(1, 10, "a", 5), Interval(2, 6, "b", 5), Interval(3, 10, "c", 7), Interval(4, 10, "d", 1))
                .TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(
                new[] { "1:1:a", "2:1:a,1:b", "3:1:c,2:a,2:b", "4:1:c,2:a,2:b", "6:1:c,2:a" },
                Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // Two events whose members are all equal are two copies of one payload, as equal ints are;
        // removing one leaves the other.
        [TestMethod, TestCategory("Gated")]
        public void EqualPayloadsAreCopies()
        {
            var result = Input(Interval(1, 10, "a", 5), Interval(2, 6, "a", 5), Interval(3, 10, "b", 5)).TopK(e => e.Value, 3);
            CollectionAssert.AreEqual(
                new[] { "1:1:a", "2:1:a,1:a", "3:1:a,1:a,1:b", "6:1:a,1:b" },
                Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // A start edge and its end edge carry two different objects with equal members: the end removes the start.
        [TestMethod, TestCategory("Gated")]
        public void EndEdgeWithAnotherObjectRemovesTheStart()
        {
            var events = new[]
            {
                StreamEvent.CreateStart(1, new TopKTieEvent { Key = "a", Value = 5 }),
                StreamEvent.CreateStart(2, new TopKTieEvent { Key = "b", Value = 5 }),
                StreamEvent.CreateEnd(4, 1, new TopKTieEvent { Key = "a", Value = 5 }),
                StreamEvent.CreatePunctuation<TopKTieEvent>(StreamEvent.InfinitySyncTime),
            };
            var result = events.ToObservable().ToStreamable().TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(new[] { "1:1:a", "2:1:a,1:b", "4:1:b" }, Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // The overload that takes a comparison breaks ties the same way.
        [TestMethod, TestCategory("Gated")]
        public void TiesWithAComparison()
        {
            var result = Input(Interval(1, 10, "a", 5), Interval(2, 10, "b", 5), Interval(3, 10, "c", 4))
                .TopK(e => e.Value, (x, y) => y.CompareTo(x), 2);
            CollectionAssert.AreEqual(new[] { "1:1:a", "2:1:a,1:b", "3:1:c,2:a,2:b" }, Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // A struct payload with properties and no comparison.
        [TestMethod, TestCategory("Gated")]
        public void TiesOnAStructWithoutAComparison()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 10, new TopKTieStruct { Key = "b", Value = 5 }),
                StreamEvent.CreateInterval(2, 10, new TopKTieStruct { Key = "a", Value = 5 }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(new[] { "1:1:b", "2:1:a,1:b" }, Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // Ties are ordered by the first member, then by the next: Group, then Name.
        [TestMethod, TestCategory("Gated")]
        public void TiesAreOrderedMemberByMember()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 10, new TopKTieTwoKeys { Group = "g1", Name = "b", Value = 5 }),
                StreamEvent.CreateInterval(2, 10, new TopKTieTwoKeys { Group = "g1", Name = "a", Value = 5 }),
                StreamEvent.CreateInterval(3, 10, new TopKTieTwoKeys { Group = "g0", Name = "z", Value = 5 }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 1);
            CollectionAssert.AreEqual(
                new[] { "1:1:g1b", "2:1:g1a,1:g1b", "3:1:g0z,1:g1a,1:g1b" },
                Starts(result, r => r.Rank + ":" + r.Payload.Group + r.Payload.Name));
        }

        // A null member sorts before any other value, and does not stop the comparison.
        [TestMethod, TestCategory("Gated")]
        public void TiesWithANullMember()
        {
            var result = Input(Interval(1, 10, "a", 5), Interval(2, 10, null, 5), Interval(3, 6, null, 5)).TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(
                new[] { "1:1:a", "2:1:null,1:a", "3:1:null,1:null,1:a", "6:1:null,1:a" },
                Starts(result, r => r.Rank + ":" + (r.Payload.Key ?? "null")));
        }

        // A payload with its own comparison keeps it: ties come out in its order (Key descending), as before the fix.
        [TestMethod, TestCategory("Gated")]
        public void TiesOnAComparablePayloadUseItsComparison()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 10, new TopKTieComparable { Key = "a", Value = 5 }),
                StreamEvent.CreateInterval(2, 10, new TopKTieComparable { Key = "b", Value = 5 }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(new[] { "1:1:a", "2:1:b,1:a" }, Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // The same for a payload with only the non-generic IComparable.
        [TestMethod, TestCategory("Gated")]
        public void TiesOnANonGenericComparablePayloadUseItsComparison()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 10, new TopKTieNonGenericComparable { Key = "a", Value = 5 }),
                StreamEvent.CreateInterval(2, 10, new TopKTieNonGenericComparable { Key = "b", Value = 5 }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(new[] { "1:1:a", "2:1:b,1:a" }, Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // The same for a payload type that is its own IComparer.
        [TestMethod, TestCategory("Gated")]
        public void TiesOnAPayloadThatIsItsOwnComparerUseIt()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 10, new TopKTieSelfComparer { Key = "a", Value = 5 }),
                StreamEvent.CreateInterval(2, 10, new TopKTieSelfComparer { Key = "b", Value = 5 }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(new[] { "1:1:a", "2:1:b,1:a" }, Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // Untied events of a plain class, which worked before the fix: each removal finds its event.
        [TestMethod, TestCategory("Gated")]
        public void UntiedEventsOfAPayloadWithoutAComparison()
        {
            var result = Input(Interval(1, 4, "a", 5), Interval(2, 6, "b", 3), Interval(3, 8, "c", 7)).TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(
                new[] { "1:1:a", "2:1:a,2:b", "3:1:c,2:a", "4:1:c,2:b", "6:1:c" },
                Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // When no comparison can be built for some member (here a ReadOnlySpan property), TopK keeps the old tie
        // comparer, and untied events work, as they did before the fix. (A tie would still throw, as before.)
        [TestMethod, TestCategory("Gated")]
        public void PayloadWithASpanPropertyKeepsTheOldTieComparer()
        {
            var untied = new[]
            {
                StreamEvent.CreateInterval(1, 3, new TopKTieSpanPayload { Key = "a", Value = 5 }),
                StreamEvent.CreateInterval(2, 4, new TopKTieSpanPayload { Key = "b", Value = 7 }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 1);
            CollectionAssert.AreEqual(new[] { "1:1:a", "2:1:b", "3:1:b" }, Starts(untied, r => r.Rank + ":" + r.Payload.Key));
        }

        // Untied events whose payload has a member of a struct type with no comparison: when an event ends, its
        // payload is compared with itself, which must give 0 without comparing the member, as it did before the fix.
        [TestMethod, TestCategory("Gated")]
        public void UntiedEventsWithAStructMemberWithoutAComparison()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 3, new TopKTieTagged { Value = 5, Tag = new TopKTieTag { A = 1 } }),
                StreamEvent.CreateInterval(2, 4, new TopKTieTagged { Value = 7, Tag = new TopKTieTag { A = 2 } }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 1);
            CollectionAssert.AreEqual(new[] { "1:1:5", "2:1:7", "3:1:7" }, Starts(result, r => r.Rank + ":" + r.Payload.Value));
        }

        private static IStreamable<Empty, TopKTieEvent> Input(params StreamEvent<TopKTieEvent>[] events)
            => events.ToObservable().ToStreamable();

        private static StreamEvent<TopKTieEvent> Interval(long start, long end, string key, int value)
            => StreamEvent.CreateInterval(start, end, new TopKTieEvent { Key = key, Value = value });

        internal static List<string> Starts<T>(IStreamable<Empty, List<RankedEvent<T>>> result, Func<RankedEvent<T>, string> show)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsStart)
                .Select(e => e.StartTime + ":" + string.Join(",", e.Payload.Select(show)))
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class TopKTieTestsRow : TopKTieTestsBase
    {
        public TopKTieTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // Payloads whose runtime types differ from the declared type, which only row mode carries.
    [TestClass]
    public class TopKTieTestsRuntimeTypes : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public TopKTieTestsRuntimeTypes()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }

        // Declared as object, the payloads are boxed ints, which the default comparer can compare: ties keep its order.
        [TestMethod, TestCategory("Gated")]
        public void TiesOnBoxedIntsDeclaredAsObject()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 10, (object)3),
                StreamEvent.CreateInterval(2, 10, (object)1),
            }.ToObservable().ToStreamable().TopK(x => (int)x % 2, 2);
            CollectionAssert.AreEqual(new[] { "1:1:3", "2:1:1,1:3" }, TopKTieTestsBase.Starts(result, r => r.Rank + ":" + r.Payload));
        }

        // Declared as a base type with no comparison, the payloads are of a derived type with IComparable: ties keep
        // the derived type's order (Key, descending).
        [TestMethod, TestCategory("Gated")]
        public void TiesOnADerivedTypeWithAComparison()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 10, (TopKTieBase)new TopKTieDerived { Key = "a", Value = 5 }),
                StreamEvent.CreateInterval(2, 10, (TopKTieBase)new TopKTieDerived { Key = "b", Value = 5 }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 2);
            CollectionAssert.AreEqual(new[] { "1:1:a", "2:1:b,1:a" }, TopKTieTestsBase.Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }

        // Base-type payloads without IComparable and a derived payload with it, all tied: the base payloads come
        // first, in member order, then the derived one; the order is consistent, so the derived event can end.
        [TestMethod, TestCategory("Gated")]
        public void TiesOnBaseAndDerivedPayloadsTogether()
        {
            var result = new[]
            {
                StreamEvent.CreateInterval(1, 10, new TopKTieBase { Key = "a", Value = 5 }),
                StreamEvent.CreateInterval(2, 10, new TopKTieBase { Key = "c", Value = 5 }),
                StreamEvent.CreateInterval(3, 6, (TopKTieBase)new TopKTieDerived { Key = "b", Value = 5 }),
            }.ToObservable().ToStreamable().TopK(e => e.Value, 1);
            CollectionAssert.AreEqual(
                new[] { "1:1:a", "2:1:a,1:c", "3:1:a,1:c,1:b", "6:1:a,1:c" },
                TopKTieTestsBase.Starts(result, r => r.Rank + ":" + r.Payload.Key));
        }
    }

    [TestClass]
    public class TopKTieTestsColumnar : TopKTieTestsBase
    {
        public TopKTieTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
