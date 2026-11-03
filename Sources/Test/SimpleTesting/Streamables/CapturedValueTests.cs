using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct CapturedInput
    {
        public int Key;
    }

    public enum CapturedColor : short
    {
        Red = 1,
        Green = 2,
    }

    public sealed class CapturedBox
    {
        public int Value;
    }

    // Each of the five expressions reads a variable captured from the constructor, and Set assigns all of
    // them, so a test can change them after the query is built.
    public sealed class CapturedSumAggregate : IAggregate<CapturedInput, long, long>
    {
        private readonly Expression<Func<long>> initialState;
        private readonly Expression<Func<long, long, CapturedInput, long>> accumulate;
        private readonly Expression<Func<long, long, CapturedInput, long>> deaccumulate;
        private readonly Expression<Func<long, long, long>> difference;
        private readonly Expression<Func<long, long>> computeResult;
        private readonly Action<long, long, long> set;

        public CapturedSumAggregate(long start, long mult, long scale)
        {
            this.initialState = () => start;
            this.accumulate = (s, t, e) => s + (e.Key * mult);
            this.deaccumulate = (s, t, e) => s - (e.Key * mult);

            // Both states include the start value once, so the difference adds it back.
            this.difference = (a, b) => a - b + start;
            this.computeResult = s => s * scale;
            this.set = (newStart, newMult, newScale) =>
            {
                start = newStart;
                mult = newMult;
                scale = newScale;
            };
        }

        public void Set(long start, long mult, long scale) => this.set(start, mult, scale);

        public Expression<Func<long>> InitialState() => this.initialState;

        public Expression<Func<long, long, CapturedInput, long>> Accumulate() => this.accumulate;

        public Expression<Func<long, long, CapturedInput, long>> Deaccumulate() => this.deaccumulate;

        public Expression<Func<long, long, long>> Difference() => this.difference;

        public Expression<Func<long, long>> ComputeResult() => this.computeResult;
    }

    // A C# lambda that reads a local variable reads a field of a compiler-generated closure class. For a variable
    // of a literal type, the operator replaces that read with the value when the query is built. Most tests below
    // build a query, assign the variable again, and check that the results use the value at the build. A read
    // that was not replaced fails that check in every configuration: it leaves the operator in row mode, which
    // reads the new value as events arrive, or it makes the operator throw when fallback is disabled.
    public abstract class CapturedValueTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // Two quiet NaNs that differ only in their lowest payload bit.
        private const int NaNBitsA = 0x7FC00001;
        private const int NaNBitsB = 0x7FC00002;

        // Keys 1, 2 and 1 fall in the window that ends at 5; keys 2 and 1 in the one that ends at 10.
        private static readonly int[] Keys = { 1, 2, 1, 2, 1 };
        private static readonly long[] Times = { 1, 2, 4, 7, 9 };

        private static readonly string[] KeysTimesTen = { "1:10", "2:20", "4:10", "7:20", "9:10" };

        // Time, the key times ten, and the input key, which the ungroup result selectors put in Count.
        private static readonly string[] UngroupedTimesTen = { "1:10/1", "2:20/2", "4:10/1", "7:20/2", "9:10/1" };

        internal CapturedValueTestsBase(ConfigModifier modifier) : base(modifier) { }

        // The key selector reads a local, so the expression reads a field of a compiler-generated closure class.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInGroupKey()
        {
            int mult = 10;
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * mult, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });
            mult = 100;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInGroupAggregateResult()
        {
            int mult = 10;
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key, w => w.Count(), (g, c) => new KeyCount { Key = g.Key * mult, Count = c });
            mult = 100;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        // The same key selector in a two-input map and reduce, which builds a Map2ReduceStreamable.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInTwoInputReduce()
        {
            int mult = 10;
            var result = Input().Map(s => s, e => e.Key * mult).Reduce(
                Input().Map(s => s, e => e.Key * mult),
                (l, r) => l.Union(r).TumblingWindowLifetime(5).Count(),
                (g, c) => new KeyCount { Key = g.Key, Count = c });
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:10=4", "5:20=2", "10:10=2", "10:20=2" }, Collect(result));
        }

        // MapReduceStreamable and Map2ReduceStreamable each have a second class for streams without an outer
        // key. No public operator builds it, so these tests construct it directly.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInEmptyKeyMapReduce()
        {
            int mult = 10;
            var result = new MapReduceStreamable<CapturedInput, CapturedInput, int, CapturedInput, ulong, KeyCount>(
                Input(),
                null,
                (l, r) => l,
                e => e.Key * mult,
                s => s.TumblingWindowLifetime(5).Count(),
                (k, c) => new KeyCount { Key = k, Count = c });
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInEmptyKeyMapReduceResult()
        {
            int mult = 10;
            var result = new MapReduceStreamable<CapturedInput, CapturedInput, int, CapturedInput, ulong, KeyCount>(
                Input(),
                null,
                (l, r) => l,
                e => e.Key,
                s => s.TumblingWindowLifetime(5).Count(),
                (k, c) => new KeyCount { Key = k * mult, Count = c });
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInEmptyKeyTwoInputReduce()
        {
            int mult = 10;
            var result = new Map2ReduceStreamable<CapturedInput, CapturedInput, CapturedInput, CapturedInput, int, CapturedInput, CapturedInput, ulong, KeyCount>(
                Input(),
                null,
                (l, r) => l,
                e => e.Key * mult,
                Input(),
                null,
                (l, r) => l,
                e => e.Key * mult,
                (l, r) => l.Union(r).TumblingWindowLifetime(5).Count(),
                (k, c) => new KeyCount { Key = k, Count = c },
                false,
                false,
                OperationalHint.None);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:10=4", "5:20=2", "10:10=2", "10:20=2" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInEmptyKeyTwoInputReduceResult()
        {
            int mult = 10;
            var result = new Map2ReduceStreamable<CapturedInput, CapturedInput, CapturedInput, CapturedInput, int, CapturedInput, CapturedInput, ulong, KeyCount>(
                Input(),
                null,
                (l, r) => l,
                e => e.Key,
                Input(),
                null,
                (l, r) => l,
                e => e.Key,
                (l, r) => l.Union(r).TumblingWindowLifetime(5).Count(),
                (k, c) => new KeyCount { Key = k * mult, Count = c },
                false,
                false,
                OperationalHint.None);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:10=4", "5:20=2", "10:10=2", "10:20=2" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInGroupApplyResult()
        {
            int mult = 10;
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupApply(e => e.Key, g => g.Count(), (g, c) => new KeyCount { Key = g.Key * mult, Count = c });
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        // Directly after ingress, Where, Select and SelectMany are fused into the ingress operator.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInFusedWhere()
        {
            int key = 2;
            var result = Input().Where(e => e.Key == key);
            key = 1;

            CollectionAssert.AreEquivalent(new[] { "2:2", "7:2" }, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInWhere()
        {
            int key = 2;
            var result = Unfused().Where(e => e.Key == key);
            key = 1;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "2:2", "7:2" }, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInFusedSelect()
        {
            int mult = 10;
            var result = Input().Select(e => new CapturedInput { Key = e.Key * mult });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInSelect()
        {
            int mult = 10;
            var result = Unfused().Select(e => new CapturedInput { Key = e.Key * mult });
            mult = 100;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInSelectWithStartTime()
        {
            int mult = 10;
            var result = Unfused().Select((t, e) => new CapturedInput { Key = e.Key * mult });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInSelectByKey()
        {
            int mult = 10;
            var result = Unfused().SelectByKey((k, e) => new CapturedInput { Key = e.Key * mult });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInSelectByKeyWithStartTime()
        {
            int mult = 10;
            var result = Unfused().SelectByKey((t, k, e) => new CapturedInput { Key = e.Key * mult });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInSelectMany()
        {
            int mult = 10;
            var result = Unfused().SelectMany(e => new[] { new CapturedInput { Key = e.Key * mult } });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInSelectManyWithStartTime()
        {
            int mult = 10;
            var result = Unfused().SelectMany((t, e) => new[] { new CapturedInput { Key = e.Key * mult } });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInSelectManyByKey()
        {
            int mult = 10;
            var result = Unfused().SelectManyByKey((k, e) => new[] { new CapturedInput { Key = e.Key * mult } });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInSelectManyByKeyWithStartTime()
        {
            int mult = 10;
            var result = Unfused().SelectManyByKey((t, k, e) => new[] { new CapturedInput { Key = e.Key * mult } });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        // A captured long duration becomes a constant, so the operator takes its constant-duration path.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInDuration()
        {
            long duration = 3;
            var result = Input().AlterEventDuration(s => duration);
            duration = 100;

            CollectionAssert.AreEquivalent(new[] { "1-4:1", "2-5:2", "4-7:1", "7-10:2", "9-12:1" }, CollectIntervals(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInStartTime()
        {
            long shift = 1;
            var result = Input().AlterEventLifetime(s => s + shift, s => 3);
            shift = 100;

            CollectionAssert.AreEquivalent(new[] { "2-5:1", "3-6:2", "5-8:1", "8-11:2", "10-13:1" }, CollectIntervals(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInWindowSelector()
        {
            int mult = 10;
            var result = Input().TumblingWindowLifetime(5).Aggregate(w => w.Sum(e => e.Key * mult));
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:40", "10:30" }, CollectValues(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInWindowFilter()
        {
            int key = 1;
            var result = Input().TumblingWindowLifetime(5).Aggregate(w => w.Where(e => e.Key == key).Count());
            key = 2;

            CollectionAssert.AreEquivalent(new[] { "5:2", "10:1" }, CollectValues(result));
        }

        // In a hopping window, an event leaves the aggregate before the window ends, so the aggregate's
        // difference expression runs as well as its accumulate expression. Each window's sum is reported as a start
        // edge and an end edge, both with the window's start time.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInHoppingWindowSelector()
        {
            int mult = 10;
            var result = Input().HoppingWindowLifetime(10, 5).Aggregate(w => w.Sum(e => e.Key * mult));
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:40", "5:40", "10:70", "10:70", "15:30", "15:30" }, CollectValues(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInAggregateResult()
        {
            int mult = 10;
            var result = Input()
                .TumblingWindowLifetime(5)
                .Aggregate(w => w.Count(), w => w.Sum(e => e.Key), (c, s) => new KeyCount { Key = s * mult, Count = c });
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:40=3", "10:30=2" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInJoin()
        {
            int mult = 10;
            var result = Unfused().Join(Unfused(), (l, r) => new CapturedInput { Key = l.Key * mult });
            mult = 100;

            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInPartitionKey()
        {
            int mult = 10;
            var result = Input().Partition(e => e.Key * mult);
            mult = 100;

            var keys = result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.PartitionKey)
                .ToEnumerable()
                .ToList();
            CollectionAssert.AreEquivalent(KeysTimesTen, keys);
        }

        // GroupStreamable, GroupNestedStreamable and UngroupStreamable are built by the map and reduce operators,
        // which rewrite the lambdas first. These tests construct them directly.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInGroupStreamable()
        {
            int mult = 10;
            var grouped = new GroupStreamable<Empty, CapturedInput, int>(Input(), e => e.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(UngroupedTimesTen, CollectUngrouped(Ungroup(grouped)));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInGroupNestedStreamable()
        {
            int mult = 10;
            var grouped = new GroupNestedStreamable<Empty, CapturedInput, int>(Input(), e => e.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(UngroupedTimesTen, CollectUngrouped(UngroupNested(grouped)));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInUngroupStreamable()
        {
            int mult = 10;
            var result = new UngroupStreamable<int, CapturedInput, KeyCount>(
                new GroupStreamable<Empty, CapturedInput, int>(Input(), e => e.Key),
                (k, p) => new KeyCount { Key = k * mult, Count = (ulong)p.Key });
            mult = 100;

            CollectionAssert.AreEquivalent(UngroupedTimesTen, CollectUngrouped(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInUngroupNestedStreamable()
        {
            int mult = 10;
            var result = new UngroupStreamable<Empty, int, CapturedInput, KeyCount>(
                EqualityComparerExpression<Empty>.Default,
                new GroupNestedStreamable<Empty, CapturedInput, int>(Input(), e => e.Key),
                (k, p) => new KeyCount { Key = k * mult, Count = (ulong)p.Key });
            mult = 100;

            CollectionAssert.AreEquivalent(UngroupedTimesTen, CollectUngrouped(result));
        }

        // The loop gives the inner variable its own closure, which holds a reference to the outer closure, so
        // the lambda reads the outer variable through a chain of two fields.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalsInNestedScopes()
        {
            int outer = 10;
            IStreamable<Empty, CapturedInput> result = null;
            for (int i = 0; i < 1; i++)
            {
                int inner = i;
                result = Unfused().Select(e => new CapturedInput { Key = (e.Key * outer) + inner });
                inner = 5;
            }
            outer = 100;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
        }

        // One query reads a captured variable of every literal type. If any one of them were still read as
        // events arrive, the values assigned after the build would make the predicate false.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalsOfEveryLiteralType()
        {
            bool b = true;
            char c = 'x';
            string s = "abc";
            string nullString = null;
            sbyte sb = -1;
            byte by = 2;
            short sh = -3;
            ushort us = 4;
            int n = -5;
            uint ui = 6;
            long l = -7;
            ulong ul = 8;
            float f = 1.5f;
            double d = -2.5;
            decimal m = 3.25m;
            CapturedColor color = CapturedColor.Green;
            var result = Unfused().Where(e =>
                b && c == 'x' && s == "abc" && nullString == null && sb == -1 && by == 2 && sh == -3 && us == 4
                && n == -5 && ui == 6 && l == -7 && ul == 8 && f == 1.5f && d == -2.5 && m == 3.25m
                && color == CapturedColor.Green);
            b = false;
            c = 'y';
            s = "abd";
            nullString = "x";
            sb = 1;
            by = 3;
            sh = 3;
            us = 5;
            n = 5;
            ui = 7;
            l = 7;
            ul = 9;
            f = 2.5f;
            d = 2.5;
            m = 3.5m;
            color = CapturedColor.Red;

            AssertColumnar(result);
            Assert.AreEqual(5, CollectKeys(result).Count);
        }

        // A captured object is not a literal type, so its field is still read as events arrive, and the operator
        // runs in row mode because generated code cannot read the closure class. With fallback disabled, the
        // query throws instead.
        [TestMethod, TestCategory("Gated")]
        public void CapturedObjectIsReadLive()
            => AssertRowModeUnlessFallbackIsDisabled(() =>
            {
                var box = new CapturedBox { Value = 10 };
                var result = Unfused().Select(e => new CapturedInput { Key = e.Key * box.Value });
                box.Value = 100;

                Assert.IsFalse(result.Properties.IsColumnar);
                CollectionAssert.AreEquivalent(new[] { "1:100", "2:200", "4:100", "7:200", "9:100" }, CollectKeys(result));
            });

        // A nullable value type has no literal form, so it is still read as events arrive.
        [TestMethod, TestCategory("Gated")]
        public void CapturedNullableIsReadLive()
            => AssertRowModeUnlessFallbackIsDisabled(() =>
            {
                int? key = 2;
                var result = Unfused().Where(e => e.Key == key);
                key = 1;

                Assert.IsFalse(result.Properties.IsColumnar);
                CollectionAssert.AreEquivalent(new[] { "1:1", "4:1", "9:1" }, CollectKeys(result));
            });

        // A user-written aggregate is rewritten by the window operator that receives it.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalsInCustomAggregate()
        {
            var aggregate = new CapturedSumAggregate(0, 10, 1);
            var result = Input().TumblingWindowLifetime(5).Aggregate(w => aggregate);
            aggregate.Set(1000, 100, 7);

            CollectionAssert.AreEquivalent(new[] { "5:40", "10:30" }, CollectValues(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalsInCustomAggregateHoppingWindow()
        {
            var aggregate = new CapturedSumAggregate(0, 10, 1);
            var result = Input().HoppingWindowLifetime(10, 5).Aggregate(w => aggregate);
            aggregate.Set(1000, 100, 7);

            CollectionAssert.AreEquivalent(new[] { "5:40", "5:40", "10:70", "10:70", "15:30", "15:30" }, CollectValues(result));
        }

        // Map-reduce builds its inner operators again each time it is subscribed, and only then calls the reduce
        // function, here the one GroupAggregate passes the aggregate through. So a value captured inside the
        // reduce function is read when the query is subscribed, not when it is built.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalsInReduceFunctionAreReadAtSubscribe()
        {
            var aggregate = new CapturedSumAggregate(0, 10, 1);
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key, w => aggregate, (g, s) => new KeyCount { Key = g.Key, Count = (ulong)s });
            aggregate.Set(1000, 100, 7);

            // (1000 + sum of the group's keys * 100) * 7
            CollectionAssert.AreEquivalent(new[] { "5:1=8400", "5:2=8400", "10:1=7700", "10:2=8400" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInGroupApplyKey()
        {
            int mult = 10;
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupApply(e => e.Key * mult, g => g.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        // GroupAggregate uses GroupedWindowStreamable, not map-reduce, when its input has no outer key and only
        // start edges. Each group's running value is reported as a start edge and, once the next value
        // replaces it, as an end edge; both carry the value's start time.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInStartEdgeGroupKey()
        {
            int mult = 10;
            var result = StartEdges()
                .GroupAggregate(e => e.Key * mult, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });
            mult = 100;

            AssertGroupedWindow(result);
            CollectionAssert.AreEquivalent(new[] { "1:10=1", "1:10=1", "2:20=1", "2:20=1", "4:10=2", "4:10=2", "7:20=2", "9:10=3" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInStartEdgeGroupAggregate()
        {
            int mult = 10;
            var result = StartEdges()
                .GroupAggregate(e => e.Key, w => w.Sum(e => e.Key * mult), (g, s) => new KeyCount { Key = g.Key, Count = (ulong)s });
            mult = 100;

            AssertGroupedWindow(result);
            CollectionAssert.AreEquivalent(new[] { "1:1=10", "1:1=10", "2:2=20", "2:2=20", "4:1=20", "4:1=20", "7:2=40", "9:1=30" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInStartEdgeGroupResult()
        {
            int mult = 10;
            var result = StartEdges()
                .GroupAggregate(e => e.Key, w => w.Count(), (g, c) => new KeyCount { Key = g.Key * mult, Count = c });
            mult = 100;

            AssertGroupedWindow(result);
            CollectionAssert.AreEquivalent(new[] { "1:10=1", "1:10=1", "2:20=1", "2:20=1", "4:10=2", "4:10=2", "7:20=2", "9:10=3" }, Collect(result));
        }

        // An explicit end edge makes the aggregate run its deaccumulate expression. Each value is reported as a
        // start edge and an end edge, both with the value's start time.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInDeaccumulate()
        {
            int mult = 10;
            var result = Edges().Aggregate(w => w.Sum(e => e.Key * mult));
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "1:10", "1:10", "2:30", "2:30", "5:20", "5:20" }, CollectValues(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInTwoInputReduceResult()
        {
            int mult = 10;
            var result = Input().Map(s => s, e => e.Key).Reduce(
                Input().Map(s => s, e => e.Key),
                (l, r) => l.Union(r).TumblingWindowLifetime(5).Count(),
                (g, c) => new KeyCount { Key = g.Key * mult, Count = c });
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "5:10=4", "5:20=2", "10:10=2", "10:20=2" }, Collect(result));
        }

        // The query writes the captured variable itself, as an out argument, so it must keep reading the
        // variable rather than a constant. Generated code cannot read the closure class, so with fallback
        // disabled the query throws.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalWrittenByTheQueryIsNotReplaced()
            => AssertRowModeUnlessFallbackIsDisabled(() =>
            {
                int parsed = 0;
                var result = Unfused().Select(e => new CapturedInput { Key = int.TryParse((e.Key * 10).ToString(), out parsed) ? parsed : -1 });

                CollectionAssert.AreEquivalent(KeysTimesTen, CollectKeys(result));
            });

        // GroupedWindowStreamable caches its generated pipe. Two aggregates that differ only in a captured value
        // or a constant must not share one.
        [TestMethod, TestCategory("Gated")]
        public void GroupedWindowAggregatesWithDifferentCapturedValuesDoNotShareCode()
        {
            CollectionAssert.AreEquivalent(new[] { "1:1=10", "1:1=10", "2:2=20", "2:2=20", "4:1=20", "4:1=20", "7:2=40", "9:1=30" }, Collect(StartEdgeSum(10)));
            CollectionAssert.AreEquivalent(new[] { "1:1=100", "1:1=100", "2:2=200", "2:2=200", "4:1=200", "4:1=200", "7:2=400", "9:1=300" }, Collect(StartEdgeSum(100)));
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupedWindowAggregatesWithDifferentConstantsDoNotShareCode()
        {
            var ten = StartEdges()
                .GroupAggregate(e => e.Key, w => w.Sum(e => e.Key * 10), (g, s) => new KeyCount { Key = g.Key, Count = (ulong)s });
            var hundred = StartEdges()
                .GroupAggregate(e => e.Key, w => w.Sum(e => e.Key * 100), (g, s) => new KeyCount { Key = g.Key, Count = (ulong)s });

            CollectionAssert.AreEquivalent(new[] { "1:1=10", "1:1=10", "2:2=20", "2:2=20", "4:1=20", "4:1=20", "7:2=40", "9:1=30" }, Collect(ten));
            CollectionAssert.AreEquivalent(new[] { "1:1=100", "1:1=100", "2:2=200", "2:2=200", "4:1=200", "4:1=200", "7:2=400", "9:1=300" }, Collect(hundred));
        }

        // Group and Shuffle cache their generated code by the key selector's text. These two NaNs differ only in
        // their bits, so code generated for one must not be used for the other.
        [TestMethod, TestCategory("Gated")]
        public void GroupKeysThatDifferOnlyInNaNBitsDoNotShareCode()
        {
            CollectionAssert.AreEquivalent(ExpectedNaNKeys(NaNBitsA), GroupedNaNKeys(NaNBitsA));
            CollectionAssert.AreEquivalent(ExpectedNaNKeys(NaNBitsB), GroupedNaNKeys(NaNBitsB));
        }

        // With two threads, GroupApply shuffles the input between two reduce branches, and the generated shuffle
        // code computes the grouping key.
        [TestMethod, TestCategory("Gated")]
        public void ShuffleKeysThatDifferOnlyInNaNBitsDoNotShareCode()
        {
            using (new ConfigModifier().DefaultScheduler(StreamScheduler.OwnedThreads(2)).Modify())
            {
                CollectionAssert.AreEquivalent(new[] { NaNBitsA }, ShuffledNaNKeys(NaNBitsA));
                CollectionAssert.AreEquivalent(new[] { NaNBitsB }, ShuffledNaNKeys(NaNBitsB));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupNestedKeysThatDifferOnlyInNaNBitsDoNotShareCode()
        {
            CollectionAssert.AreEquivalent(ExpectedNaNKeys(NaNBitsA), GroupedNestedNaNKeys(NaNBitsA));
            CollectionAssert.AreEquivalent(ExpectedNaNKeys(NaNBitsB), GroupedNestedNaNKeys(NaNBitsB));
        }

        // A selector that the C# renderer cannot write must still fall back to row mode, as it did before this
        // change; working out a cache key for its generated code must not throw first. The renderer's exception
        // stops code generation, so with fallback disabled these queries throw instead.
        [TestMethod, TestCategory("Gated")]
        public void UnrenderableGroupKeyFallsBack()
            => AssertFallsBack(
                () => Collect(Input().TumblingWindowLifetime(5)
                    .GroupApply(ReturnKey(), g => g.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c })),
                new[] { "5:1=2", "5:2=1", "10:1=1", "10:2=1" });

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableFlatGroupKeyFallsBack()
            => AssertFallsBack(
                () => CollectUngrouped(new UngroupStreamable<int, CapturedInput, KeyCount>(
                    new GroupStreamable<Empty, CapturedInput, int>(Input(), ReturnKey()),
                    (k, p) => new KeyCount { Key = k * 10, Count = (ulong)p.Key })),
                UngroupedTimesTen);

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableShuffleKeyFallsBack()
        {
            using (new ConfigModifier().DefaultScheduler(StreamScheduler.OwnedThreads(2)).Modify())
            {
                AssertFallsBack(
                    () => CollectOnTwoThreads(s => s
                        .GroupApply(ReturnKey(), g => g.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c }))
                        .Select(k => k.ToString(CultureInfo.InvariantCulture)).ToList(),
                    new[] { "1", "2" });
            }

            // With fallback disabled, the shuffle throws during Subscribe, after the query has taken batches from
            // the memory pools, and the abandoned query never returns them. That is not what this test checks, so
            // reset the pools rather than report a leak.
            if (Config.CodegenOptions.DontFallBackToRowBasedExecution && !Config.ForceRowBasedExecution)
                MemoryManager.Free(true);
        }

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableGroupedWindowAggregateFallsBack()
            => AssertFallsBack(
                () => Collect(StartEdges()
                    .GroupAggregate(e => e.Key, w => w.Sum(ReturnKey()), (g, s) => new KeyCount { Key = g.Key, Count = (ulong)s })),
                new[] { "1:1=1", "1:1=1", "2:2=2", "2:2=2", "4:1=2", "4:1=2", "7:2=4", "9:1=3" });

        // Without a captured value every operator can run columnar, so the query must stay columnar.
        [TestMethod, TestCategory("Gated")]
        public void ConstantKeyStaysColumnar()
        {
            var result = Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * 10, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "5:10=2", "5:20=1", "10:10=1", "10:20=1" }, Collect(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ConstantKeyTwoInputReduceStaysColumnar()
        {
            var result = Input().Map(s => s, e => e.Key * 10).Reduce(
                Input().Map(s => s, e => e.Key * 10),
                (l, r) => l.Union(r).TumblingWindowLifetime(5).Count(),
                (g, c) => new KeyCount { Key = g.Key, Count = c });

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "5:10=4", "5:20=2", "10:10=2", "10:20=2" }, Collect(result));
        }

        private static IStreamable<Empty, KeyCount> StartEdgeSum(int mult)
            => StartEdges()
                .GroupAggregate(e => e.Key, w => w.Sum(e => e.Key * mult), (g, s) => new KeyCount { Key = g.Key, Count = (ulong)s });

        private static List<string> ExpectedNaNKeys(int bits)
            => Enumerable.Range(0, Keys.Length).Select(i => Times[i] + ":" + bits + "/" + Keys[i]).ToList();

        // Groups every event under one NaN key with the given bits, and reports the key's bits.
        private static List<string> GroupedNaNKeys(int bits)
        {
            float nan = BitConverter.Int32BitsToSingle(bits);
            var grouped = new GroupStreamable<Empty, CapturedInput, float>(Input(), e => nan);
            return CollectUngrouped(new UngroupStreamable<float, CapturedInput, KeyCount>(
                grouped, (k, p) => new KeyCount { Key = BitConverter.SingleToInt32Bits(k), Count = (ulong)p.Key }));
        }

        private static List<string> GroupedNestedNaNKeys(int bits)
        {
            float nan = BitConverter.Int32BitsToSingle(bits);
            var grouped = new GroupNestedStreamable<Empty, CapturedInput, float>(Input(), e => nan);
            return CollectUngrouped(new UngroupStreamable<Empty, float, CapturedInput, KeyCount>(
                EqualityComparerExpression<Empty>.Default,
                grouped,
                (k, p) => new KeyCount { Key = BitConverter.SingleToInt32Bits(k), Count = (ulong)p.Key }));
        }

        // The distinct key bits that a two-branch GroupApply over one NaN key reports.
        private static List<int> ShuffledNaNKeys(int bits)
        {
            float nan = BitConverter.Int32BitsToSingle(bits);
            return CollectOnTwoThreads(s => s
                .GroupApply(e => nan, g => g.Count(), (g, c) => new KeyCount { Key = BitConverter.SingleToInt32Bits(g.Key), Count = c }));
        }

        // Runs the query over the input on the current two-thread scheduler and returns the distinct output
        // keys. An error from the query is thrown.
        private static List<int> CollectOnTwoThreads(Func<IStreamable<Empty, CapturedInput>, IStreamable<Empty, KeyCount>> query)
        {
            var input = new Subject<StreamEvent<CapturedInput>>();
            var result = query(input.ToStreamable());

            var keys = new HashSet<int>();
            Exception error = null;
            using (var done = new ManualResetEventSlim())
            using (result.ToStreamEventObservable().Subscribe(
                e =>
                {
                    if (e.IsData)
                    {
                        lock (keys) keys.Add(e.Payload.Key);
                    }
                },
                e =>
                {
                    error = e;
                    done.Set();
                },
                done.Set))
            {
                for (int i = 0; i < Keys.Length; i++)
                {
                    input.OnNext(StreamEvent.CreatePoint(Times[i], new CapturedInput { Key = Keys[i] }));
                }

                input.OnCompleted();
                Assert.IsTrue(done.Wait(TimeSpan.FromSeconds(30)), "the query did not complete");
            }

            if (error != null) throw error;
            return keys.ToList();
        }

        // Returns e.Key through a Return node. The expression compiles and runs, but the C# renderer cannot
        // write a Return node, so code generation fails for any operator that has to write it.
        private static Expression<Func<CapturedInput, int>> ReturnKey()
        {
            var e = Expression.Parameter(typeof(CapturedInput), "e");
            var done = Expression.Label(typeof(int), "done");
            return Expression.Lambda<Func<CapturedInput, int>>(
                Expression.Block(
                    Expression.Return(done, Expression.Field(e, nameof(CapturedInput.Key))),
                    Expression.Label(done, Expression.Constant(0))),
                e);
        }

        private static void AssertFallsBack(Func<List<string>> run, string[] expected)
            => AssertRowModeUnlessFallbackIsDisabled(() => CollectionAssert.AreEquivalent(expected, run()));

        // For a query whose columnar code generation fails: it runs in row mode, unless fallback to row mode is
        // disabled (and row mode is not forced), when it throws.
        private static void AssertRowModeUnlessFallbackIsDisabled(Action run)
        {
            if (Config.CodegenOptions.DontFallBackToRowBasedExecution && !Config.ForceRowBasedExecution)
            {
                var error = Assert.ThrowsException<InvalidOperationException>(run);
                StringAssert.StartsWith(error.Message, "Code Generation failed when it wasn't supposed to! Operator: ");
            }
            else
            {
                run();
            }
        }

        private static void AssertGroupedWindow<TPayload>(IStreamable<Empty, TPayload> result)
            => StringAssert.StartsWith(result.GetType().Name, "GroupedWindowStreamable");

        // The point events, each made to last forever, so the stream has only start edges.
        private static IStreamable<Empty, CapturedInput> StartEdges() => Input().AlterEventDuration(StreamEvent.InfinitySyncTime);

        // Key 1 lives from 1 to 5 and key 2 from 2 to 7, each as a start edge and an explicit end edge.
        private static IStreamable<Empty, CapturedInput> Edges()
            => new[]
            {
                StreamEvent.CreateStart(1, new CapturedInput { Key = 1 }),
                StreamEvent.CreateStart(2, new CapturedInput { Key = 2 }),
                StreamEvent.CreateEnd(5, 1, new CapturedInput { Key = 1 }),
                StreamEvent.CreateEnd(7, 2, new CapturedInput { Key = 2 }),
            }.ToObservable().ToStreamable();

        private static void AssertColumnar<TKey, TPayload>(IStreamable<TKey, TPayload> result)
            => Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);

        private static IStreamable<Empty, CapturedInput> Input()
            => Enumerable.Range(0, Keys.Length)
                .Select(i => StreamEvent.CreatePoint(Times[i], new CapturedInput { Key = Keys[i] }))
                .ToObservable()
                .ToStreamable();

        // An operator between the ingress and the one under test stops the ingress from fusing it.
        private static IStreamable<Empty, CapturedInput> Unfused() => Input().AlterEventDuration(1);

        private static IStreamable<Empty, KeyCount> Ungroup(IStreamable<int, CapturedInput> grouped)
            => new UngroupStreamable<int, CapturedInput, KeyCount>(
                grouped, (k, p) => new KeyCount { Key = k, Count = (ulong)p.Key });

        private static IStreamable<Empty, KeyCount> UngroupNested(IStreamable<CompoundGroupKey<Empty, int>, CapturedInput> grouped)
            => new UngroupStreamable<Empty, int, CapturedInput, KeyCount>(
                EqualityComparerExpression<Empty>.Default, grouped, (k, p) => new KeyCount { Key = k, Count = (ulong)p.Key });

        private static List<string> Collect(IStreamable<Empty, KeyCount> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.Payload.Key + "=" + e.Payload.Count)
                .ToEnumerable()
                .ToList();

        // For the ungrouped streams, whose Count holds the input key.
        private static List<string> CollectUngrouped(IStreamable<Empty, KeyCount> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.Payload.Key + "/" + e.Payload.Count)
                .ToEnumerable()
                .ToList();

        private static List<string> CollectKeys(IStreamable<Empty, CapturedInput> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.Payload.Key)
                .ToEnumerable()
                .ToList();

        private static List<string> CollectIntervals(IStreamable<Empty, CapturedInput> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + "-" + e.EndTime + ":" + e.Payload.Key)
                .ToEnumerable()
                .ToList();

        private static List<string> CollectValues<T>(IStreamable<Empty, T> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.Payload)
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class CapturedValueTestsRow : CapturedValueTestsBase
    {
        public CapturedValueTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // The default settings, where a failed code generation falls back to row mode.
    [TestClass]
    public class CapturedValueTestsColumnarFallback : CapturedValueTestsBase
    {
        public CapturedValueTestsColumnarFallback()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }

    [TestClass]
    public class CapturedValueTestsColumnarNoFallback : CapturedValueTestsBase
    {
        public CapturedValueTestsColumnarNoFallback()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
