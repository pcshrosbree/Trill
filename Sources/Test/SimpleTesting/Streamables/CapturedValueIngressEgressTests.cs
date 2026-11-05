using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct CapturedEvent
    {
        public int Key;
        public long Time;
    }

    // The ingress lambdas (start and end edge extractors, partition key) and the egress result constructors. As in
    // CapturedValueTests, each capture test builds a query from a lambda that reads a local, assigns the local again,
    // and checks that the results use the value at the build. A read that was not replaced leaves the operator in row
    // mode, which reads the new value as events arrive, or makes it throw when fallback is disabled. The partitioned
    // ingress runs in row mode, and the array ingress and egress run without generated code, so for them only the
    // value shows it. The tests of lambdas the renderer cannot write, near the end, check the fallback instead.
    public abstract class CapturedValueIngressEgressTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // Keys 1, 2 and 1 at times 1, 2 and 4.
        private static readonly int[] Keys = { 1, 2, 1 };
        private static readonly long[] Times = { 1, 2, 4 };

        internal CapturedValueIngressEgressTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInStartEdgeExtractor()
        {
            long offset = 10;
            var result = Events().ToTemporalStreamable(e => e.Time + offset, e => e.Time + 20);
            offset = 15;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "11-21:1", "12-22:2", "14-24:1" }, Intervals(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInEndEdgeExtractor()
        {
            long length = 5;
            var result = Events().ToTemporalStreamable(e => e.Time, e => e.Time + length);
            length = 50;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "1-6:1", "2-7:2", "4-9:1" }, Intervals(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInStartEdgeOnlyExtractor()
        {
            long offset = 10;
            var result = Events().ToTemporalStreamable(e => e.Time + offset);
            offset = 100;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "11:1", "12:2", "14:1" }, Starts(result));
        }

        // Directly after the ingress, Where and Select are fused into it, and the fused ingress takes its extractors
        // from the one it replaces.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInExtractorBeforeFusedWhere()
        {
            long offset = 10;
            var result = Events().ToTemporalStreamable(e => e.Time + offset, e => e.Time + 20).Where(e => e.Key > 0);
            offset = 15;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "11-21:1", "12-22:2", "14-24:1" }, Intervals(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInExtractorBeforeFusedSelect()
        {
            long length = 5;
            var result = Events().ToTemporalStreamable(e => e.Time, e => e.Time + length)
                .Select(e => new CapturedEvent { Key = e.Key * 10, Time = e.Time });
            length = 50;

            AssertColumnar(result);
            CollectionAssert.AreEquivalent(new[] { "1-6:10", "2-7:20", "4-9:10" }, Intervals(result));
        }

        // The ingress works out a constant duration from an end edge of the form start + constant, so a captured
        // length now gives one.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLengthGivesConstantDuration()
        {
            long length = 5;
            var result = Events().ToTemporalStreamable(e => e.Time, e => e.Time + length);
            length = 50;

            Assert.IsTrue(result.Properties.IsConstantDuration);
            Assert.AreEqual(5L, result.Properties.ConstantDurationLength);
        }

        // An end edge that is the constant StreamEvent.InfinitySyncTime makes a stream of start edges only.
        [TestMethod, TestCategory("Gated")]
        public void CapturedInfiniteEndGivesStartEdgeOnly()
        {
            long end = StreamEvent.InfinitySyncTime;
            var result = Events().ToTemporalStreamable(e => e.Time, e => end);
            end = 100;

            Assert.IsTrue(result.Properties.IsStartEdgeOnly);
            CollectionAssert.AreEquivalent(new[] { "1:1", "2:2", "4:1" }, Starts(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInPartitionKey()
        {
            int mult = 10;
            var result = Events().ToPartitionedStreamable(e => e.Key * mult, e => e.Time, e => e.Time + 1);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "10/1:1", "20/2:2", "10/4:1" }, PartitionedStarts(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInPartitionedStartEdgeExtractor()
        {
            long offset = 10;
            var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time + offset, e => e.Time + 20);
            offset = 15;

            CollectionAssert.AreEquivalent(new[] { "1/11:1", "2/12:2", "1/14:1" }, PartitionedStarts(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInPartitionedEndEdgeExtractor()
        {
            long length = 5;
            var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time, e => e.Time + length);
            length = 50;

            CollectionAssert.AreEquivalent(new[] { "1-6:1", "2-7:2", "4-9:1" }, PartitionedIntervals(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayStartEdgeExtractor()
        {
            long offset = 10;
            var result = Segments().ToTemporalArrayStreamable(e => e.Time + offset, e => e.Time + 20);
            offset = 15;

            CollectionAssert.AreEquivalent(new[] { "11-21:1", "12-22:2", "14-24:1" }, Intervals(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayEndEdgeExtractor()
        {
            long length = 5;
            var result = Segments().ToTemporalArrayStreamable(e => e.Time, e => e.Time + length);
            length = 50;

            CollectionAssert.AreEquivalent(new[] { "1-6:1", "2-7:2", "4-9:1" }, Intervals(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayPartitionKey()
        {
            int mult = 10;
            var result = Segments().ToPartitionedArrayStreamable(e => e.Key * mult, e => e.Time, e => e.Time + 1);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { "10/1-2:1", "20/2-3:2", "10/4-5:1" }, PartitionedValues(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayPartitionedStartEdgeExtractor()
        {
            long offset = 10;
            var result = Segments().ToPartitionedArrayStreamable(e => e.Key, e => e.Time + offset, e => e.Time + 20);
            offset = 15;

            CollectionAssert.AreEquivalent(new[] { "1/11-21:1", "2/12-22:2", "1/14-24:1" }, PartitionedValues(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayPartitionedEndEdgeExtractor()
        {
            long length = 5;
            var result = Segments().ToPartitionedArrayStreamable(e => e.Key, e => e.Time, e => e.Time + length);
            length = 50;

            CollectionAssert.AreEquivalent(new[] { "1/1-6:1", "2/2-7:2", "1/4-9:1" }, PartitionedValues(result));
        }

        // Two ingresses whose extractors differ only in the captured value get different generated code.
        [TestMethod, TestCategory("Gated")]
        public void IngressesWithDifferentCapturedValuesDoNotShareCode()
        {
            long offset = 10;
            var first = Events().ToTemporalStreamable(e => e.Time + offset, e => e.Time + 20);
            offset = 11;
            var second = Events().ToTemporalStreamable(e => e.Time + offset, e => e.Time + 20);
            offset = 15;

            AssertColumnar(first);
            AssertColumnar(second);
            CollectionAssert.AreEquivalent(new[] { "11-21:1", "12-22:2", "14-24:1" }, Intervals(first));
            CollectionAssert.AreEquivalent(new[] { "12-21:1", "13-22:2", "15-24:1" }, Intervals(second));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInStartEdgeEgress()
        {
            int mult = 10;
            var result = StartEdges().ToTemporalObservable((s, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, CollectWithoutRowMode(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInIntervalEgress()
        {
            int mult = 10;
            var result = IntervalInput().ToTemporalObservable((s, e, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, CollectWithoutRowMode(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInPartitionedStartEdgeEgress()
        {
            int mult = 10;
            var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time).ToTemporalObservable((k, s, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, result.ToEnumerable().ToList());
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInPartitionedIntervalEgress()
        {
            int mult = 10;
            var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time, e => e.Time + 1)
                .ToTemporalObservable((k, s, e, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, result.ToEnumerable().ToList());
        }

        // With floating reorder policies allowed, an egress directly after an ingress fuses into it.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInFusedStartEdgeEgress()
        {
            using (new ConfigModifier().AllowFloatingReorderPolicy(true).Modify())
            {
                int mult = 10;
                var result = Events().ToTemporalStreamable(e => e.Time).ToTemporalObservable((s, p) => s * 1000 + p.Key * mult);
                mult = 100;

                StringAssert.StartsWith(result.GetType().Name, "FusedObservable");
                CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, CollectWithoutRowMode(result));
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInFusedIntervalEgress()
        {
            using (new ConfigModifier().AllowFloatingReorderPolicy(true).Modify())
            {
                int mult = 10;
                var result = Events().ToTemporalStreamable(e => e.Time, e => e.Time + 1).ToTemporalObservable((s, e, p) => s * 1000 + p.Key * mult);
                mult = 100;

                StringAssert.StartsWith(result.GetType().Name, "FusedObservable");
                CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, CollectWithoutRowMode(result));
            }
        }

        // A partitioned ingress of known constant duration, or of start edges only, fuses its egress too. Its fused
        // egress wraps the partition key, which it used to do by substituting into the wrong lambda.
        [TestMethod, TestCategory("Gated")]
        public void FusedPartitionedIntervalEgressWithConstantDuration()
        {
            using (new ConfigModifier().AllowFloatingReorderPolicy(true).Modify())
            {
                var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time, e => e.Time + 1)
                    .ToTemporalObservable((k, s, e, p) => k * 1000 + s * 10 + (e - s));

                StringAssert.StartsWith(result.GetType().Name, "FusedObservable");
                CollectionAssert.AreEquivalent(new[] { 1011L, 2021L, 1041L }, result.ToEnumerable().ToList());
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void FusedPartitionedEgressAfterFusedWhere()
        {
            using (new ConfigModifier().AllowFloatingReorderPolicy(true).Modify())
            {
                var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time, e => e.Time + 1)
                    .Where(e => e.Key > 0)
                    .ToTemporalObservable((k, s, e, p) => k * 1000 + s * 10 + (e - s));

                StringAssert.StartsWith(result.GetType().Name, "FusedObservable");
                CollectionAssert.AreEquivalent(new[] { 1011L, 2021L, 1041L }, result.ToEnumerable().ToList());
            }
        }

        // A captured length now gives the partitioned ingress a constant duration, so its egress fuses.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLengthInFusedPartitionedIntervalEgress()
        {
            using (new ConfigModifier().AllowFloatingReorderPolicy(true).Modify())
            {
                long length = 1;
                var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time, e => e.Time + length)
                    .ToTemporalObservable((k, s, e, p) => k * 1000 + s * 10 + (e - s));
                length = 50;

                StringAssert.StartsWith(result.GetType().Name, "FusedObservable");
                CollectionAssert.AreEquivalent(new[] { 1011L, 2021L, 1041L }, result.ToEnumerable().ToList());
            }
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedInfiniteEndInFusedPartitionedStartEdgeEgress()
        {
            using (new ConfigModifier().AllowFloatingReorderPolicy(true).Modify())
            {
                long end = StreamEvent.InfinitySyncTime;
                var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time, e => end)
                    .ToTemporalObservable((k, s, p) => k * 1000 + s);

                StringAssert.StartsWith(result.GetType().Name, "FusedObservable");
                CollectionAssert.AreEquivalent(new[] { 1001L, 2002L, 1004L }, result.ToEnumerable().ToList());
            }
        }

        // An interval egress whose constructor returns the payload unchanged skips the egress pipe.
        [TestMethod, TestCategory("Gated")]
        public void PayloadOnlyIntervalEgressStillSkipsThePipe()
        {
            var result = IntervalInput().ToTemporalObservable((s, e, p) => p);

            StringAssert.StartsWith(result.GetType().Name, "ReactiveObservable");
            CollectionAssert.AreEquivalent(new[] { 1, 2, 1 }, result.ToEnumerable().Select(p => p.Key).ToList());
        }

        [TestMethod, TestCategory("Gated")]
        public void EgressesWithDifferentCapturedValuesDoNotShareCode()
        {
            int mult = 10;
            var first = StartEdges().ToTemporalObservable((s, p) => s * 1000 + p.Key * mult);
            mult = 20;
            var second = StartEdges().ToTemporalObservable((s, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, CollectWithoutRowMode(first));
            CollectionAssert.AreEquivalent(new[] { 1020L, 2040L, 4020L }, CollectWithoutRowMode(second));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayStartEdgeEgress()
        {
            int mult = 10;
            var result = StartEdges().ToTemporalArrayObservable((s, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, Flatten(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayIntervalEgress()
        {
            int mult = 10;
            var result = IntervalInput().ToTemporalArrayObservable((s, e, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, Flatten(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayPartitionedStartEdgeEgress()
        {
            int mult = 10;
            var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time).ToTemporalArrayObservable((k, s, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, Flatten(result));
        }

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalInArrayPartitionedIntervalEgress()
        {
            int mult = 10;
            var result = Events().ToPartitionedStreamable(e => e.Key, e => e.Time, e => e.Time + 1)
                .ToTemporalArrayObservable((k, s, e, p) => s * 1000 + p.Key * mult);
            mult = 100;

            CollectionAssert.AreEquivalent(new[] { 1010L, 2020L, 4010L }, Flatten(result));
        }

        // A lambda the C# renderer cannot write. The ingress and egress keep generated code in caches keyed by the
        // lambdas' text, and the key must not throw: the operator falls back to row mode, or throws by the
        // fallback policy, as for any other failure of code generation.
        [TestMethod, TestCategory("Gated")]
        public void UnrenderableStartEdgeExtractorFallsBack()
        {
            var result = Events().ToTemporalStreamable(ReturnTime(0), e => e.Time + 1);

            AssertFallsBack(() => Intervals(result), new[] { "1-2:1", "2-3:2", "4-5:1" });
        }

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableEndEdgeExtractorFallsBack()
        {
            var result = Events().ToTemporalStreamable(e => e.Time, ReturnTime(1));

            AssertFallsBack(() => Intervals(result), new[] { "1-2:1", "2-3:2", "4-5:1" });
        }

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableStartEdgeExtractorBeforeFusedWhereFallsBack()
        {
            var result = Events().ToTemporalStreamable(ReturnTime(0), e => e.Time + 1).Where(e => e.Key > 0);

            AssertFallsBack(() => Intervals(result), new[] { "1-2:1", "2-3:2", "4-5:1" });
        }

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableEndEdgeExtractorBeforeFusedWhereFallsBack()
        {
            var result = Events().ToTemporalStreamable(e => e.Time, ReturnTime(1)).Where(e => e.Key > 0);

            AssertFallsBack(() => Intervals(result), new[] { "1-2:1", "2-3:2", "4-5:1" });
        }

        // The fused operators' lambdas are part of the ingress key too.
        [TestMethod, TestCategory("Gated")]
        public void UnrenderableFusedWhereFallsBack()
        {
            var result = Input().Where(ReturnKeyIsPositive());

            AssertFallsBack(() => Starts(result), new[] { "1:1", "2:2", "4:1" });
        }

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableStartEdgeConstructorFallsBack()
        {
            var result = StartEdges().ToTemporalObservable(ReturnKey<Func<long, CapturedEvent, int>>(typeof(long)));

            AssertRowModeUnlessFallbackIsDisabled(() => CollectionAssert.AreEquivalent(new[] { 1, 2, 1 }, result.ToEnumerable().ToList()));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableIntervalConstructorFallsBack()
        {
            var result = IntervalInput().ToTemporalObservable(ReturnKey<Func<long, long, CapturedEvent, int>>(typeof(long), typeof(long)));

            AssertRowModeUnlessFallbackIsDisabled(() => CollectionAssert.AreEquivalent(new[] { 1, 2, 1 }, result.ToEnumerable().ToList()));
        }

        // The array ingress runs in columnar mode, so a partitioned start-edge egress after it tries to generate code.
        // That generator fails for another reason first, so this checks only that the key does not throw: the query
        // falls back, or throws by the fallback policy.
        [TestMethod, TestCategory("Gated")]
        public void UnrenderablePartitionedStartEdgeConstructorFallsBack()
        {
            var result = Segments().ToPartitionedArrayStreamable(e => e.Key, e => e.Time, e => StreamEvent.InfinitySyncTime)
                .ToTemporalObservable(ReturnKey<Func<int, long, CapturedEvent, int>>(typeof(int), typeof(long)));

            AssertRowModeUnlessFallbackIsDisabled(() => CollectionAssert.AreEquivalent(new[] { 1, 2, 1 }, result.ToEnumerable().ToList()), anyCause: true);
        }

        // A Where or Select that is not fused into the ingress keys its own generated code by its lambda.
        [TestMethod, TestCategory("Gated")]
        public void UnrenderableWherePredicateFallsBack()
        {
            // The operator decides while it is built, so the query is built inside the check.
            AssertFallsBack(
                () =>
                {
                    var result = Unfused().Where(ReturnKeyIsPositive());
                    StringAssert.StartsWith(result.GetType().Name, "WhereStreamable");
                    return Intervals(result);
                },
                new[] { "1-2:1", "2-3:2", "4-5:1" });
        }

        [TestMethod, TestCategory("Gated")]
        public void UnrenderableSelectorFallsBack()
        {
            // The operator decides while it is built, so the query is built inside the check.
            AssertFallsBack(
                () =>
                {
                    var result = Unfused().Select(ReturnKeyTimesTen());
                    StringAssert.StartsWith(result.GetType().Name, "SelectStreamable");
                    return Intervals(result);
                },
                new[] { "1-2:10", "2-3:20", "4-5:10" });
        }

        // e => new CapturedEvent { Key = e.Key * 10, Time = e.Time } written with a Return node.
        private static Expression<Func<CapturedEvent, CapturedEvent>> ReturnKeyTimesTen()
        {
            var e = Expression.Parameter(typeof(CapturedEvent), "e");
            var done = Expression.Label(typeof(CapturedEvent), "done");
            var value = Expression.MemberInit(
                Expression.New(typeof(CapturedEvent)),
                Expression.Bind(typeof(CapturedEvent).GetField(nameof(CapturedEvent.Key)), Expression.Multiply(Expression.Field(e, nameof(CapturedEvent.Key)), Expression.Constant(10))),
                Expression.Bind(typeof(CapturedEvent).GetField(nameof(CapturedEvent.Time)), Expression.Field(e, nameof(CapturedEvent.Time))));
            return Expression.Lambda<Func<CapturedEvent, CapturedEvent>>(
                Expression.Block(Expression.Return(done, value), Expression.Label(done, Expression.Default(typeof(CapturedEvent)))),
                e);
        }

        // e => e.Time + offset written with a Return node. It compiles and runs, but the C# renderer cannot write it.
        private static Expression<Func<CapturedEvent, long>> ReturnTime(long offset)
        {
            var e = Expression.Parameter(typeof(CapturedEvent), "e");
            var done = Expression.Label(typeof(long), "done");
            return Expression.Lambda<Func<CapturedEvent, long>>(
                Expression.Block(
                    Expression.Return(done, Expression.Add(Expression.Field(e, nameof(CapturedEvent.Time)), Expression.Constant(offset))),
                    Expression.Label(done, Expression.Constant(0L))),
                e);
        }

        private static Expression<Func<CapturedEvent, bool>> ReturnKeyIsPositive()
        {
            var e = Expression.Parameter(typeof(CapturedEvent), "e");
            var done = Expression.Label(typeof(bool), "done");
            return Expression.Lambda<Func<CapturedEvent, bool>>(
                Expression.Block(
                    Expression.Return(done, Expression.GreaterThan(Expression.Field(e, nameof(CapturedEvent.Key)), Expression.Constant(0))),
                    Expression.Label(done, Expression.Constant(false))),
                e);
        }

        // A constructor that returns the payload's key through a Return node, after parameters of the given types.
        private static Expression<TDelegate> ReturnKey<TDelegate>(params Type[] leading)
        {
            var parameters = leading.Select((t, i) => Expression.Parameter(t, "arg" + (char)('A' + i))).ToList();
            var p = Expression.Parameter(typeof(CapturedEvent), "p");
            parameters.Add(p);
            var done = Expression.Label(typeof(int), "done");
            return Expression.Lambda<TDelegate>(
                Expression.Block(
                    Expression.Return(done, Expression.Field(p, nameof(CapturedEvent.Key))),
                    Expression.Label(done, Expression.Constant(0))),
                parameters);
        }

        private static void AssertFallsBack(Func<List<string>> run, string[] expected)
            => AssertRowModeUnlessFallbackIsDisabled(() => CollectionAssert.AreEquivalent(expected, run()));

        // For a query with a lambda the renderer cannot write: it runs in row mode, unless fallback to row mode is
        // disabled (and row mode is not forced), when it throws. Unless anyCause is set, the cause recorded must be
        // the renderer's NotImplementedException, so the test cannot pass through some other failure, or through an
        // operator that falls back without trying to generate code.
        private static void AssertRowModeUnlessFallbackIsDisabled(Action run, bool anyCause = false)
        {
            if (Config.ForceRowBasedExecution)
            {
                run();
            }
            else if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
            {
                var error = Assert.ThrowsException<InvalidOperationException>(run);
                StringAssert.StartsWith(error.Message, "Code Generation failed when it wasn't supposed to! Operator: ");
                Assert.IsTrue(anyCause || CausedByRenderer(error.InnerException), error.ToString());
            }
            else
            {
                var reports = new List<RowModeReport>();
                Config.RowModeReporter = r =>
                {
                    lock (reports) reports.Add(r);
                };
                try
                {
                    run();
                }
                finally
                {
                    Config.RowModeReporter = null;
                }

                // A report's ToString() writes the operator, so it must not throw for this lambda either.
                var text = string.Join("; ", reports);
                Assert.IsTrue(
                    reports.Any(r => r.Reason == RowModeReason.CodeGenerationFailed && (anyCause || CausedByRenderer(r.Exception))),
                    text);
            }
        }

        private static bool CausedByRenderer(Exception e)
        {
            for (; e != null; e = e.InnerException)
            {
                if (e is NotImplementedException) return true;
            }

            return false;
        }

        // The egress results, checked to come from an egress that reported no fall back to row mode.
        private static List<T> CollectWithoutRowMode<T>(IObservable<T> result)
        {
            var reports = new List<RowModeReport>();
            Config.RowModeReporter = r =>
            {
                lock (reports) reports.Add(r);
            };
            try
            {
                var values = result.ToEnumerable().ToList();
                Assert.AreEqual(0, reports.Count, reports.Count == 0 ? string.Empty : reports[0].ToString());
                return values;
            }
            finally
            {
                Config.RowModeReporter = null;
            }
        }

        private static void AssertColumnar<TKey, TPayload>(IStreamable<TKey, TPayload> result)
            => Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);

        private static CapturedEvent[] EventArray()
            => Enumerable.Range(0, Keys.Length).Select(i => new CapturedEvent { Key = Keys[i], Time = Times[i] }).ToArray();

        private static IObservable<CapturedEvent> Events() => EventArray().ToObservable();

        private static IObservable<ArraySegment<CapturedEvent>> Segments() => new[] { new ArraySegment<CapturedEvent>(EventArray()) }.ToObservable();

        private static IStreamable<Empty, CapturedEvent> Input()
            => EventArray().Select(e => StreamEvent.CreatePoint(e.Time, e)).ToObservable().ToStreamable();

        // The constant-duration AlterEventDuration fuses into the ingress, so these two are still ingress streams.
        // Without floating reorder policies, an egress does not fuse into the ingress.
        private static IStreamable<Empty, CapturedEvent> StartEdges() => Input().AlterEventDuration(StreamEvent.InfinitySyncTime);

        private static IStreamable<Empty, CapturedEvent> IntervalInput() => Input().AlterEventDuration(1);

        // AlterEventDuration with a function is not fused, so a Where or Select after it is an operator of its own.
        private static IStreamable<Empty, CapturedEvent> Unfused() => Input().AlterEventDuration(s => 1);

        private static List<string> Intervals(IStreamable<Empty, CapturedEvent> result)
            => result.ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + "-" + e.EndTime + ":" + e.Payload.Key)
                .ToEnumerable()
                .ToList();

        private static List<string> Starts(IStreamable<Empty, CapturedEvent> result)
            => result.ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.Payload.Key)
                .ToEnumerable()
                .ToList();

        private static List<string> PartitionedStarts<TKey>(IStreamable<PartitionKey<TKey>, CapturedEvent> result)
            => result.ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.PartitionKey + "/" + e.StartTime + ":" + e.Payload.Key)
                .ToEnumerable()
                .ToList();

        private static List<string> PartitionedIntervals<TKey>(IStreamable<PartitionKey<TKey>, CapturedEvent> result)
            => result.ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + "-" + e.EndTime + ":" + e.Payload.Key)
                .ToEnumerable()
                .ToList();

        // Copies each segment as it arrives, because the egress may reuse its array.
        // Collected through a result constructor: in columnar mode the partitioned StreamEvent egress does not
        // compile, a separate defect.
        private static List<string> PartitionedValues(IStreamable<PartitionKey<int>, CapturedEvent> result)
            => result.ToTemporalObservable((k, s, e, p) => k + "/" + s + "-" + e + ":" + p.Key).ToEnumerable().ToList();

        private static List<T> Flatten<T>(IObservable<ArraySegment<T>> result)
            => result.SelectMany(s => s.ToArray()).ToEnumerable().ToList();
    }

    [TestClass]
    public class CapturedValueIngressEgressTestsRow : CapturedValueIngressEgressTestsBase
    {
        public CapturedValueIngressEgressTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // The default settings, where a failed code generation falls back to row mode.
    [TestClass]
    public class CapturedValueIngressEgressTestsColumnarFallback : CapturedValueIngressEgressTestsBase
    {
        public CapturedValueIngressEgressTestsColumnarFallback()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }

    [TestClass]
    public class CapturedValueIngressEgressTestsColumnarNoFallback : CapturedValueIngressEgressTestsBase
    {
        public CapturedValueIngressEgressTestsColumnarNoFallback()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
