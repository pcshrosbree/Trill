using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

// StyleCop 1.0.2 predates records, so it misreads a positional record's parameters.
#pragma warning disable SA1313

namespace SimpleTesting
{
    // A positional record: its properties have init-only setters, so it cannot be held in columns.
    public sealed record RowModePosition(int Key, ulong Count);

    public sealed class RowModeInitOnly { public int Key { get; init; } }

    public sealed class RowModeGetOnly { public int Key { get; } }

    public sealed class RowModeReadOnlyField { public readonly int Key; }

    public sealed class RowModeHiddenField { private int key; public int Key { get => this.key; set => this.key = value; } }

    public sealed class RowModeHiddenProperty { private int Key { get; set; } public int Visible => this.Key; }

    public abstract class RowModeAbstract { public int Key; }

    public sealed class RowModeNoDefaultConstructor
    {
        public int Key;

        public RowModeNoDefaultConstructor(int key) => this.Key = key;
    }

    public sealed class RowModeGeneric<T> { public T Value; }

    // Each test sets the row-mode hook, builds a query, and checks the reports it made.
    [TestClass]
    public class RowModeReportTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private ConcurrentQueue<RowModeReport> reports;

        public RowModeReportTests()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }

        // MSTest runs the base class's TestInitialize before this one, and this TestCleanup before the base's.
        [TestInitialize]
        public void StartCollecting()
        {
            var queue = new ConcurrentQueue<RowModeReport>();
            this.reports = queue;
            Config.RowModeReporter = queue.Enqueue;
        }

        [TestCleanup]
        public void StopCollecting() => Config.RowModeReporter = null;

        private sealed class RowModeNotPublic { public int Key { get; set; } }

        [DataTestMethod, TestCategory("Gated")]
        [DataRow(typeof(RowModePosition), "property Key has an init-only setter")]
        [DataRow(typeof(RowModeInitOnly), "property Key has an init-only setter")]
        [DataRow(typeof(RowModeGetOnly), "property Key has no setter")]
        [DataRow(typeof(RowModeReadOnlyField), "field Key is read-only")]
        [DataRow(typeof(RowModeHiddenField), "field key is not public")]
        [DataRow(typeof(RowModeHiddenProperty), "property Key is not public")]
        [DataRow(typeof(RowModeAbstract), "the type is abstract or an interface")]
        [DataRow(typeof(RowModeNoDefaultConstructor), "the type has no public parameterless constructor")]
        [DataRow(typeof(RowModeNotPublic), "the type is not public")]
        public void RefusalNamesTheRule(Type type, string rule)
        {
            Assert.AreEqual(rule, type.ColumnarRefusal());
            Assert.IsFalse(type.CanRepresentAsColumnar());
        }

        [TestMethod, TestCategory("Gated")]
        public void RefusalNamesAnonymousMembers()
        {
            var nested = new { Inner = new { Key = 1 } }.GetType();
            Assert.AreEqual("property Inner has an anonymous type", nested.ColumnarRefusal());

            var holder = typeof(RowModeGeneric<>).MakeGenericType(new { Key = 1 }.GetType());
            Assert.AreEqual("field Value has an anonymous type", holder.ColumnarRefusal());
        }

        [TestMethod, TestCategory("Gated")]
        public void ColumnarTypesHaveNoRefusal()
        {
            Assert.IsNull(typeof(CapturedInput).ColumnarRefusal());
            Assert.IsNull(typeof(KeyCount).ColumnarRefusal());
            Assert.IsNull(new { Key = 1 }.GetType().ColumnarRefusal());
        }

        // A payload that cannot be held in columns makes the ingress row mode. The ingress reports, and the
        // operators after it, which only inherit row input, do not.
        [TestMethod, TestCategory("Gated")]
        public void IngressReportsPayloadType()
        {
            var result = Ingress(new RowModeInitOnly { Key = 1 })
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });
            Run(result);

            var report = Single();
            Assert.AreEqual(RowModeReason.TypeNotColumnar, report.Reason);
            Assert.AreEqual(typeof(RowModeInitOnly), report.NonColumnarType);
            Assert.AreEqual("property Key has an init-only setter", report.Detail);
            Assert.IsFalse(report.AtSubscribe);
            StringAssert.Contains(report.Operator.GetType().Name, "IngressStreamable");
        }

        // A Where folded into the generated ingress fails to compile, so the ingress reports when an operator
        // after it first asks whether its input is columnar.
        [TestMethod, TestCategory("Gated")]
        public void IngressReportsCodeGenerationFailure()
        {
            Run(Input().Where(BlockPredicate()).TumblingWindowLifetime(5)
                .GroupAggregate(y => y.Key, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c }));

            var report = Single();
            Assert.AreEqual(RowModeReason.CodeGenerationFailed, report.Reason);
            StringAssert.Contains(report.Operator.GetType().Name, "IngressStreamable");
            StringAssert.Contains(report.Diagnostics, "error CS");
        }

        // With egress straight after the folded Where, nothing asks whether the ingress is columnar until the
        // egress subscribes, so the ingress decides then, and its report must say so.
        [TestMethod, TestCategory("Gated")]
        public void IngressDecidingAtSubscribeSaysSo()
        {
            var result = Input().Where(BlockPredicate());
            Assert.AreEqual(0, this.reports.Count, "nothing reports while the query is built");
            Run(result);

            var report = Single();
            Assert.AreEqual(RowModeReason.CodeGenerationFailed, report.Reason);
            StringAssert.Contains(report.Operator.GetType().Name, "IngressStreamable");
            Assert.IsTrue(report.AtSubscribe);
        }

        // Map-reduce builds its query in its constructor and again at Subscribe; only the second build's reports
        // come from Subscribe.
        [TestMethod, TestCategory("Gated")]
        public void MapReduceSecondBuildReportsAtSubscribe()
        {
            int mult = 10;
            var result = Input().TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * mult, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });
            var built = this.reports.ToArray();
            Assert.IsTrue(built.Length > 0, "no report from the constructor");
            Assert.IsFalse(built.Any(r => r.AtSubscribe));

            Run(result);

            var subscribed = this.reports.Skip(built.Length).ToArray();
            Assert.IsTrue(subscribed.Length > 0, "no report from the second build");
            Assert.IsTrue(subscribed.All(r => r.AtSubscribe));
        }

        // Cache() subscribes the stream directly, not through an egress method; map-reduce's second build is still
        // a decision made during Subscribe.
        [TestMethod, TestCategory("Gated")]
        public void MapReduceSecondBuildUnderCacheReportsAtSubscribe()
        {
            int mult = 10;
            var result = Input().TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * mult, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });
            var built = this.reports.Count;

            using (var cache = result.Cache())
            {
                var subscribed = this.reports.Skip(built).ToArray();
                Assert.IsTrue(subscribed.Length > 0, "no report from the second build");
                Assert.IsTrue(subscribed.All(r => r.AtSubscribe));
            }
        }

        // Cache() straight after the folded Where: the ingress decides when Cache() subscribes.
        [TestMethod, TestCategory("Gated")]
        public void IngressDecidingUnderCacheSaysSo()
        {
            var result = Input().Where(BlockPredicate());
            Assert.AreEqual(0, this.reports.Count, "nothing reports while the query is built");

            using (var cache = result.Cache())
            {
                var report = Single();
                Assert.AreEqual(RowModeReason.CodeGenerationFailed, report.Reason);
                Assert.IsTrue(report.AtSubscribe);
            }
        }

        // The hook must see a fully built ingress, including the name it was given.
        [TestMethod, TestCategory("Gated")]
        public void AtemporalIngressIsBuiltWhenItReports()
        {
            string seen = null;
            Config.RowModeReporter = r => seen = ((IIngressStreamable<Empty, RowModeInitOnly>)r.Operator).IngressSiteIdentifier;
            _ = new QueryContainer().RegisterAtemporalInput(Observable.Empty<RowModeInitOnly>(), identifier: "atemporal");
            Assert.AreEqual("atemporal", seen);
        }

        [TestMethod, TestCategory("Gated")]
        public void AtemporalArrayIngressIsBuiltWhenItReports()
        {
            string seen = null;
            Config.RowModeReporter = r => seen = ((IIngressStreamable<Empty, RowModeInitOnly>)r.Operator).IngressSiteIdentifier;
            _ = new QueryContainer().RegisterAtemporalArrayInput(
                Observable.Empty<ArraySegment<RowModeInitOnly>>(), TimelinePolicy.Sequence(1), identifier: "atemporal array");
            Assert.AreEqual("atemporal array", seen);
        }

        [TestMethod, TestCategory("Gated")]
        public void TemporalArrayIngressIsBuiltWhenItReports()
        {
            string seen = null;
            Config.RowModeReporter = r => seen = ((IIngressStreamable<Empty, RowModeInitOnly>)r.Operator).IngressSiteIdentifier;
            _ = Observable.Empty<ArraySegment<StreamEvent<RowModeInitOnly>>>()
                .ToCheckpointableStreamable(null, "temporal array", OnCompletedPolicy.EndOfStream);
            Assert.AreEqual("temporal array", seen);
        }

        // A unary operator whose output type cannot be held in columns.
        [TestMethod, TestCategory("Gated")]
        public void UnaryOperatorReportsResultType()
        {
            Run(Unfused().Select(e => new RowModePosition(e.Key, 1UL)));

            var report = Single();
            Assert.AreEqual(RowModeReason.TypeNotColumnar, report.Reason);
            Assert.AreEqual(typeof(RowModePosition), report.NonColumnarType);
            StringAssert.StartsWith(report.Operator.GetType().Name, "SelectStreamable");
        }

        [TestMethod, TestCategory("Gated")]
        public void BinaryOperatorReportsResultType()
        {
            Run(Unfused().Join(Unfused(), l => l.Key, r => r.Key, (l, r) => new RowModePosition(l.Key, 1UL)));

            var report = OnePerBuild();
            Assert.AreEqual(RowModeReason.TypeNotColumnar, report.Reason);
            Assert.AreEqual(typeof(RowModePosition), report.NonColumnarType);
            StringAssert.StartsWith(report.Operator.GetType().Name, "EquiJoinStreamable");
        }

        [TestMethod, TestCategory("Gated")]
        public void UngroupReportsResultType()
        {
            Run(Unfused().GroupApply(e => e.Key, g => g.Count(), (g, c) => new RowModePosition(g.Key, c)));

            var report = OnePerBuild();
            Assert.AreEqual(RowModeReason.TypeNotColumnar, report.Reason);
            Assert.AreEqual(typeof(RowModePosition), report.NonColumnarType);
            StringAssert.StartsWith(report.Operator.GetType().Name, "UngroupStreamable");
        }

        // The map-reduce class for streams without an outer key ends with the other Ungroup class.
        [TestMethod, TestCategory("Gated")]
        public void EmptyKeyUngroupReportsResultType()
        {
            Run(new MapReduceStreamable<CapturedInput, CapturedInput, int, CapturedInput, ulong, RowModePosition>(
                Input(),
                null,
                (l, r) => l,
                e => e.Key,
                s => s.Count(),
                (k, c) => new RowModePosition(k, c)));

            var report = OnePerBuild();
            Assert.AreEqual(RowModeReason.TypeNotColumnar, report.Reason);
            Assert.AreEqual(typeof(RowModePosition), report.NonColumnarType);
            StringAssert.StartsWith(report.Operator.GetType().Name, "UngroupStreamable");
        }

        // Ungroup has no columnar form for an anonymous grouping key, and says so.
        [TestMethod, TestCategory("Gated")]
        public void UngroupReportsAnonymousKey()
        {
            Run(Unfused().GroupApply(e => new { e.Key }, g => g.Count(), (g, c) => new KeyCount { Key = g.Key.Key, Count = c }));

            var report = this.reports.Where(r => r.Operator.GetType().Name.StartsWith("UngroupStreamable", StringComparison.Ordinal)).ToArray();
            Assert.IsTrue(report.Length > 0, string.Join("; ", this.reports.Select(r => r.ToString())));
            Assert.IsTrue(report.All(r => r.Reason == RowModeReason.NotSupported && r.Detail == "the grouping key is an anonymous type"));
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressReportsAnonymousPayload()
        {
            _ = Ingress(new { Key = 1 });

            var report = Single();
            Assert.AreEqual(RowModeReason.NotSupported, report.Reason);
            Assert.AreEqual("the ingress payload type is an anonymous type", report.Detail);
        }

        // The key selector reads a captured local, so the grouping operator's generated code does not compile.
        // Trill caches the failed result, so the second build skips the generator; it must still carry the
        // exception.
        [TestMethod, TestCategory("Gated")]
        public void GroupReportsCodeGenerationFailureOnEveryBuild()
        {
            int mult = 10;
            for (int build = 0; build < 2; build++)
            {
                Run(Input().TumblingWindowLifetime(5)
                    .GroupAggregate(e => e.Key * mult, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c }));

                var report = OnePerBuild();
                Assert.AreEqual(RowModeReason.CodeGenerationFailed, report.Reason);
                StringAssert.StartsWith(report.Operator.GetType().Name, "GroupNestedStreamable");
                StringAssert.Contains(report.Diagnostics, "error CS");
                Assert.IsNotNull(report.Exception, "build " + build);
                StringAssert.StartsWith(report.Detail, "code generation threw ");
                this.reports = new ConcurrentQueue<RowModeReport>();
                Config.RowModeReporter = this.reports.Enqueue;
            }
        }

        // An egress decides at Subscribe. The source is columnar, but the egress does not generate code for an
        // anonymous output type.
        [TestMethod, TestCategory("Gated")]
        public void EgressReportsAtSubscribe()
        {
            var result = Unfused().Select(e => new { e.Key });
            Assert.AreEqual(0, this.reports.Count, "nothing reports while the query is built");
            result.ToStreamEventObservable().ToEnumerable().ToList();

            var report = Single();
            Assert.AreEqual(RowModeReason.NotSupported, report.Reason);
            Assert.AreEqual("the egress output uses an anonymous type", report.Detail);
            Assert.IsTrue(report.AtSubscribe);
            Assert.IsNotInstanceOfType(report.Operator, typeof(Streamable<Empty, CapturedInput>));
        }

        // With more than one reduce branch, the shuffle decides at Subscribe. No data is sent, so the stream's
        // columnar properties, which the fallback does not change, are never put to the test.
        [TestMethod, TestCategory("Gated")]
        public void MultiBranchShuffleReportsAtSubscribe()
        {
            int mult = 10;
            using (new ConfigModifier().DefaultScheduler(StreamScheduler.OwnedThreads(2)).Modify())
            {
                var input = new Subject<StreamEvent<CapturedInput>>();
                var result = new MapReduceStreamable<CapturedInput, CapturedInput, int, CapturedInput, ulong, KeyCount>(
                    input.ToStreamable(),
                    null,
                    (l, r) => l,
                    e => e.Key * mult,
                    s => s.Count(),
                    (k, c) => new KeyCount { Key = k, Count = c });
                Assert.IsFalse(this.reports.Any(r => r.AtSubscribe));

                Exception error = null;
                using (var done = new System.Threading.ManualResetEventSlim())
                using (result.ToStreamEventObservable().Subscribe(_ => { }, e => { error = e; done.Set(); }, done.Set))
                {
                    input.OnCompleted();
                    Assert.IsTrue(done.Wait(TimeSpan.FromSeconds(30)), "the query did not complete");
                }
                Assert.IsNull(error, error?.ToString());

                // One shuffle for each map branch, and each reports once. Map-reduce's second build, at Subscribe,
                // reports too; only the shuffles are checked here.
                var atSubscribe = this.reports
                    .Where(r => r.AtSubscribe && r.Operator.GetType().Name.StartsWith("Shuffle", StringComparison.Ordinal))
                    .ToArray();
                Assert.IsTrue(atSubscribe.Length > 0, "no shuffle report at Subscribe");
                Assert.AreEqual(atSubscribe.Length, atSubscribe.Select(r => r.Operator).Distinct(ReferenceEqualityComparer.Instance).Count());
                foreach (var report in atSubscribe)
                {
                    Assert.AreEqual(RowModeReason.CodeGenerationFailed, report.Reason);
                    StringAssert.StartsWith(report.Operator.GetType().Name, "ShuffleStreamable");
                }
            }
        }

        // GroupApply builds the map-reduce class with an outer key, whose shuffle is the nested one.
        [TestMethod, TestCategory("Gated")]
        public void MultiBranchNestedShuffleReportsAtSubscribe()
        {
            int mult = 10;
            using (new ConfigModifier().DefaultScheduler(StreamScheduler.OwnedThreads(2)).Modify())
            {
                var input = new Subject<StreamEvent<CapturedInput>>();
                var result = input.ToStreamable()
                    .GroupApply(e => e.Key * mult, g => g.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c });

                Exception error = null;
                using (var done = new System.Threading.ManualResetEventSlim())
                using (result.ToStreamEventObservable().Subscribe(_ => { }, e => { error = e; done.Set(); }, done.Set))
                {
                    input.OnCompleted();
                    Assert.IsTrue(done.Wait(TimeSpan.FromSeconds(30)), "the query did not complete");
                }
                Assert.IsNull(error, error?.ToString());

                var atSubscribe = this.reports
                    .Where(r => r.AtSubscribe && r.Operator.GetType().Name.StartsWith("Shuffle", StringComparison.Ordinal))
                    .ToArray();
                Assert.IsTrue(atSubscribe.Length > 0, string.Join("; ", this.reports.Select(r => r.ToString())));
                foreach (var report in atSubscribe)
                {
                    Assert.AreEqual(RowModeReason.CodeGenerationFailed, report.Reason);
                    StringAssert.StartsWith(report.Operator.GetType().Name, "ShuffleNestedStreamable");
                }
            }
        }

        // Map-reduce builds its query in its constructor, so the constructor reports.
        [TestMethod, TestCategory("Gated")]
        public void MapReduceReportsWhenConstructed()
        {
            int mult = 10;
            _ = new MapReduceStreamable<CapturedInput, CapturedInput, int, CapturedInput, ulong, KeyCount>(
                Input(),
                null,
                (l, r) => l,
                e => e.Key * mult,
                s => s.TumblingWindowLifetime(5).Count(),
                (k, c) => new KeyCount { Key = k, Count = c });

            Assert.IsTrue(this.reports.Any(r => r.Reason == RowModeReason.CodeGenerationFailed), "no report from the constructor");
            Assert.IsFalse(this.reports.Any(r => r.AtSubscribe));
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionedIngressReports()
        {
            _ = new[] { PartitionedStreamEvent.CreatePoint(0, 1L, new CapturedInput { Key = 1 }) }.ToObservable().ToStreamable();

            var report = Single();
            Assert.AreEqual(RowModeReason.PartitionedStream, report.Reason);
            StringAssert.Contains(report.Operator.GetType().Name, "IngressStreamable");
        }

        [TestMethod, TestCategory("Gated")]
        public void PartitionOperatorReports()
        {
            _ = Unfused().Partition(e => e.Key);

            var report = Single();
            Assert.AreEqual(RowModeReason.PartitionedStream, report.Reason);
            StringAssert.StartsWith(report.Operator.GetType().Name, "PartitionStreamable");
        }

        // An exception from the hook is counted, and the query is still built and still gives the right answer.
        [TestMethod, TestCategory("Gated")]
        public void HookFailureIsCountedAndTheQueryRuns()
        {
            Config.RowModeReporter = r => throw new InvalidOperationException("hook failed");
            var before = Config.RowModeReporterFailures;

            var counts = Ingress(new RowModeInitOnly { Key = 1 }, new RowModeInitOnly { Key = 1 })
                .TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c })
                .ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload.Count).ToEnumerable().ToList();

            Assert.AreEqual(1, Config.RowModeReporterFailures - before);
            CollectionAssert.AreEqual(new[] { 2UL }, counts);
        }

        [TestMethod, TestCategory("Gated")]
        public void NoReportsWhenRowModeIsForced()
        {
            using (new ConfigModifier().ForceRowBasedExecution(true).Modify())
            {
                Run(Ingress(new RowModeInitOnly { Key = 1 }).Select(e => new KeyCount { Key = e.Key }));
                _ = Unfused().Partition(e => e.Key);

                // A partitioned ingress always runs in row mode, so only the forced setting keeps it quiet.
                _ = new[] { PartitionedStreamEvent.CreatePoint(0, 1L, new CapturedInput { Key = 1 }) }.ToObservable().ToStreamable();
            }

            Assert.AreEqual(0, this.reports.Count);
        }

        [TestMethod, TestCategory("Gated")]
        public void NoReportsForAColumnarQuery()
        {
            Run(Input().TumblingWindowLifetime(5)
                .GroupAggregate(e => e.Key * 10, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c }));

            Assert.AreEqual(0, this.reports.Count);
        }

        // A block expression prints as C# that does not compile.
        private static Expression<Func<CapturedInput, bool>> BlockPredicate()
        {
            var x = Expression.Parameter(typeof(CapturedInput), "x");
            var odd = Expression.Equal(
                Expression.Modulo(Expression.Field(x, nameof(CapturedInput.Key)), Expression.Constant(2)),
                Expression.Constant(1));
            return Expression.Lambda<Func<CapturedInput, bool>>(Expression.Block(odd), x);
        }

        private static IStreamable<Empty, T> Ingress<T>(params T[] payloads)
            => payloads.Select((p, i) => StreamEvent.CreatePoint(i + 1, p)).ToObservable().ToStreamable();

        private static IStreamable<Empty, CapturedInput> Input()
            => Ingress(new CapturedInput { Key = 1 }, new CapturedInput { Key = 2 }, new CapturedInput { Key = 1 });

        // Trill folds a Select or Where straight after the ingress into the generated ingress. A Union first
        // keeps the operator under test separate.
        private static IStreamable<Empty, CapturedInput> Unfused() => Input().Union(Input());

        private static void Run<T>(IStreamable<Empty, T> result) => result.ToStreamEventObservable().ToEnumerable().ToList();

        private RowModeReport Single()
        {
            var all = this.reports.ToArray();
            Assert.AreEqual(1, all.Length, string.Join("; ", all.Select(r => r.ToString())));
            return all[0];
        }

        // Map-reduce builds its query more than once (in its constructor and again at Subscribe), and each build
        // makes new operators. Each operator reports once, and every report says the same thing.
        private RowModeReport OnePerBuild()
        {
            var all = this.reports.ToArray();
            Assert.IsTrue(all.Length > 0, "no report");
            Assert.AreEqual(all.Length, all.Select(r => r.Operator).Distinct(ReferenceEqualityComparer.Instance).Count(), "an operator reported twice");
            Assert.AreEqual(1, all.Select(r => new { Type = r.Operator.GetType(), r.Reason, r.NonColumnarType, r.Detail }).Distinct().Count(),
                string.Join("; ", all.Select(r => r.ToString())));
            return all[0];
        }
    }
}
