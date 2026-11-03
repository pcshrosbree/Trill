// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // Trill caches the result of columnar code generation for each operator, and a failure is cached as a
    // fallback to row mode. With DontFallBackToRowBasedExecution set, a cached failure must throw just as a new
    // failure does. Each test uses its own constant in the query, so that its cache entries are its own whatever
    // ran earlier in the process.
    [TestClass]
    public class CachedFallbackTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const string StrictFailure = "Code Generation failed when it wasn't supposed to! Operator: ";

        private ConcurrentQueue<RowModeReport> reports;

        public CachedFallbackTests()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }

        [TestInitialize]
        public void StartCollecting()
        {
            var queue = new ConcurrentQueue<RowModeReport>();
            this.reports = queue;
            Config.RowModeReporter = queue.Enqueue;
        }

        [TestCleanup]
        public void StopCollecting() => Config.RowModeReporter = null;

        // A Where straight after the ingress is folded into the generated ingress, as in the issue's sample.
        [TestMethod, TestCategory("Gated")]
        public void FusedIngressThrowsOnACachedFallbackWhenFallbackIsOff()
        {
            var fallback = RunAllowingFallback(() => Input().Where(BlockPredicate(101)));
            StringAssert.Contains(fallback.Operator.GetType().Name, "IngressStreamable");

            var cached = ThrowsWithFallbackOff(() => Input().Where(BlockPredicate(101)));
            Assert.AreSame(fallback.Exception, cached.InnerException, "the exception that stopped code generation");
            StringAssert.Contains(cached.Message, "Compiler diagnostics:\n");
            StringAssert.Contains(cached.Message, "error CS");

            // A query that has not been built before fails in its generator. The cached failure must give the
            // same message, up to the compiler diagnostics, which name the generated class.
            var fresh = ThrowsWithFallbackOff(() => Input().Where(BlockPredicate(102)));
            Assert.AreEqual(UpToDiagnostics(fresh.Message), UpToDiagnostics(cached.Message));
            Assert.AreEqual(0, this.reports.Count, "a failure that throws is not reported");
        }

        // The same for an operator that is not folded into the ingress.
        [TestMethod, TestCategory("Gated")]
        public void WhereThrowsOnACachedFallbackWhenFallbackIsOff()
        {
            var fallback = RunAllowingFallback(() => Unfused().Where(BlockPredicate(103)));
            StringAssert.StartsWith(fallback.Operator.GetType().Name, "WhereStreamable");

            var cached = ThrowsWithFallbackOff(() => Unfused().Where(BlockPredicate(103)));
            Assert.AreSame(fallback.Exception, cached.InnerException);

            var fresh = ThrowsWithFallbackOff(() => Unfused().Where(BlockPredicate(104)));
            Assert.AreEqual(UpToDiagnostics(fresh.Message), UpToDiagnostics(cached.Message));
            StringAssert.Contains(cached.Message, "Operator: Where");
        }

        // Throwing does not remove the cached failure: with fallback allowed again, the query runs in row mode,
        // gives the right answer, and reports again.
        [TestMethod, TestCategory("Gated")]
        public void ACachedFallbackStillRunsWhenFallbackIsAllowedAgain()
        {
            RunAllowingFallback(() => Input().Where(BlockPredicate(105)));
            ThrowsWithFallbackOff(() => Input().Where(BlockPredicate(105)));

            var query = Input().Where(BlockPredicate(105));
            var keys = query.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload.Key).ToEnumerable().ToList();
            CollectionAssert.AreEqual(new[] { 1, 1 }, keys);
            Assert.IsFalse(query.Properties.IsColumnar);
            Assert.AreEqual(RowModeReason.CodeGenerationFailed, Single().Reason);
        }

        // A generator can also decline without an exception, as Select does for a selector that it cannot turn
        // into columnar code. That is not a failure to compile, and it falls back to row mode even when fallback
        // is disabled. The new check must leave it alone, whether or not the result is cached.
        [TestMethod, TestCategory("Gated")]
        public void ADeclinedGenerationStillFallsBackWhenFallbackIsOff()
        {
            var declined = RunAllowingFallback(() => Unfused().Select(NestedLambdaSelector(106)));
            StringAssert.StartsWith(declined.Operator.GetType().Name, "SelectStreamable");
            Assert.IsNull(declined.Exception, "Select declined without an exception");

            using (new ConfigModifier().DontFallBackToRowBasedExecution(true).Modify())
            {
                var query = Unfused().Select(NestedLambdaSelector(106));
                var keys = query.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload.Key).ToEnumerable().OrderBy(k => k).ToList();
                CollectionAssert.AreEqual(new[] { 107, 107, 107, 107, 108, 108 }, keys);
                Assert.IsFalse(query.Properties.IsColumnar);
            }

            Assert.AreEqual(RowModeReason.CodeGenerationFailed, Single().Reason);
        }

        private static InvalidOperationException ThrowsWithFallbackOff<T>(Func<IStreamable<Empty, T>> build)
        {
            using (new ConfigModifier().DontFallBackToRowBasedExecution(true).Modify())
            {
                var error = Assert.ThrowsException<InvalidOperationException>(() => Run(build()));
                StringAssert.StartsWith(error.Message, StrictFailure);
                return error;
            }
        }

        private static string UpToDiagnostics(string message)
        {
            var end = message.IndexOf(" Compiler diagnostics:", StringComparison.Ordinal);
            return end < 0 ? message : message.Substring(0, end);
        }

        // A block expression prints as C# that does not compile, so code generation throws.
        private static Expression<Func<CapturedInput, bool>> BlockPredicate(int modulus)
        {
            var x = Expression.Parameter(typeof(CapturedInput), "x");
            var test = Expression.NotEqual(
                Expression.Modulo(Expression.Field(x, nameof(CapturedInput.Key)), Expression.Constant(modulus)),
                Expression.Constant(2));
            return Expression.Lambda<Func<CapturedInput, bool>>(Expression.Block(test), x);
        }

        // A lambda inside the selector: Select's transformer declines it before any code is compiled.
        private static Expression<Func<CapturedInput, KeyCount>> NestedLambdaSelector(int offset)
        {
            Expression<Func<int, int>> add = k => k + offset;
            var e = Expression.Parameter(typeof(CapturedInput), "e");
            var key = Expression.Invoke(add, Expression.Field(e, nameof(CapturedInput.Key)));
            var body = Expression.MemberInit(
                Expression.New(typeof(KeyCount)),
                Expression.Bind(typeof(KeyCount).GetField(nameof(KeyCount.Key)), key));
            return Expression.Lambda<Func<CapturedInput, KeyCount>>(body, e);
        }

        private static IStreamable<Empty, CapturedInput> Input()
            => new[] { 1, 2, 1 }.Select((k, i) => StreamEvent.CreatePoint(i + 1, new CapturedInput { Key = k })).ToObservable().ToStreamable();

        // Trill folds a Select or Where straight after the ingress into the generated ingress. A Union first
        // keeps the operator under test separate.
        private static IStreamable<Empty, CapturedInput> Unfused() => Input().Union(Input());

        private static void Run<T>(IStreamable<Empty, T> result) => result.ToStreamEventObservable().ToEnumerable().ToList();

        // Builds and runs the query with fallback allowed, checks that it ran in row mode because code generation
        // failed, and returns the one report it made.
        private RowModeReport RunAllowingFallback<T>(Func<IStreamable<Empty, T>> build)
        {
            var query = build();
            Run(query);
            Assert.IsFalse(query.Properties.IsColumnar);
            var report = Single();
            Assert.AreEqual(RowModeReason.CodeGenerationFailed, report.Reason);
            this.reports = new ConcurrentQueue<RowModeReport>();
            Config.RowModeReporter = this.reports.Enqueue;
            return report;
        }

        private RowModeReport Single()
        {
            var all = this.reports.ToArray();
            Assert.AreEqual(1, all.Length, string.Join("; ", all.Select(r => r.ToString())));
            return all[0];
        }
    }
}
