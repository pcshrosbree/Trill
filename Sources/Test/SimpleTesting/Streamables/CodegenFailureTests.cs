using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // With fallback to row mode disabled, a failed code generation must say which operator failed and why.
    [TestClass]
    public class CodegenFailureTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public CodegenFailureTests()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }

        // The key selector reads a captured local, a field of the compiler's closure class. The generated
        // grouping code names that class, which is not a valid C# name, so it does not compile.
        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalNamesOperatorAndKeepsCause()
        {
            int mult = 10;
            var e = Assert.ThrowsException<InvalidOperationException>(() => Run(Input()
                .TumblingWindowLifetime(5)
                .GroupAggregate(x => x.Key * mult, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c })));

            AssertContains(e.Message, "Operator: Group.");
            AssertContains(e.Message, "error CS");
            Assert.IsNotNull(e.InnerException);
        }

        // The join transformer calls a shared generator, which throws first. The join's own exception must
        // still carry that generator's compiler errors in its message.
        [TestMethod, TestCategory("Gated")]
        public void NestedGeneratorKeepsCompilerDiagnostics()
        {
            var l = Expression.Parameter(typeof(CapturedInput), "l");
            var r = Expression.Parameter(typeof(CapturedInput), "r");
            var keyCount = Expression.MemberInit(
                Expression.New(typeof(KeyCount)),
                Expression.Bind(typeof(KeyCount).GetField(nameof(KeyCount.Key)), Expression.Field(l, nameof(CapturedInput.Key))),
                Expression.Bind(typeof(KeyCount).GetField(nameof(KeyCount.Count)), Expression.Constant(1UL)));
            var resultSelector = Expression.Lambda<Func<CapturedInput, CapturedInput, KeyCount>>(Expression.Block(keyCount), l, r);

            var e = Assert.ThrowsException<InvalidOperationException>(() => Run(Input()
                .Join(Input(), x => x.Key, y => y.Key, resultSelector)));

            AssertContains(e.Message, "Operator: EquiJoin.");
            AssertContains(e.Message, "error CS");
            Assert.IsNotNull(e.InnerException);
        }

        // A block expression prints as C# that does not compile, so the message must carry the compiler's errors.
        // Trill folds this Where into the generated ingress, so the ingress is the operator that fails.
        [TestMethod, TestCategory("Gated")]
        public void CompileErrorKeepsCompilerDiagnostics()
        {
            var x = Expression.Parameter(typeof(CapturedInput), "x");
            var odd = Expression.Equal(
                Expression.Modulo(Expression.Field(x, nameof(CapturedInput.Key)), Expression.Constant(2)),
                Expression.Constant(1));
            var predicate = Expression.Lambda<Func<CapturedInput, bool>>(Expression.Block(odd), x);

            var e = Assert.ThrowsException<InvalidOperationException>(() => Run(Input()
                .Where(predicate)
                .TumblingWindowLifetime(5)
                .GroupAggregate(y => y.Key, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c })));

            AssertContains(e.Message, "Operator: TemporalIngressTemplate.");
            AssertContains(e.Message, "Compiler diagnostics:");
            AssertContains(e.Message, "error CS");
            Assert.IsNotNull(e.InnerException);
        }

        // StringAssert.Contains formats its failure message with the actual text, so braces in generated
        // code make it throw FormatException. AreEqual shows the whole message when the text is missing.
        private static void AssertContains(string actual, string expected)
        {
            if (!actual.Contains(expected)) Assert.AreEqual(expected, actual);
        }

        private static IStreamable<Empty, CapturedInput> Input()
            => new[] { 1, 2, 1 }
                .Select((k, i) => StreamEvent.CreatePoint(i + 1, new CapturedInput { Key = k }))
                .ToObservable()
                .ToStreamable();

        private static void Run(IStreamable<Empty, KeyCount> result)
            => result.ToStreamEventObservable().ToEnumerable().ToList();
    }
}
