using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct InitializerSource
    {
        public int A;
        public string S;
    }

    public sealed class InitializerClass
    {
        public InitializerClass() { }

        public InitializerClass(int a) => this.A = a;

        public InitializerClass(InitializerClass other) => this.A = other.A * 10;

        public int A { get; set; }

        public int B { get; set; }
    }

    public struct InitializerStruct
    {
        public int A;
        public int B;

        public InitializerStruct(int a)
        {
            this.A = a;
            this.B = 0;
        }
    }

    public sealed class InitializerText
    {
        public InitializerText() { }

        public InitializerText(int a) => this.A = a;

        public InitializerText(string s) => this.S = s;

        public int A { get; set; }

        public string S { get; set; }
    }

    public sealed class InitializerOverload
    {
        public InitializerOverload() { }

        public InitializerOverload(object value) => this.A = 1;

        public InitializerOverload(string value) => this.A = 2;

        public int A { get; set; }

        public int B { get; set; }
    }

    public sealed class InitializerShared
    {
        public InitializerShared() { }

        public InitializerShared(int a) => this.Left = this.Right = new object();

        public object Left { get; set; }

        public object Right { get; set; }

        public int B { get; set; }
    }

    // A Select whose body calls a constructor with arguments and then sets members must keep the
    // constructor's values. Columns cannot hold what such a constructor does, so the operator runs in row
    // mode. Every query reads from a Union or a GroupAggregate, so the operator under test is not fused into
    // the ingress.
    public abstract class SelectInitializerTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal SelectInitializerTestsBase(ConfigModifier modifier) : base(modifier.MapArity(1)) { }

        [TestMethod, TestCategory("Gated")]
        public void ClassConstructorWithInitializer()
            => Check(Separate().Select(s => new InitializerClass(s.A) { B = 7 }), p => p.A + ":" + p.B, "1:7 2:7 3:7", false);

        [TestMethod, TestCategory("Gated")]
        public void StructConstructorWithInitializer()
            => Check(Separate().Select(s => new InitializerStruct(s.A) { B = 7 }), q => q.A + ":" + q.B, "1:7 2:7 3:7", false);

        [TestMethod, TestCategory("Gated")]
        public void ConstructorWithInitializerThatReadsTheInput()
            => Check(Separate().Select(s => new InitializerClass(s.A) { B = s.A + 7 }), p => p.A + ":" + p.B, "1:8 2:9 3:10", false);

        [TestMethod, TestCategory("Gated")]
        public void ConstructorWithAStringOperationInTheInitializer()
            => Check(Separate().Select(s => new InitializerClass(s.A) { B = s.S.IndexOf("s") }), p => p.A + ":" + p.B, "1:0 2:0 3:0", false);

        [TestMethod, TestCategory("Gated")]
        public void ConstructorSetsAStringAfterAWhere()
            => Check(Separate().Where(s => s.A != 2).Select(s => new InitializerText(s.S) { A = s.A }), t => t.A + ":" + t.S, "1:s1 3:s3", false);

        [TestMethod, TestCategory("Gated")]
        public void ConstructorWithANullArgument()
            => Check(Separate().Select(s => new InitializerOverload((object)null) { B = s.A }), p => p.A + ":" + p.B, "1:1 1:2 1:3", false);

        [TestMethod, TestCategory("Gated")]
        public void ConstructorWithANestedInitializer()
            => Check(Separate().Select(s => new InitializerClass(new InitializerClass { A = s.A }) { B = 7 }), p => p.A + ":" + p.B, "10:7 20:7 30:7", false);

        // The constructor stores one object in both members.
        [TestMethod, TestCategory("Gated")]
        public void ConstructorSharesAnObjectBetweenMembers()
            => Check(Separate().Select(s => new InitializerShared(s.A) { B = s.A }), p => ReferenceEquals(p.Left, p.Right) + ":" + p.B, "True:1 True:2 True:3", false);

        [TestMethod, TestCategory("Gated")]
        public void JoinResultConstructorWithInitializer()
        {
            var query = Separate().Join(Separate(), l => l.A, r => r.A, (l, r) => new InitializerClass(l.A) { B = r.A + 7 });
            Check(query, p => p.A + ":" + p.B, "1:8 2:9 3:10", false);
        }

        [TestMethod, TestCategory("Gated")]
        public void GroupAggregateResultConstructorWithInitializer()
            => Check(Grouped((g, c) => new InitializerClass(g.Key) { B = (int)c }), p => p.A + ":" + p.B, "1:1 2:1 3:1", false);

        // GroupApply's result selector goes to Ungroup.
        [TestMethod, TestCategory("Gated")]
        public void GroupApplyResultConstructorWithInitializer()
        {
            var query = Separate().GroupApply(s => s.A, w => w.Count(), (g, c) => new InitializerClass(g.Key) { B = (int)c });
            Check(query, p => p.A + ":" + p.B, "1:1 1:1 2:1 2:1 3:1 3:1", false);
        }

        [TestMethod, TestCategory("Gated")]
        public void SelectManyRepeatsAConstructorWithInitializer()
            => Check(Separate().SelectMany(s => Enumerable.Repeat(new InitializerClass(s.A) { B = 7 }, 1)), p => p.A + ":" + p.B, "1:7 2:7 3:7", false);

        // Control: a constructor call with no initializer.
        [TestMethod, TestCategory("Gated")]
        public void ConstructorOnly()
            => Check(Separate().Select(s => new InitializerClass(s.A)), p => p.A + ":" + p.B, "1:0 2:0 3:0", true);

        // Control: an initializer with no constructor arguments.
        [TestMethod, TestCategory("Gated")]
        public void InitializerOnly()
            => Check(Separate().Select(s => new InitializerClass { A = s.A, B = 7 }), p => p.A + ":" + p.B, "1:7 2:7 3:7", true);

        // Control: with no constructor arguments, a member the initializer does not set keeps its default.
        [TestMethod, TestCategory("Gated")]
        public void InitializerOnlySetsSomeMembers()
            => Check(Separate().Select(s => new InitializerClass { B = s.A }), p => p.A + ":" + p.B, "0:1 0:2 0:3", true);

        // The initializer sets every member, but the constructor still runs in row mode.
        [TestMethod, TestCategory("Gated")]
        public void InitializerSetsEveryMemberAfterAWhere()
            => Check(Separate().Where(s => s.A != 2).Select(s => new InitializerText(0) { A = s.A, S = s.S }), t => t.A + ":" + t.S, "1:s1 3:s3", false);

        [TestMethod, TestCategory("Gated")]
        public void InitializerSetsEveryMemberAfterANestedInitializer()
            => Check(Separate().Select(s => new InitializerClass(new InitializerClass { A = 5 }) { A = s.A, B = 7 }), p => p.A + ":" + p.B, "1:7 2:7 3:7", false);

        [TestMethod, TestCategory("Gated")]
        public void GroupAggregateInitializerSetsEveryMember()
            => Check(Grouped((g, c) => new InitializerClass(0) { A = g.Key, B = (int)c }), p => p.A + ":" + p.B, "1:1 2:1 3:1", false);

        private static IStreamable<Empty, InitializerSource> Separate()
            => Enumerable.Range(1, 3)
                .Select(i => StreamEvent.CreatePoint(i, new InitializerSource { A = i, S = "s" + i }))
                .ToObservable()
                .ToStreamable()
                .Union(Array.Empty<StreamEvent<InitializerSource>>().ToObservable().ToStreamable());

        // With start edges only and MapArity 1, GroupAggregate builds a GroupedWindowStreamable.
        private static IStreamable<Empty, InitializerClass> Grouped(
            Expression<Func<GroupSelectorInput<int>, ulong, InitializerClass>> result)
        {
            var query = Enumerable.Range(1, 3)
                .Select(i => StreamEvent.CreateStart(i, new InitializerSource { A = i }))
                .ToObservable()
                .ToStreamable()
                .SetProperty().IsConstantDuration(true, StreamEvent.InfinitySyncTime)
                .GroupAggregate(s => s.A, w => w.Count(), result);
            StringAssert.StartsWith(query.GetType().Name, "GroupedWindowStreamable");
            return query;
        }

        private static void Check<T>(IStreamable<Empty, T> query, Func<T, string> show, string expected, bool columnar)
        {
            var values = query.ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => show(e.Payload))
                .ToEnumerable()
                .OrderBy(v => v, StringComparer.Ordinal);
            Assert.AreEqual(expected, string.Join(" ", values));
            Assert.AreEqual(columnar && !Config.ForceRowBasedExecution, query.Properties.IsColumnar, "columnar");
        }
    }

    [TestClass]
    public class SelectInitializerTestsRow : SelectInitializerTestsBase
    {
        public SelectInitializerTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class SelectInitializerTestsColumnar : SelectInitializerTestsBase
    {
        public SelectInitializerTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }

    [TestClass]
    public class SelectInitializerTestsColumnarMultiString : SelectInitializerTestsBase
    {
        public SelectInitializerTestsColumnarMultiString()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)
                .UseMultiString(true).MultiStringTransforms(Config.CodegenOptions.MultiStringFlags.VectorOperations)) { }
    }
}
