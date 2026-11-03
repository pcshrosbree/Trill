using System;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public static class MultiStringSelectHelpers
    {
        public static T Same<T>(T value) => value;
    }

    public struct MultiStringSource
    {
        public int A;
        public string Name;
    }

    public sealed class MultiStringResult
    {
        public string Name;

        public MultiStringResult() { }

        public MultiStringResult(string name) => this.Name = name;

        public MultiStringResult(int a) => this.Name = "i" + a;
    }

    public sealed class MultiStringPair
    {
        public string First;
        public int A;
        public string Second;

        public MultiStringPair() { }

        public MultiStringPair(string first, int a, string second)
        {
            this.First = first;
            this.A = a;
            this.Second = second;
        }
    }

    public abstract class MultiStringSelectAfterWhereTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private readonly bool columnar;

        internal MultiStringSelectAfterWhereTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(columnar).UseMultiString(true))
            => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void ConstructorWithAString()
            => Assert.AreEqual("n1 n3 n5", Names(Source().Where(s => s.A % 2 == 1).Select(s => new MultiStringResult(s.Name))));

        [TestMethod, TestCategory("Gated")]
        public void ConstructorWithAnInt()
            => Assert.AreEqual("i1 i3 i5", Names(Source().Where(s => s.A % 2 == 1).Select(s => new MultiStringResult(s.A))));

        [TestMethod, TestCategory("Gated")]
        public void ComputedMember()
            => Assert.AreEqual("n1! n3! n5!", Names(Source().Where(s => s.A % 2 == 1).Select(s => new MultiStringResult { Name = s.Name + "!" })));

        [TestMethod, TestCategory("Gated")]
        public void FirstRowsRemoved()
            => Assert.AreEqual("n4! n5!", Names(Source().Where(s => s.A > 3).Select(s => new MultiStringResult { Name = s.Name + "!" })));

        [TestMethod, TestCategory("Gated")]
        public void LastRowsRemoved()
            => Assert.AreEqual("n1 n2", Names(Source().Where(s => s.A < 3).Select(s => new MultiStringResult(s.Name))));

        [TestMethod, TestCategory("Gated")]
        public void TwoStringMembers()
        {
            var query = Source().Where(s => s.A % 2 == 0).Select(s => new MultiStringPair(s.Name, s.A, s.Name + "?"));
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            var output = query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload);
            Assert.AreEqual("n2/2/n2? n4/4/n4?", string.Join(" ", output.Select(p => p.First + "/" + p.A + "/" + p.Second)));
        }

        [TestMethod, TestCategory("Gated")]
        public void AnonymousResult()
        {
            var query = Source().Select(s => new { s.A, s.Name }).Where(s => s.A % 2 == 1).Select(s => MultiStringSelectHelpers.Same(s));
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            var output = query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.Name);
            Assert.AreEqual("n1 n3 n5", string.Join(" ", output));
        }

        [TestMethod, TestCategory("Gated")]
        public void WhereOnTheResult()
            => Assert.AreEqual("n3!", Names(Source().Where(s => s.A % 2 == 1).Select(s => new MultiStringResult { Name = s.Name + "!" })
                .Where(r => r.Name.StartsWith("n3"))));

        [TestMethod, TestCategory("Gated")]
        public void NoWhereControl()
            => Assert.AreEqual("n1 n2 n3 n4 n5", Names(Source().Select(s => new MultiStringResult(s.Name))));

        [TestMethod, TestCategory("Gated")]
        public void CopiedMemberControl()
            => Assert.AreEqual("n1 n3 n5", Names(Source().Where(s => s.A % 2 == 1).Select(s => new MultiStringResult { Name = s.Name })));

        // The Union keeps the Where and the Select out of the ingress.
        private static IStreamable<Empty, MultiStringSource> Source()
            => Enumerable.Range(1, 5).Select(i => StreamEvent.CreatePoint(i, new MultiStringSource { A = i, Name = "n" + i }))
                .ToObservable().ToStreamable()
                .Union(Array.Empty<StreamEvent<MultiStringSource>>().ToObservable().ToStreamable());

        private string Names(IStreamable<Empty, MultiStringResult> query)
        {
            Assert.AreEqual(this.columnar, query.Properties.IsColumnar);
            return string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.Name));
        }
    }

    [TestClass]
    public class MultiStringSelectAfterWhereTestsRow : MultiStringSelectAfterWhereTestsBase
    {
        public MultiStringSelectAfterWhereTestsRow() : base(false) { }
    }

    [TestClass]
    public class MultiStringSelectAfterWhereTestsColumnar : MultiStringSelectAfterWhereTestsBase
    {
        public MultiStringSelectAfterWhereTestsColumnar() : base(true) { }
    }
}
