using System;
using System.Linq;
using System.Reactive.Linq;
using System.Runtime.CompilerServices;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [InlineArray(2)]
    public struct FollowupPair
    {
        public int Element;
    }

    // Columnar execution with no fallback to row mode. The second element is 0, so the expected result does not depend
    // on the fix for #104: before that fix, a columnar inline array kept only its first element.
    [TestClass]
    public class RowToColumnInlineArrayTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public RowToColumnInlineArrayTests()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }

        private static FollowupPair Of(int first, int second)
        {
            var pair = default(FollowupPair);
            pair[0] = first;
            pair[1] = second;
            return pair;
        }

        // The Union keeps the query from being fused into ingress.
        private static IStreamable<Empty, FollowupPair> Separate(FollowupPair pair)
            => new[] { StreamEvent.CreatePoint(1, pair) }.ToObservable().ToStreamable()
                .Union(Array.Empty<StreamEvent<FollowupPair>>().ToObservable().ToStreamable());

        private static string Values(IStreamable<Empty, FollowupPair> query)
            => string.Join(" ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData)
                .Select(e => "[" + e.Payload[0] + ", " + e.Payload[1] + "]"));

        [TestMethod]
        public void WithoutConversion() => Assert.AreEqual("[3, 0]", Values(Separate(Of(3, 0))));

        [TestMethod]
        public void ColumnToRowThenRowToColumn() => Assert.AreEqual("[3, 0]", Values(Separate(Of(3, 0)).ColumnToRow().RowToColumn()));
    }
}
