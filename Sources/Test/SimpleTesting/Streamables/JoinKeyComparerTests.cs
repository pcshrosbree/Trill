using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct JoinKeyComparerEvent
    {
        public int Key;
    }

    public struct JoinKeyComparerPair
    {
        public int Outer;
        public int Inner;
    }

    // A key type whose default comparer cannot be built: it implements its own comparer, and Trill builds that default
    // with the parameterless constructor, which throws.
    public sealed class JoinKeyComparerKey : IEqualityComparerExpression<JoinKeyComparerKey>
    {
        public int Value;

        public JoinKeyComparerKey() => throw new InvalidOperationException("no default comparer");

        public JoinKeyComparerKey(int value) => this.Value = value;

        public Expression<Func<JoinKeyComparerKey, JoinKeyComparerKey, bool>> GetEqualsExpr() => (a, b) => a.Value == b.Value;

        public Expression<Func<JoinKeyComparerKey, int>> GetGetHashCodeExpr() => a => a.Value;
    }

    // Join and WhereNotExists with an equality comparer for the key selector on one input or both. The comparer
    // (a % 2 == b % 2, hash a % 2) agrees with default equality on keys 1 and 2, so it must not change the result.
    // Generated grouping code is cached by key selector text, so each test uses key selector texts of its own.
    public abstract class JoinKeyComparerTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const string AllMatches = "11, 11, 11, 11, 22";

        private static readonly long[] Times = { 1, 2, 4 };

        internal JoinKeyComparerTestsBase(bool columnar, bool strict)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(strict)) { }

        // Keys 1, 2, 1 by default, at times 1, 2 and 4, each lasting 10 ticks.
        private static IStreamable<Empty, JoinKeyComparerEvent> Input(int first = 1, int second = 2)
            => new[] { first, second, first }.Select((k, i) => StreamEvent.CreateInterval(Times[i], Times[i] + 10, new JoinKeyComparerEvent { Key = k }))
                .ToObservable().ToStreamable();

        // One event per key, all from time 1 to time 11.
        private static IStreamable<Empty, JoinKeyComparerEvent> Simultaneous(params int[] keys)
            => keys.Select(k => StreamEvent.CreateInterval(1, 11, new JoinKeyComparerEvent { Key = k })).ToObservable().ToStreamable();

        private static IStreamable<Empty, JoinKeyComparerEvent> WithComparer(IStreamable<Empty, JoinKeyComparerEvent> input, Expression<Func<JoinKeyComparerEvent, int>> keySelector)
            => input.SetProperty().EqualityComparer(keySelector, (a, b) => a % 2 == b % 2, a => a % 2);

        private static IStreamable<Empty, JoinKeyComparerEvent> WithComparer(Expression<Func<JoinKeyComparerEvent, int>> keySelector)
            => WithComparer(Input(), keySelector);

        private static string Payloads(IStreamable<Empty, int> query)
            => string.Join(", ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload).OrderBy(x => x));

        private static string Join(
            IStreamable<Empty, JoinKeyComparerEvent> left,
            IStreamable<Empty, JoinKeyComparerEvent> right,
            Expression<Func<JoinKeyComparerEvent, int>> leftKey,
            Expression<Func<JoinKeyComparerEvent, int>> rightKey,
            OperationalHint joinOptions = OperationalHint.None)
            => Payloads(left.Join(right, leftKey, rightKey, (l, r) => l.Key * 10 + r.Key, joinOptions));

        [TestMethod, TestCategory("Gated")]
        public void ComparerOnTheLeftWithADifferentRightKeySelector()
            => Assert.AreEqual(AllMatches, Join(WithComparer(e => e.Key), Input(), e => e.Key, e => e.Key + 0));

        [TestMethod, TestCategory("Gated")]
        public void ComparerOnTheLeft()
            => Assert.AreEqual(AllMatches, Join(WithComparer(e => e.Key - 0), Input(), e => e.Key - 0, e => e.Key - 0));

        [TestMethod, TestCategory("Gated")]
        public void ComparerOnTheRight()
            => Assert.AreEqual(AllMatches, Join(Input(), WithComparer(e => e.Key * 1), e => e.Key * 1, e => e.Key * 1));

        [TestMethod, TestCategory("Gated")]
        public void ComparerOnBothSides()
            => Assert.AreEqual(AllMatches, Join(WithComparer(e => e.Key / 1), WithComparer(e => e.Key / 1), e => e.Key / 1, e => e.Key / 1));

        [TestMethod, TestCategory("Gated")]
        public void NoComparer()
            => Assert.AreEqual(AllMatches, Join(Input(), Input(), e => e.Key | 0, e => e.Key | 0));

        // Every left event has a right event with the same key at the same time.
        [TestMethod, TestCategory("Gated")]
        public void WhereNotExistsComparerOnTheLeft()
        {
            var result = WithComparer(e => e.Key ^ 0).WhereNotExists(Input(), e => e.Key ^ 0, e => e.Key ^ 0)
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.Key + " at " + e.StartTime);
            Assert.AreEqual(string.Empty, string.Join(", ", result));
        }

        // With the same comparer on both inputs, the join still matches keys with it: 1 matches 3, and 2 matches 4.
        [TestMethod, TestCategory("Gated")]
        public void ComparerOnBothSidesMatchesWithTheComparer()
            => Assert.AreEqual("13, 13, 13, 13, 24", Join(WithComparer(e => e.Key + 1 - 1), WithComparer(Input(3, 4), e => e.Key + 1 - 1), e => e.Key + 1 - 1, e => e.Key + 1 - 1));

        [TestMethod, TestCategory("Gated")]
        public void ComparerOnTheLeftWithAnAsymmetricHint()
            => Assert.AreEqual(AllMatches, Join(WithComparer(e => e.Key + 2 - 2), Input(), e => e.Key + 2 - 2, e => e.Key + 2 - 2, OperationalHint.Asymmetric));

        [TestMethod, TestCategory("Gated")]
        public void ComparerOnTheRightWithAnAsymmetricHint()
            => Assert.AreEqual(AllMatches, Join(Input(), WithComparer(e => e.Key + 3 - 3), e => e.Key + 3 - 3, e => e.Key + 3 - 3, OperationalHint.Asymmetric));

        // A reducer written with Map and Reduce that joins the two grouped inputs. Only the left input has the comparer,
        // and both use the same key selector text.
        [TestMethod, TestCategory("Gated")]
        public void ReduceThatJoins()
        {
            var left = WithComparer(e => e.Key * 2 / 2).Map(e => e.Key * 2 / 2);
            var right = Input().Map(e => e.Key * 2 / 2);
            Assert.AreEqual(AllMatches, Payloads(left.Reduce(right, (l, r) => l.Join(r, (a, b) => a.Key * 10 + b.Key), (g, x) => x)));
        }

        [TestMethod, TestCategory("Gated")]
        public void ReduceThatUsesWhereNotExists()
        {
            var left = WithComparer(e => e.Key * 3 / 3).Map(e => e.Key * 3 / 3);
            var right = Input().Map(e => e.Key * 3 / 3);
            var result = left.Reduce(right, (l, r) => l.WhereNotExists(r), (g, x) => x)
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.Key);
            Assert.AreEqual(string.Empty, string.Join(", ", result));
        }

        [TestMethod, TestCategory("Gated")]
        public void ReduceThatClips()
        {
            var left = new[] { StreamEvent.CreateInterval(1, 11, new JoinKeyComparerEvent { Key = 2 }) }.ToObservable().ToStreamable()
                .SetProperty().EqualityComparer(e => e.Key * 4 / 4, (a, b) => a % 2 == b % 2, a => a % 2).Map(e => e.Key * 4 / 4);
            var right = new[] { StreamEvent.CreateInterval(5, 15, new JoinKeyComparerEvent { Key = 2 }) }.ToObservable().ToStreamable().Map(e => e.Key * 4 / 4);
            var result = left.Reduce(right, (l, r) => l.ClipEventDuration(r), (g, x) => x)
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData && e.EndTime != StreamEvent.InfinitySyncTime).Select(e => e.StartTime + "-" + e.EndTime);
            Assert.AreEqual("1-5", string.Join(", ", result));
        }

        // String keys "a", "bb", "a", with a length comparer on the left only. Every left event has a match.
        [TestMethod, TestCategory("Gated")]
        public void WhereNotExistsOnStringKeys()
        {
            var left = Input().SetProperty().EqualityComparer(e => e.Key == 1 ? "a" : "bb", (a, b) => a.Length == b.Length, a => a.Length);
            var result = left.WhereNotExists(Input(), e => e.Key == 1 ? "a" : "bb", e => e.Key == 1 ? "a" : "bb")
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.Key);
            Assert.AreEqual(string.Empty, string.Join(", ", result));
        }

        // The left grouped input feeds both a join, which rehashes it to the default comparer, and a count, which
        // groups keys 1 and 3 together with the left comparer. The rehash must not change the count's batches.
        [TestMethod, TestCategory("Gated")]
        public void ReduceThatJoinsAndCountsTheSameInput()
        {
            var left = Simultaneous(1, 3).SetProperty().EqualityComparer(e => e.Key * 5 / 5, (a, b) => a % 2 == b % 2, a => a % 2).Map(e => e.Key * 5 / 5);
            var right = Simultaneous(1).Map(e => e.Key * 5 / 5);
            var result = left.Reduce(right, (l, r) => l.Join(r, (a, b) => a.Key * 10 + b.Key).Union(l.Count().Select(c => 1000 + (int)c)), (g, x) => x);
            Assert.AreEqual("11, 1002", string.Join(", ", result.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload).Distinct().OrderBy(x => x)));
        }

        // A reducer written with Map and Reduce still sees each input grouped with that input's own comparer. The left
        // comparer puts keys 1 and 3 in one group, whose count is 2, and the right group of key 1 has a count of 1.
        [TestMethod, TestCategory("Gated")]
        public void ReduceThatCountsFirstKeepsEachInputsComparer()
        {
            var left = Simultaneous(1, 3).SetProperty().EqualityComparer(e => e.Key + 4 - 4, (a, b) => a % 2 == b % 2, a => a % 2).Map(e => e.Key + 4 - 4);
            var right = Simultaneous(1).Map(e => e.Key + 5 - 5);
            var result = left.Reduce(right, (l, r) => l.Count().Join(r.Count(), (a, b) => (int)(a * 10 + b)), (g, x) => x);
            Assert.AreEqual("21", string.Join(", ", result.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload).Distinct()));
        }

        // When the inputs' comparers differ, Join compares keys with default equality, so the left key 3 does not
        // match the right key 1, although the left comparer would match them.
        [TestMethod, TestCategory("Gated")]
        public void JoinWithDifferentComparersUsesDefaultEquality()
        {
            var left = Simultaneous(3).SetProperty().EqualityComparer(e => e.Key + 9 - 9, (a, b) => a % 2 == b % 2, a => a % 2);
            Assert.AreEqual(string.Empty, Payloads(left.Join(Simultaneous(1), e => e.Key + 9 - 9, e => e.Key + 10 - 10, (l, r) => l.Key * 10 + r.Key)));
        }

        // A join inside a group whose key has a comparer, with a comparer on one input's join key, followed by an
        // aggregate. The aggregate must see the same key hashes that its stream's properties describe.
        [TestMethod, TestCategory("Gated")]
        public void JoinInsideAGroupThenCount()
        {
            var input = new[] { StreamEvent.CreateInterval(1, 11, new JoinKeyComparerPair { Outer = 2, Inner = 1 }) }.ToObservable().ToStreamable()
                .SetProperty().EqualityComparer(e => e.Outer, (a, b) => a % 2 == b % 2, a => a % 2);
            var query = input.GroupApply(
                e => e.Outer,
                g => g.SetProperty().EqualityComparer(e => e.Inner, (a, b) => a % 2 == b % 2, a => a % 2)
                    .Join(g, e => e.Inner, e => e.Inner, (l, r) => l.Inner + r.Inner).Count(),
                (g, c) => (int)c);
            Assert.AreEqual("1", string.Join(", ", query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload).Distinct()));
        }

        // WhereNotExists compares keys with the left input's comparer, which matches the left key 3 with the right key 1.
        [TestMethod, TestCategory("Gated")]
        public void WhereNotExistsKeepsTheLeftComparer()
        {
            var left = Simultaneous(3).SetProperty().EqualityComparer(e => e.Key + 6 - 6, (a, b) => a % 2 == b % 2, a => a % 2);
            var result = left.WhereNotExists(Simultaneous(1), e => e.Key + 6 - 6, e => e.Key + 7 - 7)
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload.Key);
            Assert.AreEqual(string.Empty, string.Join(", ", result));
        }

        // The right event with key 2 starts at time 5, so it clips the left event with key 2 to end at 5.
        [TestMethod, TestCategory("Gated")]
        public void ClipEventDurationComparerOnTheLeft()
        {
            var left = new[] { StreamEvent.CreateInterval(1, 11, new JoinKeyComparerEvent { Key = 2 }) }.ToObservable().ToStreamable()
                .SetProperty().EqualityComparer(e => e.Key + 8 - 8, (a, b) => a % 2 == b % 2, a => a % 2);
            var right = new[] { StreamEvent.CreateInterval(5, 15, new JoinKeyComparerEvent { Key = 2 }) }.ToObservable().ToStreamable();
            var result = left.ClipEventDuration(right, e => e.Key + 8 - 8, e => e.Key + 8 - 8)
                .ToStreamEventObservable().ToEnumerable().Where(e => e.IsData && e.EndTime != StreamEvent.InfinitySyncTime).Select(e => e.StartTime + "-" + e.EndTime);
            Assert.AreEqual("1-5", string.Join(", ", result));
        }

        // The mappers register the comparer, and the key type has no usable default comparer, which nothing needs here.
        [TestMethod, TestCategory("Gated")]
        public void ReduceWithMapperComparersNeedsNoDefaultComparer()
        {
            IStreamable<Empty, JoinKeyComparerEvent> Mapper(IStreamable<Empty, JoinKeyComparerEvent> s)
                => s.SetProperty().EqualityComparer(e => new JoinKeyComparerKey(e.Key), (a, b) => a.Value == b.Value, a => a.Value);
            var left = Simultaneous(1).Map(Mapper, e => new JoinKeyComparerKey(e.Key));
            var right = Simultaneous(1).Map(Mapper, e => new JoinKeyComparerKey(e.Key));
            var result = left.Reduce(right, (l, r) => l.Join(r, (a, b) => a.Key * 10 + b.Key), (g, x) => x);
            Assert.AreEqual("11", string.Join(", ", result.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => e.Payload).Distinct()));
        }
    }

    [TestClass]
    public class JoinKeyComparerTestsRow : JoinKeyComparerTestsBase
    {
        public JoinKeyComparerTestsRow() : base(false, false) { }
    }

    [TestClass]
    public class JoinKeyComparerTestsColumnar : JoinKeyComparerTestsBase
    {
        public JoinKeyComparerTestsColumnar() : base(true, false) { }
    }

    [TestClass]
    public class JoinKeyComparerTestsColumnarStrict : JoinKeyComparerTestsBase
    {
        public JoinKeyComparerTestsColumnarStrict() : base(true, true) { }
    }
}
