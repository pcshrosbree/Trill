using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

// StyleCop 1.0.2 predates required members and records, so it misreads both.
#pragma warning disable SA1206, SA1313

namespace SimpleTesting
{
    public sealed class InitOnlyPayload
    {
        public required int Key { get; init; }
    }

    public readonly record struct ReadonlyRecordPayload(int Key);

    public readonly struct GetOnlyPayload
    {
        public GetOnlyPayload(int key) => this.Key = key;

        public int Key { get; }
    }

    public readonly struct ReadonlyFieldPayload
    {
        public readonly int Key;

        public ReadonlyFieldPayload(int key) => this.Key = key;
    }

    // Key is the only public field, but the value also stores Hidden.
    public struct HiddenGetOnlyPayload
    {
        public int Key;

        public HiddenGetOnlyPayload(int key, int hidden)
        {
            this.Key = key;
            this.Hidden = hidden;
        }

        public int Total => this.Key + this.Hidden;

        private int Hidden { get; }
    }

    public sealed class KeyCount
    {
        public int Key;
        public ulong Count;
    }

    // A columnar batch rebuilds each payload by assigning its members one at a time, which these
    // types do not allow. Each must run in row mode instead of failing when the query starts.
    public abstract class ImmutablePayloadTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // Keys 1, 2 and 1 fall in the window that ends at 5; keys 2 and 1 in the one that ends at 10.
        private static readonly int[] Keys = { 1, 2, 1, 2, 1 };
        private static readonly long[] Times = { 1, 2, 4, 7, 9 };
        private static readonly string[] Expected = { "5:1=2", "5:2=1", "10:1=1", "10:2=1" };

        internal ImmutablePayloadTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestMethod, TestCategory("Gated")]
        public void InitOnlyProperties()
            => CollectionAssert.AreEquivalent(Expected, CountByKey(k => new InitOnlyPayload { Key = k }, e => e.Key));

        [TestMethod, TestCategory("Gated")]
        public void ReadonlyRecordStruct()
            => CollectionAssert.AreEquivalent(Expected, CountByKey(k => new ReadonlyRecordPayload(k), e => e.Key));

        [TestMethod, TestCategory("Gated")]
        public void GetOnlyProperties()
            => CollectionAssert.AreEquivalent(Expected, CountByKey(k => new GetOnlyPayload(k), e => e.Key));

        [TestMethod, TestCategory("Gated")]
        public void ReadonlyFields()
            => CollectionAssert.AreEquivalent(Expected, CountByKey(k => new ReadonlyFieldPayload(k), e => e.Key));

        // A private get-only property has storage too, so a columnar batch would drop its value.
        [TestMethod, TestCategory("Gated")]
        public void PrivateGetOnlyProperty()
        {
            var totals = Enumerable.Range(0, Keys.Length)
                .Select(i => StreamEvent.CreatePoint(Times[i], new HiddenGetOnlyPayload(Keys[i], 100)))
                .ToObservable()
                .ToStreamable()
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload.Total)
                .ToEnumerable()
                .ToList();

            CollectionAssert.AreEqual(new[] { 101, 102, 101, 102, 101 }, totals);
        }

        private static List<string> CountByKey<T>(
            System.Func<int, T> create,
            System.Linq.Expressions.Expression<System.Func<T, int>> key)
            => Enumerable.Range(0, Keys.Length)
                .Select(i => StreamEvent.CreatePoint(Times[i], create(Keys[i])))
                .ToObservable()
                .ToStreamable()
                .TumblingWindowLifetime(5)
                .GroupAggregate(key, w => w.Count(), (g, c) => new KeyCount { Key = g.Key, Count = c })
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + e.Payload.Key + "=" + e.Payload.Count)
                .ToEnumerable()
                .ToList();
    }

    [TestClass]
    public class ImmutablePayloadTestsRow : ImmutablePayloadTestsBase
    {
        public ImmutablePayloadTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class ImmutablePayloadTestsColumnar : ImmutablePayloadTestsBase
    {
        public ImmutablePayloadTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }

    // The default settings, where a failed code generation falls back to row mode.
    [TestClass]
    public class ImmutablePayloadTestsColumnarFallback : ImmutablePayloadTestsBase
    {
        public ImmutablePayloadTestsColumnarFallback()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }
}
