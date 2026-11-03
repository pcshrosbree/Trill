// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // Builds a result in a method call, which Ungroup's code generation cannot split into column assignments.
    public static class FirstLevelUngroupResults
    {
        public static KeyCount Make(int key, int value) => new KeyCount { Key = key * 1000, Count = (ulong)value };
    }

    // The Ungroup class for a stream with no outer grouping key: its input is keyed by the grouping key itself, not
    // by a CompoundGroupKey. No public operator builds it (map-reduce's classes without an outer key do), so these
    // tests construct it directly, after a GroupStreamable with an Empty outer key.
    public abstract class FirstLevelUngroupTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        internal FirstLevelUngroupTestsBase(ConfigModifier modifier) : base(modifier) { }

        // The selector reads the key into a field: the computed-field branch, which must use the key itself.
        [TestMethod, TestCategory("Gated")]
        public void SelectorReadsTheKey()
        {
            var result = Ungroup<KeyCount>((k, p) => new KeyCount { Key = k * 10, Count = (ulong)p.Key });

            CollectionAssert.AreEquivalent(new[] { "1:10=1", "2:20=2", "3:10=1" }, Rows(result, r => r.Key + "=" + r.Count));
            AssertColumnar(result);
        }

        // The selector reads only the payload, so the key expression plays no part. Only the input batch's assembly
        // reference matters here.
        [TestMethod, TestCategory("Gated")]
        public void SelectorReadsOnlyThePayload()
        {
            var result = Ungroup<KeyCount>((k, p) => new KeyCount { Key = p.Key * 100 });

            CollectionAssert.AreEquivalent(new[] { "1:100", "2:200", "3:100" }, Rows(result, r => r.Key.ToString(CultureInfo.InvariantCulture)));
            AssertColumnar(result);
        }

        // An int result has no fields: the selector becomes the one computed column.
        [TestMethod, TestCategory("Gated")]
        public void SelectorWithAnIntResultReadsTheKey()
        {
            var result = Ungroup<int>((k, p) => (k * 100) + p.Key);

            CollectionAssert.AreEquivalent(new[] { "1:101", "2:202", "3:101" }, Rows(result, r => r.ToString(CultureInfo.InvariantCulture)));
            AssertColumnar(result);
        }

        // A method call builds the whole result instance: the other branch that substitutes the key.
        [TestMethod, TestCategory("Gated")]
        public void SelectorBuildsTheResultInAMethod()
        {
            var result = Ungroup<KeyCount>((k, p) => FirstLevelUngroupResults.Make(k, p.Key));

            CollectionAssert.AreEquivalent(new[] { "1:1000=1", "2:2000=2", "3:1000=1" }, Rows(result, r => r.Key + "=" + r.Count));
            AssertColumnar(result);
        }

        private static IStreamable<Empty, TResult> Ungroup<TResult>(Expression<Func<int, CapturedInput, TResult>> resultSelector)
        {
            var input = new[] { 1, 2, 1 }
                .Select((k, i) => StreamEvent.CreatePoint(i + 1, new CapturedInput { Key = k }))
                .ToObservable()
                .ToStreamable();
            var grouped = new GroupStreamable<Empty, CapturedInput, int>(input, e => e.Key);
            return new UngroupStreamable<int, CapturedInput, TResult>(grouped, resultSelector);
        }

        // Each data event as "time:value".
        private static List<string> Rows<TResult>(IStreamable<Empty, TResult> result, Func<TResult, string> show)
            => result.ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime.ToString(CultureInfo.InvariantCulture) + ":" + show(e.Payload))
                .ToEnumerable()
                .ToList();

        // With row mode not forced, the Ungroup's input is columnar, and the Ungroup must stay columnar too.
        private static void AssertColumnar<TResult>(IStreamable<Empty, TResult> result)
            => Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);
    }

    [TestClass]
    public class FirstLevelUngroupTestsRow : FirstLevelUngroupTestsBase
    {
        public FirstLevelUngroupTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // Columnar, with fallback disabled: code generation that fails throws, so each query must compile.
    [TestClass]
    public class FirstLevelUngroupTestsColumnar : FirstLevelUngroupTestsBase
    {
        public FirstLevelUngroupTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }
    }
}
