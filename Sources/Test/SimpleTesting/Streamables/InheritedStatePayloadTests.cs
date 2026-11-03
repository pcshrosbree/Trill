// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // A base class that keeps state in a private field. Reflection on a derived class does not return that field.
    public class PrivateStateBase
    {
        private int secret;

        public int Secret => this.secret;

        public void SetSecret(int value) => this.secret = value;
    }

    public sealed class PrivateBaseFieldPayload : PrivateStateBase { public string Key; }

    // The same private field, two classes up.
    public class PrivateStateMiddle : PrivateStateBase { public long Middle; }

    public sealed class PrivateGrandparentFieldPayload : PrivateStateMiddle { public string Key; }

    public class SameTypePropertyBase { public string Hidden { get; set; } }

    // An auto-property hides a base auto-property of the same type: GetProperties returns only the derived one.
    public sealed class SameTypeAutoPropertyPayload : SameTypePropertyBase
    {
        public string Key;

        public new string Hidden { get; set; }
    }

    // A hand-written property hides the base auto-property, and keeps its value in a public field.
    public sealed class SameTypeHandWrittenPropertyPayload : SameTypePropertyBase
    {
        public string Key;
        public string Shadow;

        public new string Hidden
        {
            get => this.Shadow;
            set => this.Shadow = value;
        }
    }

    public class PrivateSetterStateBase
    {
        public int Count { get; private set; }

        public void SetCount(int value) => this.Count = value;
    }

    public sealed class PrivateSetterBasePayload : PrivateSetterStateBase { public string Key; }

    // A public field that reuses the name of the base class's private field: the column of that name holds only the
    // derived field.
    public sealed class PrivateFieldNameReusedPayload : PrivateStateBase { public int secret; }

    // No public field or auto-property at all: the base class's private field is still state that no column holds.
    public sealed class WholePrivateBasePayload : PrivateStateBase { }

    // Controls: a base class with only public state, a generic one, and a base class with no state.
    public class PublicStateBase
    {
        public long Field;

        public string Property { get; set; }
    }

    public sealed class PublicBasePayload : PublicStateBase { public string Key; }

    public class PublicGenericStateBase<T>
    {
        public T Value;

        public T Other { get; set; }
    }

    public sealed class PublicGenericBasePayload : PublicGenericStateBase<int> { public string Key; }

    public class StatelessBase
    {
        public int Two => 2;
    }

    public sealed class StatelessBasePayload : StatelessBase { public string Key; }

    // Columnar mode holds a payload in one column for each public field and auto-property. State that a base class
    // keeps anywhere else would be lost, so Trill refuses columnar mode for such a type, and its queries run in row
    // mode in every mode.
    public abstract class InheritedStatePayloadTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private ConcurrentQueue<RowModeReport> reports;

        internal InheritedStatePayloadTestsBase(ConfigModifier modifier) : base(modifier) { }

        [TestInitialize]
        public void StartCollecting()
        {
            var queue = new ConcurrentQueue<RowModeReport>();
            this.reports = queue;
            Config.RowModeReporter = queue.Enqueue;
        }

        [TestCleanup]
        public void StopCollecting() => Config.RowModeReporter = null;

        [TestMethod, TestCategory("Gated")]
        public void IngressKeepsAPrivateBaseField()
        {
            var payload = new PrivateBaseFieldPayload { Key = "a" };
            payload.SetSecret(42);

            var events = RoundTrip(Ingress(payload));

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual("a", events[0].Key);
            Assert.AreEqual(42, events[0].Secret, "the base class's private field keeps its value");
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressKeepsAPrivateGrandparentField()
        {
            var payload = new PrivateGrandparentFieldPayload { Key = "a", Middle = 5 };
            payload.SetSecret(42);

            var events = RoundTrip(Ingress(payload));

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual(5L, events[0].Middle);
            Assert.AreEqual(42, events[0].Secret);
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressKeepsABaseAutoPropertyHiddenByAnAutoProperty()
        {
            var payload = new SameTypeAutoPropertyPayload { Key = "a", Hidden = "derived" };
            ((SameTypePropertyBase)payload).Hidden = "base";

            var events = RoundTrip(Ingress(payload));

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual("derived", events[0].Hidden);
            Assert.AreEqual("base", ((SameTypePropertyBase)events[0]).Hidden, "the hidden base auto-property keeps its value");
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressKeepsABaseAutoPropertyHiddenByAHandWrittenProperty()
        {
            var payload = new SameTypeHandWrittenPropertyPayload { Key = "a", Hidden = "derived" };
            ((SameTypePropertyBase)payload).Hidden = "base";

            var events = RoundTrip(Ingress(payload));

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual("derived", events[0].Hidden);
            Assert.AreEqual("base", ((SameTypePropertyBase)events[0]).Hidden);
        }

        // The state must also survive an operator after the ingress that is not fused into it.
        [TestMethod, TestCategory("Gated")]
        public void AnOperatorKeepsAPrivateBaseField()
        {
            var first = new PrivateBaseFieldPayload { Key = "a" };
            first.SetSecret(1);
            var second = new PrivateBaseFieldPayload { Key = "b" };
            second.SetSecret(2);
            var input = new[] { first, second }.Select((p, i) => StreamEvent.CreatePoint(i + 1, p)).ToObservable().ToStreamable();

            var rows = RoundTrip(input.ShiftEventLifetime(10)).Select(p => p.Key + "=" + p.Secret).ToList();

            CollectionAssert.AreEqual(new[] { "a=1", "b=2" }, rows);
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressKeepsABaseAutoPropertyWithAPrivateSetter()
        {
            var payload = new PrivateSetterBasePayload { Key = "a" };
            payload.SetCount(7);

            var events = RoundTrip(Ingress(payload));

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual(7, events[0].Count);
        }

        // Control: public fields and auto-properties of a base class have columns of their own.
        [TestMethod, TestCategory("Gated")]
        public void IngressOfAPublicBaseStateStaysColumnar()
        {
            var query = Ingress(new PublicBasePayload { Key = "a", Field = 3, Property = "p" });

            var events = RoundTrip(query);

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual(3L, events[0].Field);
            Assert.AreEqual("p", events[0].Property);
            Assert.AreEqual(!Config.ForceRowBasedExecution, query.Properties.IsColumnar);
            Assert.AreEqual(0, this.reports.Count, string.Join("; ", this.reports.Select(r => r.ToString())));
        }

        // Control: a base class with no fields keeps no state.
        [TestMethod, TestCategory("Gated")]
        public void IngressOfAStatelessBaseStaysColumnar()
        {
            var query = Ingress(new StatelessBasePayload { Key = "a" });

            var events = RoundTrip(query);

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual("a", events[0].Key);
            Assert.AreEqual(!Config.ForceRowBasedExecution, query.Properties.IsColumnar);
            Assert.AreEqual(0, this.reports.Count, string.Join("; ", this.reports.Select(r => r.ToString())));
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressKeepsAPrivateBaseFieldWhoseNameIsReused()
        {
            var payload = new PrivateFieldNameReusedPayload { secret = 5 };
            payload.SetSecret(42);

            var events = RoundTrip(Ingress(payload));

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual(5, events[0].secret);
            Assert.AreEqual(42, events[0].Secret);
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressKeepsAPrivateBaseFieldOfAPayloadWithNoColumns()
        {
            var payload = new WholePrivateBasePayload();
            payload.SetSecret(42);

            var events = RoundTrip(Ingress(payload));

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual(42, events[0].Secret);
        }

        // Control: the public members of a generic base class have columns of their own.
        [TestMethod, TestCategory("Gated")]
        public void IngressOfAPublicGenericBaseStateStaysColumnar()
        {
            var query = Ingress(new PublicGenericBasePayload { Key = "a", Value = 3, Other = 4 });

            var events = RoundTrip(query);

            Assert.AreEqual(1, events.Length);
            Assert.AreEqual(3, events[0].Value);
            Assert.AreEqual(4, events[0].Other);
            Assert.AreEqual(!Config.ForceRowBasedExecution, query.Properties.IsColumnar);
            Assert.AreEqual(0, this.reports.Count, string.Join("; ", this.reports.Select(r => r.ToString())));
        }

        internal static IStreamable<Empty, T> Ingress<T>(T payload)
            => new[] { StreamEvent.CreateInterval(1, 5, payload) }.ToObservable().ToStreamable();

        internal static T[] RoundTrip<T>(IStreamable<Empty, T> query)
            => query.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload).ToEnumerable().ToArray();

        internal RowModeReport[] Reports() => this.reports.ToArray();
    }

    [TestClass]
    public class InheritedStatePayloadTestsRow : InheritedStatePayloadTestsBase
    {
        public InheritedStatePayloadTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // Columnar, with fallback disabled. Refusing a type is not a code generation failure, so these queries run in
    // row mode rather than throw.
    [TestClass]
    public class InheritedStatePayloadTestsColumnar : InheritedStatePayloadTestsBase
    {
        public InheritedStatePayloadTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }

        [DataTestMethod, TestCategory("Gated")]
        [DataRow(typeof(PrivateBaseFieldPayload), "field secret of base class PrivateStateBase has no column")]
        [DataRow(typeof(PrivateGrandparentFieldPayload), "field secret of base class PrivateStateBase has no column")]
        [DataRow(typeof(SameTypeAutoPropertyPayload), "property Hidden of base class SameTypePropertyBase has no column")]
        [DataRow(typeof(SameTypeHandWrittenPropertyPayload), "property Hidden of base class SameTypePropertyBase has no column")]
        [DataRow(typeof(PrivateFieldNameReusedPayload), "field secret of base class PrivateStateBase has no column")]
        [DataRow(typeof(WholePrivateBasePayload), "field secret of base class PrivateStateBase has no column")]
        [DataRow(typeof(PrivateSetterBasePayload), "property Count has no setter")]
        public void RefusalNamesTheState(Type type, string refusal)
        {
            Assert.AreEqual(refusal, type.ColumnarRefusal());
            Assert.IsFalse(type.CanRepresentAsColumnar());
        }

        [DataTestMethod, TestCategory("Gated")]
        [DataRow(typeof(PublicBasePayload))]
        [DataRow(typeof(PublicGenericBasePayload))]
        [DataRow(typeof(StatelessBasePayload))]
        public void ControlsHaveNoRefusal(Type type) => Assert.IsNull(type.ColumnarRefusal());

        // The ingress reports the payload type, as for any type that cannot be held in columns.
        [TestMethod, TestCategory("Gated")]
        public void IngressReportsTheBaseClassState()
        {
            var query = Ingress(new SameTypeAutoPropertyPayload { Key = "a", Hidden = "derived" });

            Assert.IsFalse(query.Properties.IsColumnar);
            var all = Reports();
            Assert.AreEqual(1, all.Length, string.Join("; ", all.Select(r => r.ToString())));
            Assert.AreEqual(RowModeReason.TypeNotColumnar, all[0].Reason);
            Assert.AreEqual(typeof(SameTypeAutoPropertyPayload), all[0].NonColumnarType);
            Assert.AreEqual("property Hidden of base class SameTypePropertyBase has no column", all[0].Detail);
        }
    }
}
