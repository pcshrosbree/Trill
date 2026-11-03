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
    public class HiddenFieldBase { public string Hidden; }

    // A field hides a base field of another type: GetFields returns both.
    public sealed class HiddenFieldPayload : HiddenFieldBase
    {
        public string Key;
        public new long Hidden;
    }

    // A field hides a base field of the same type: GetFields still returns both.
    public sealed class HiddenSameTypeFieldPayload : HiddenFieldBase
    {
        public string Key;
        public new string Hidden;
    }

    public class HiddenPropertyBase { public string Hidden { get; set; } }

    // An auto-property hides a base auto-property of another type: GetProperties returns both.
    public sealed class HiddenPropertyPayload : HiddenPropertyBase
    {
        public string Key;
        public new long Hidden { get; set; }
    }

    // A field hides a base auto-property: GetFields returns the field, GetProperties the base property.
    public sealed class FieldHidingPropertyPayload : HiddenPropertyBase
    {
        public string Key;
        public new long Hidden;
    }

    // An auto-property hides a base field: GetProperties returns the property, GetFields the base field.
    public sealed class PropertyHidingFieldPayload : HiddenFieldBase
    {
        public string Key;
        public new long Hidden { get; set; }
    }

    public class InheritedFieldBase { public long Inherited; }

    // Inherits a public field without hiding it: one member of each name, so it can be held in columns.
    public sealed class InheritedFieldPayload : InheritedFieldBase { public string Key; }

    // A payload class that uses new to hide an inherited member has two members of one name, which one columnar
    // layout cannot hold. Trill refuses columnar mode for it, and its queries run in row mode in every mode.
    public abstract class HiddenMemberPayloadTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private ConcurrentQueue<RowModeReport> reports;

        internal HiddenMemberPayloadTestsBase(ConfigModifier modifier) : base(modifier) { }

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
        public void IngressOfAHiddenFieldRuns()
        {
            var payload = new HiddenFieldPayload { Key = "a", Hidden = 2 };
            ((HiddenFieldBase)payload).Hidden = "base";

            var events = Ingress(payload).ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToList();

            Assert.AreEqual(1, events.Count);
            Assert.AreEqual("a", events[0].Payload.Key);
            Assert.AreEqual(2L, events[0].Payload.Hidden);
            Assert.AreEqual("base", ((HiddenFieldBase)events[0].Payload).Hidden, "the hidden base field keeps its value");
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressOfAHiddenSameTypeFieldRuns()
        {
            var payload = new HiddenSameTypeFieldPayload { Key = "a", Hidden = "derived" };
            ((HiddenFieldBase)payload).Hidden = "base";

            var events = Ingress(payload).ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToList();

            Assert.AreEqual(1, events.Count);
            Assert.AreEqual("derived", events[0].Payload.Hidden);
            Assert.AreEqual("base", ((HiddenFieldBase)events[0].Payload).Hidden);
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressOfAHiddenPropertyRuns()
        {
            var payload = new HiddenPropertyPayload { Key = "a", Hidden = 2 };
            ((HiddenPropertyBase)payload).Hidden = "base";

            var events = Ingress(payload).ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToList();

            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(2L, events[0].Payload.Hidden);
            Assert.AreEqual("base", ((HiddenPropertyBase)events[0].Payload).Hidden);
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressOfAFieldHidingAPropertyRuns()
        {
            var payload = new FieldHidingPropertyPayload { Key = "a", Hidden = 2 };
            ((HiddenPropertyBase)payload).Hidden = "base";

            var events = Ingress(payload).ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToList();

            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(2L, events[0].Payload.Hidden);
            Assert.AreEqual("base", ((HiddenPropertyBase)events[0].Payload).Hidden);
        }

        [TestMethod, TestCategory("Gated")]
        public void IngressOfAPropertyHidingAFieldRuns()
        {
            var payload = new PropertyHidingFieldPayload { Key = "a", Hidden = 2 };
            ((HiddenFieldBase)payload).Hidden = "base";

            var events = Ingress(payload).ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToList();

            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(2L, events[0].Payload.Hidden);
            Assert.AreEqual("base", ((HiddenFieldBase)events[0].Payload).Hidden);
        }

        // A Select whose result type hides a member: the Select refuses columnar mode for its result type.
        [TestMethod, TestCategory("Gated")]
        public void SelectIntoAHiddenFieldRuns()
        {
            var input = new[] { 1, 2 }.Select((v, i) => StreamEvent.CreatePoint(i + 1, new InheritedFieldPayload { Key = "k" + v, Inherited = v }))
                .ToObservable().ToStreamable().AlterEventDuration(1);
            var result = input.Select(e => new HiddenFieldPayload { Key = e.Key, Hidden = e.Inherited * 10 });

            var rows = result.ToStreamEventObservable().Where(e => e.IsData).Select(e => e.Payload.Key + "=" + e.Payload.Hidden).ToEnumerable().ToList();
            CollectionAssert.AreEqual(new[] { "k1=10", "k2=20" }, rows);
            Assert.IsFalse(result.Properties.IsColumnar);
        }

        // Control: inheriting a public field without hiding it is still one member of each name.
        [TestMethod, TestCategory("Gated")]
        public void IngressOfAnInheritedFieldRuns()
        {
            var query = Ingress(new InheritedFieldPayload { Key = "a", Inherited = 7 });
            var events = query.ToStreamEventObservable().Where(e => e.IsData).ToEnumerable().ToList();

            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(7L, events[0].Payload.Inherited);
            Assert.AreEqual(!Config.ForceRowBasedExecution, query.Properties.IsColumnar);
            Assert.AreEqual(0, this.reports.Count);
        }

        internal static IStreamable<Empty, T> Ingress<T>(T payload)
            => new[] { StreamEvent.CreateInterval(1, 5, payload) }.ToObservable().ToStreamable();

        internal RowModeReport[] Reports() => this.reports.ToArray();
    }

    [TestClass]
    public class HiddenMemberPayloadTestsRow : HiddenMemberPayloadTestsBase
    {
        public HiddenMemberPayloadTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // Columnar, with fallback disabled. Refusing a type is not a code generation failure, so these queries run in
    // row mode rather than throw.
    [TestClass]
    public class HiddenMemberPayloadTestsColumnar : HiddenMemberPayloadTestsBase
    {
        public HiddenMemberPayloadTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true)) { }

        [DataTestMethod, TestCategory("Gated")]
        [DataRow(typeof(HiddenFieldPayload))]
        [DataRow(typeof(HiddenSameTypeFieldPayload))]
        [DataRow(typeof(HiddenPropertyPayload))]
        [DataRow(typeof(FieldHidingPropertyPayload))]
        [DataRow(typeof(PropertyHidingFieldPayload))]
        public void RefusalNamesTheMember(Type type)
        {
            Assert.AreEqual("more than one member is named Hidden", type.ColumnarRefusal());
            Assert.IsFalse(type.CanRepresentAsColumnar());
        }

        [TestMethod, TestCategory("Gated")]
        public void InheritedFieldHasNoRefusal() => Assert.IsNull(typeof(InheritedFieldPayload).ColumnarRefusal());

        // The ingress reports the payload type, as for any type that cannot be held in columns.
        [TestMethod, TestCategory("Gated")]
        public void IngressReportsTheHiddenMember()
        {
            var query = Ingress(new HiddenFieldPayload { Key = "a", Hidden = 2 });

            Assert.IsFalse(query.Properties.IsColumnar);
            var all = Reports();
            Assert.AreEqual(1, all.Length, string.Join("; ", all.Select(r => r.ToString())));
            Assert.AreEqual(RowModeReason.TypeNotColumnar, all[0].Reason);
            Assert.AreEqual(typeof(HiddenFieldPayload), all[0].NonColumnarType);
            Assert.AreEqual("more than one member is named Hidden", all[0].Detail);
        }
    }
}
