// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public sealed class UnpivotNullIntFields
    {
        public string Key;
        public int Low;
        public int High;
    }

    public sealed class UnpivotNullIntProperties
    {
        public string Key { get; set; }
        public int Low { get; set; }
        public int High { get; set; }
    }

    public sealed class UnpivotNullNullableFields
    {
        public string Key;
        public int? Low;
        public int? High;
    }

    public sealed class UnpivotNullNullableProperties
    {
        public string Key { get; set; }
        public int? Low { get; set; }
        public int? High { get; set; }
    }

    public sealed class UnpivotNullSingleInt
    {
        public string Key;
        public int Only;
    }

    public class UnpivotNullHiddenPropertyBase
    {
        public string Hidden { get; set; }
    }

    // Hides the base class's Hidden property with one of another type. Neither fits an int value field.
    public sealed class UnpivotNullHiddenProperty : UnpivotNullHiddenPropertyBase
    {
        public string Key;
        public int Only;

        public new long Hidden { get; set; }
    }

    public class UnpivotNullHiddenFieldBase
    {
        public string Hidden;
    }

    // Hides the base class's Hidden field with one of another type. Neither fits an int value field.
    public sealed class UnpivotNullHiddenField : UnpivotNullHiddenFieldBase
    {
        public string Key;
        public int Only;
        public new long Hidden;
    }

    public sealed class UnpivotNullStrings
    {
        public string Key;
        public string First;
        public string Second;
    }

    // Its == operator calls an empty box equal to null, so only a reference comparison sees that it is there.
    public sealed class UnpivotNullBox : IComparable
    {
        public int? Content;

        public static bool operator ==(UnpivotNullBox left, object right) => (left is null || left.Content == null) && right is null;

        public static bool operator !=(UnpivotNullBox left, object right) => !(left == right);

        public int CompareTo(object obj) => 0;

        public override bool Equals(object obj) => ReferenceEquals(this, obj);

        public override int GetHashCode() => 0;

        public override string ToString() => this.Content == null ? "empty" : this.Content.ToString();
    }

    public sealed class UnpivotNullBoxes
    {
        public string Key;
        public UnpivotNullBox EmptyField;
        public UnpivotNullBox NullField;

        public UnpivotNullBox EmptyProperty { get; set; }

        public UnpivotNullBox NullProperty { get; set; }
    }

    public sealed class UnpivotNullBoxPair
    {
        public string Key;
        public UnpivotNullBox First;

        public UnpivotNullBox Second { get; set; }
    }

    public sealed class UnpivotNullBoxRow
    {
        public string Key;
        public string Attribute;
        public UnpivotNullBox Value;
    }

    public sealed class UnpivotNullObjectRow
    {
        public string Key;
        public string Attribute;
        public object Value;
    }

    public sealed class UnpivotNullIntRow
    {
        public string Key;
        public string Attribute;
        public int Value;
    }

    public sealed class UnpivotNullComparableRow
    {
        public string Key;
        public string Attribute;
        public IComparable Value;
    }

    public abstract class UnpivotNullTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        // When set, an AlterEventLifetime that keeps each lifetime sits between ingress and Unpivot, so the
        // SelectMany that Unpivot builds is not fused into the ingress operator and gets its own pipe.
        private readonly bool afterIngress;

        // When set, every query must run columnar. A code generation failure cached by an earlier query that was
        // allowed to fall back is reused without an exception, even with fallback disabled; this check catches
        // such a query, which would otherwise run in row mode and pass.
        private readonly bool requireColumnar;

        internal UnpivotNullTestsBase(ConfigModifier modifier, bool afterIngress = false, bool requireColumnar = false)
            : base(modifier)
        {
            this.afterIngress = afterIngress;
            this.requireColumnar = requireColumnar;
        }

        // Unpivot runs in columnar mode unless the configuration forces row mode.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotRunsColumnarUnlessForcedToRows()
        {
            var result = Input(v => new UnpivotNullNullableFields { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            Assert.AreEqual(!Config.ForceRowBasedExecution, result.Properties.IsColumnar);
        }

        // Two queries with the same types that differ only in their key selector each keep their own key, so
        // generated code made for one is not reused for the other.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotQueriesThatDifferOnlyInTheirKeyKeepTheirOwnKeys()
        {
            var plain = Input(v => new UnpivotNullIntFields { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);
            var computed = Input(v => new UnpivotNullIntFields { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key + "!", r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=10", "1:a:Low=1" }, Events(plain, r => r.Key + ":" + r.Attribute + "=" + r.Value));
            CollectionAssert.AreEqual(new[] { "1:a!:High=10", "1:a!:Low=1" }, Events(computed, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        // A single member that cannot be null gives exactly one output event for each input event.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotSingleIntField()
        {
            var result = Input(v => new UnpivotNullSingleInt { Key = "a", Only = v })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:Only=1" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        // An input with no member that fits the value field gives no output.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotNoFittingMember()
        {
            var result = Input(v => new UnpivotNullStrings { Key = "a", First = "x", Second = "y" })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(Array.Empty<string>(), Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        // A non-nullable value-type member is never null, so each one becomes an output event.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotIntFields()
        {
            var result = Input(v => new UnpivotNullIntFields { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=10", "1:a:Low=1" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnpivotIntProperties()
        {
            var result = Input(v => new UnpivotNullIntProperties { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=10", "1:a:Low=1" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnpivotNullableFieldsWithValues()
        {
            var result = Input(v => new UnpivotNullNullableFields { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=10", "1:a:Low=1" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnpivotNullablePropertiesWithValues()
        {
            var result = Input(v => new UnpivotNullNullableProperties { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=10", "1:a:Low=1" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        // A null member is skipped; the other member of the same event is still emitted.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotNullablePropertiesSkipNulls()
        {
            var result = Input(v => new UnpivotNullNullableProperties { Key = "a", Low = null, High = v * 10 })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=10" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnpivotNullablePropertiesAllNull()
        {
            var result = Input(v => new UnpivotNullNullableProperties { Key = "a", Low = null, High = null })
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(Array.Empty<string>(), Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        // With a value field of an interface type, a reference-type member can be null, and a null one is skipped.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotInterfaceValueSkipsNullReferences()
        {
            var result = Input(v => new UnpivotNullStrings { Key = "a", First = "x" + v, Second = null })
                .Unpivot(() => new UnpivotNullComparableRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:First=x1" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + (r.Value == null ? "null" : r.Value.ToString())));
        }

        // A reference-type member is null only when the reference is null, whatever its == operator says.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotInterfaceValueComparesReferences()
        {
            var result = Input(v => new UnpivotNullBoxes { Key = "a", EmptyField = new UnpivotNullBox(), EmptyProperty = new UnpivotNullBox() })
                .Unpivot(() => new UnpivotNullComparableRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:EmptyField=empty", "1:a:EmptyProperty=empty" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        // With a value field of a class type, a member is null only when the reference is null, whatever its
        // == operator says: an empty box, which its == (UnpivotNullBox, object) calls null, is still emitted.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotClassValueComparesReferences()
        {
            var result = Input(v => new UnpivotNullBoxPair { Key = "a", First = new UnpivotNullBox(), Second = new UnpivotNullBox() })
                .Unpivot(() => new UnpivotNullBoxRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:First=empty", "1:a:Second=empty" }, Events(result, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        // With a value field of a class type, a null field or a null property is skipped.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotClassValueSkipsNullReferences()
        {
            var nullField = Input(v => new UnpivotNullBoxPair { Key = "a", First = null, Second = new UnpivotNullBox { Content = v } })
                .Unpivot(() => new UnpivotNullBoxRow(), p => p.Key, r => r.Attribute, r => r.Value);
            var nullProperty = Input(v => new UnpivotNullBoxPair { Key = "a", First = new UnpivotNullBox { Content = v }, Second = null })
                .Unpivot(() => new UnpivotNullBoxRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:Second=1" }, Events(nullField, r => r.Key + ":" + r.Attribute + "=" + r.Value));
            CollectionAssert.AreEqual(new[] { "1:a:First=1" }, Events(nullProperty, r => r.Key + ":" + r.Attribute + "=" + r.Value));
        }

        // Value-type members are boxed into an object value field.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotIntFieldsToObjectValue()
        {
            var result = Input(v => new UnpivotNullIntFields { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullObjectRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=Int32 10", "1:a:Low=Int32 1" }, Events(result, ShowObject));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnpivotIntPropertiesToObjectValue()
        {
            var result = Input(v => new UnpivotNullIntProperties { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullObjectRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=Int32 10", "1:a:Low=Int32 1" }, Events(result, ShowObject));
        }

        // A nullable member's value is boxed into an object value field, and a null one is skipped.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotNullableFieldsToObjectValue()
        {
            var result = Input(v => new UnpivotNullNullableFields { Key = "a", Low = null, High = v * 10 })
                .Unpivot(() => new UnpivotNullObjectRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=Int32 10" }, Events(result, ShowObject));
        }

        [TestMethod, TestCategory("Gated")]
        public void UnpivotNullablePropertiesToObjectValue()
        {
            var result = Input(v => new UnpivotNullNullableProperties { Key = "a", Low = null, High = v * 10 })
                .Unpivot(() => new UnpivotNullObjectRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=Int32 10" }, Events(result, ShowObject));
        }

        // A reference-type member goes into an object value field as it is, and a null one is skipped.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotStringFieldsToObjectValue()
        {
            var result = Input(v => new UnpivotNullStrings { Key = "a", First = "x" + v, Second = null })
                .Unpivot(() => new UnpivotNullObjectRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:First=String x1" }, Events(result, ShowObject));
        }

        // Value-type members are boxed into a value field of an interface type as well.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotIntFieldsToInterfaceValue()
        {
            var result = Input(v => new UnpivotNullIntFields { Key = "a", Low = v, High = v * 10 })
                .Unpivot(() => new UnpivotNullComparableRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "1:a:High=Int32 10", "1:a:Low=Int32 1" }, Events(result, r => ShowValue(r.Key, r.Attribute, r.Value)));
        }

        private static string ShowObject(UnpivotNullObjectRow r) => ShowValue(r.Key, r.Attribute, r.Value);

        // Names the value's runtime type, so a value stored without boxing fails here rather than passing as a number.
        private static string ShowValue(string key, string attribute, object value)
            => key + ":" + attribute + "=" + (value == null ? "null" : value.GetType().Name + " " + value);

        private List<string> Events<T>(IStreamable<Empty, T> result, Func<T, string> show)
        {
            if (this.requireColumnar) Assert.IsTrue(result.Properties.IsColumnar, "The query runs in row mode.");
            return result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + show(e.Payload))
                .ToEnumerable()
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToList();
        }

        private IStreamable<Empty, T> Input<T>(Func<int, T> make)
        {
            var input = new[] { StreamEvent.CreateInterval(1, 5, make(1)) }.ToObservable().ToStreamable();
            return this.afterIngress ? input.AlterEventLifetime(t => t, 4) : input;
        }
    }

    [TestClass]
    public class UnpivotNullTestsRow : UnpivotNullTestsBase
    {
        public UnpivotNullTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    [TestClass]
    public class UnpivotNullTestsDefault : UnpivotNullTestsBase
    {
        public UnpivotNullTestsDefault()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }

    // Columnar, with the SelectMany fused into the ingress operator. A code generation failure throws, and every
    // query must run columnar.
    [TestClass]
    public class UnpivotNullTestsColumnar : UnpivotNullTestsBase
    {
        public UnpivotNullTestsColumnar()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true), requireColumnar: true) { }
    }

    // Columnar, with the SelectMany in its own pipe after ingress. A code generation failure throws, and every
    // query must run columnar.
    [TestClass]
    public class UnpivotNullTestsColumnarAfterIngress : UnpivotNullTestsBase
    {
        public UnpivotNullTestsColumnarAfterIngress()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(true), afterIngress: true, requireColumnar: true) { }
    }

    // Input types that hide a base member with new. These run in row mode only: columnar ingress throws
    // ArgumentException for these two input types, because their column layout would hold two members named Hidden.
    [TestClass]
    public class UnpivotHiddenMemberTests : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public UnpivotHiddenMemberTests()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }

        // A property hidden with new, whose types do not fit the value field, is ignored rather than looked up by name.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotIgnoresHiddenPropertiesThatDoNotFit()
        {
            var result = new[] { StreamEvent.CreateInterval(1, 5, new UnpivotNullHiddenProperty { Key = "a", Only = 1, Hidden = 2 }) }
                .ToObservable().ToStreamable()
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "a:Only=1" }, Rows(result));
        }

        // A field hidden with new, whose types do not fit the value field, is ignored rather than looked up by name.
        [TestMethod, TestCategory("Gated")]
        public void UnpivotIgnoresHiddenFieldsThatDoNotFit()
        {
            var result = new[] { StreamEvent.CreateInterval(1, 5, new UnpivotNullHiddenField { Key = "a", Only = 1, Hidden = 2 }) }
                .ToObservable().ToStreamable()
                .Unpivot(() => new UnpivotNullIntRow(), p => p.Key, r => r.Attribute, r => r.Value);

            CollectionAssert.AreEqual(new[] { "a:Only=1" }, Rows(result));
        }

        private static List<string> Rows(IStreamable<Empty, UnpivotNullIntRow> result)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.Payload.Key + ":" + e.Payload.Attribute + "=" + e.Payload.Value)
                .ToEnumerable()
                .ToList();
    }
}
