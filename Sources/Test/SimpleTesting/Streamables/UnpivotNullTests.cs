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
        internal UnpivotNullTestsBase(ConfigModifier modifier) : base(modifier) { }

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

        private static IStreamable<Empty, T> Input<T>(Func<int, T> make)
            => new[] { StreamEvent.CreateInterval(1, 5, make(1)) }.ToObservable().ToStreamable();

        private static List<string> Events<T>(IStreamable<Empty, T> result, Func<T, string> show)
            => result
                .ToStreamEventObservable()
                .Where(e => e.IsData)
                .Select(e => e.StartTime + ":" + show(e.Payload))
                .ToEnumerable()
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToList();
    }

    [TestClass]
    public class UnpivotNullTestsRow : UnpivotNullTestsBase
    {
        public UnpivotNullTestsRow()
            : base(new ConfigModifier().ForceRowBasedExecution(true).DontFallBackToRowBasedExecution(true)) { }
    }

    // Unpivot's SelectMany captures a delegate, which columnar code generation cannot compile, so a
    // columnar query falls back to row execution. This class runs the default settings, which allow that.
    [TestClass]
    public class UnpivotNullTestsDefault : UnpivotNullTestsBase
    {
        public UnpivotNullTestsDefault()
            : base(new ConfigModifier().ForceRowBasedExecution(false).DontFallBackToRowBasedExecution(false)) { }
    }
}
