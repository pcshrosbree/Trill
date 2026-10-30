using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public enum LiteralKind
    {
        A = 1,
        B = 2,
    }

    public enum WideKind : long
    {
        Low = long.MinValue,
    }

    // Generated code writes each constant in an expression as C# source. These tests compile what
    // ExpressionToCSharp writes for a value of every type that has a C# literal form, and check that
    // the compiled value has the same type and the same bits.
    [TestClass]
    public class LiteralRenderingTests
    {
        private static readonly Case[] Values =
        {
            new Case(true, typeof(bool)),
            new Case(false, typeof(bool)),
            new Case('a', typeof(char)),
            new Case('\'', typeof(char)),
            new Case('\\', typeof(char)),
            new Case('\n', typeof(char)),
            new Case('\0', typeof(char)),
            new Case('\u2028', typeof(char)),
            new Case("a\"b\\c\n", typeof(string)),
            new Case("\u2028\u0085\uD800\t", typeof(string)),
            new Case('\uD800', typeof(char)),
            new Case(string.Empty, typeof(string)),
            new Case(null, typeof(string)),
            new Case(sbyte.MinValue, typeof(sbyte)),
            new Case(sbyte.MaxValue, typeof(sbyte)),
            new Case(byte.MaxValue, typeof(byte)),
            new Case(short.MinValue, typeof(short)),
            new Case((short)5, typeof(short)),
            new Case(ushort.MaxValue, typeof(ushort)),
            new Case(int.MinValue, typeof(int)),
            new Case(-1, typeof(int)),
            new Case(int.MaxValue, typeof(int)),
            new Case(5u, typeof(uint)),
            new Case(uint.MaxValue, typeof(uint)),
            new Case(long.MinValue, typeof(long)),
            new Case(-1L, typeof(long)),
            new Case(long.MaxValue, typeof(long)),
            new Case(5ul, typeof(ulong)),
            new Case(ulong.MaxValue, typeof(ulong)),
            new Case(1.1f, typeof(float)),
            new Case(-0f, typeof(float)),
            new Case(-2.5f, typeof(float)),
            new Case(float.Epsilon, typeof(float)),
            new Case(float.MaxValue, typeof(float)),
            new Case(float.NaN, typeof(float)),
            new Case(BitConverter.Int32BitsToSingle(0x7FC00000), typeof(float)),
            new Case(BitConverter.Int32BitsToSingle(0x7FC00001), typeof(float)),
            new Case(float.PositiveInfinity, typeof(float)),
            new Case(float.NegativeInfinity, typeof(float)),
            new Case(0.1, typeof(double)),
            new Case(5.0, typeof(double)),
            new Case(-0.0, typeof(double)),
            new Case(1e20, typeof(double)),
            new Case(double.Epsilon, typeof(double)),
            new Case(double.MinValue, typeof(double)),
            new Case(double.NaN, typeof(double)),
            new Case(BitConverter.Int64BitsToDouble(0x7FF8000000000001), typeof(double)),
            new Case(double.PositiveInfinity, typeof(double)),
            new Case(double.NegativeInfinity, typeof(double)),
            new Case(1.10m, typeof(decimal)),
            new Case(-0.5m, typeof(decimal)),
            new Case(0m, typeof(decimal)),
            new Case(new decimal(0, 0, 0, true, 0), typeof(decimal)),
            new Case(new decimal(0, 0, 0, true, 2), typeof(decimal)),
            new Case(new decimal(0, 0, 0, true, 28), typeof(decimal)),
            new Case(decimal.MaxValue, typeof(decimal)),
            new Case(decimal.MinValue, typeof(decimal)),
            new Case(LiteralKind.B, typeof(LiteralKind)),
            new Case((LiteralKind)0, typeof(LiteralKind)),
            new Case((LiteralKind)7, typeof(LiteralKind)),
            new Case(WideKind.Low, typeof(WideKind)),

            // A negated negative constant.
            Case.Of(Expression.Negate(Expression.Constant(-5)), 5),
            Case.Of(Expression.Negate(Expression.Constant(-5L)), 5L),
            Case.Of(Expression.Negate(Expression.Constant(-2.5f)), 2.5f),
            Case.Of(Expression.Negate(Expression.Constant(-2.5)), 2.5),
            Case.Of(Expression.Negate(Expression.Constant(-0.5m)), 0.5m),

            // A constant whose static type is wider than its value's keeps the value's type.
            Case.Of(Expression.Constant((short)5, typeof(object)), (short)5),
            Case.Of(Expression.Constant(1.5f, typeof(float?)), 1.5f),
        };

        [TestMethod, TestCategory("Gated")]
        public void EveryLiteralTypeRoundTrips() => AssertRoundTrips(CultureInfo.InvariantCulture);

        // Cultures such as de-DE write a decimal comma. The culture is built from the invariant one, so the
        // test does not depend on the platform's culture data and also runs in invariant globalization mode.
        [TestMethod, TestCategory("Gated")]
        public void EveryLiteralTypeRoundTripsWithDecimalComma()
            => AssertRoundTrips(CultureWith(nf =>
            {
                nf.NumberDecimalSeparator = ",";
                nf.NumberGroupSeparator = ".";
                nf.PositiveInfinitySymbol = "\u221E";
                nf.NegativeInfinitySymbol = "-\u221E";
            }));

        // Cultures such as sv-SE write U+2212 as the minus sign.
        [TestMethod, TestCategory("Gated")]
        public void EveryLiteralTypeRoundTripsWithUnicodeMinus()
            => AssertRoundTrips(CultureWith(nf =>
            {
                nf.NegativeSign = "\u2212";
                nf.NegativeInfinitySymbol = "\u2212\u221E";
            }));

        private static CultureInfo CultureWith(Action<NumberFormatInfo> change)
        {
            var culture = (CultureInfo)CultureInfo.InvariantCulture.Clone();
            change(culture.NumberFormat);
            return culture;
        }

        private static void AssertRoundTrips(CultureInfo culture)
        {
            var saved = CultureInfo.CurrentCulture;
            List<string> literals;
            try
            {
                CultureInfo.CurrentCulture = culture;
                literals = Values.Select(v => v.Expression.ExpressionToCSharp()).ToList();
            }
            finally
            {
                CultureInfo.CurrentCulture = saved;
            }

            // Compile all the literals at once; if that fails, compile each alone so the message names every bad one.
            var references = AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
                .Select(a => MetadataReference.CreateFromFile(a.Location))
                .ToList();
            var compiled = Compile(literals, references, out var errors);
            var wrong = new List<string>();
            for (int i = 0; i < Values.Length; i++)
            {
                object value;
                if (compiled != null) value = compiled[i];
                else
                {
                    var single = Compile(new[] { literals[i] }, references, out errors);
                    if (single == null)
                    {
                        wrong.Add(string.Format(CultureInfo.InvariantCulture, "{0} {1} was written as {2}, which does not compile: {3}", Values[i].Type.Name, Values[i].Value, literals[i], errors));
                        continue;
                    }
                    value = single[0];
                }

                if (!SameValue(Values[i].Value, value))
                {
                    wrong.Add(string.Format(CultureInfo.InvariantCulture, "{0} {1} was written as {2} and compiled to {3} {4}", Values[i].Type.Name, Values[i].Value, literals[i], value?.GetType().Name, value));
                }
            }

            Assert.AreEqual(0, wrong.Count, Environment.NewLine + string.Join(Environment.NewLine, wrong));
        }

        // Returns the compiled values, or null with the compiler's errors.
        private static object[] Compile(IEnumerable<string> literals, IEnumerable<MetadataReference> references, out string errors)
        {
            var source = "public static class LiteralProbe { public static object[] Values() => new object[] { "
                + string.Join(", ", literals.Select(l => "(object)(" + l + ")"))
                + " }; }";
            var compilation = CSharpCompilation.Create(
                "LiteralProbe" + Guid.NewGuid().ToString("N"),
                new[] { CSharpSyntaxTree.ParseText(source) },
                references,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
            using (var stream = new MemoryStream())
            {
                var result = compilation.Emit(stream);
                errors = string.Join("; ", result.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).Select(d => d.Id));
                if (!result.Success) return null;
                var probe = Assembly.Load(stream.ToArray()).GetType("LiteralProbe");
                return (object[])probe.GetMethod("Values").Invoke(null, null);
            }
        }

        private static bool SameValue(object expected, object actual)
        {
            if (expected == null) return actual == null;
            if (actual == null || expected.GetType() != actual.GetType()) return false;
            switch (expected)
            {
                case float f: return BitConverter.SingleToInt32Bits(f) == BitConverter.SingleToInt32Bits((float)actual);
                case double d: return BitConverter.DoubleToInt64Bits(d) == BitConverter.DoubleToInt64Bits((double)actual);
                case decimal m: return decimal.GetBits(m).SequenceEqual(decimal.GetBits((decimal)actual));
                default: return expected.Equals(actual);
            }
        }

        private sealed class Case
        {
            public readonly Expression Expression;
            public readonly object Value;
            public readonly Type Type;

            // A constant of the given type.
            public Case(object value, Type type) : this(Expression.Constant(value, type), value) { }

            private Case(Expression expression, object value)
            {
                this.Expression = expression;
                this.Value = value;
                this.Type = expression.Type;
            }

            // An expression and the value it must compile to.
            public static Case Of(Expression expression, object value) => new Case(expression, value);
        }
    }
}
