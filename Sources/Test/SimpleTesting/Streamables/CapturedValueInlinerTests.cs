using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    // Marked as compiler-generated, but its name is an ordinary C# name, so it is not a closure class.
    [CompilerGenerated]
    public sealed class CompilerGeneratedHolder
    {
        public int Value;
    }

    public sealed class RefConstructed
    {
        public RefConstructed(ref int x) => x++;
    }

    public delegate int RefFunc(ref int x);

    // Unit tests of the rewriter itself. CapturedValueTests checks the operators that call it.
    [TestClass]
    public class CapturedValueInlinerTests
    {
        private int instanceField = 7;

        [TestMethod, TestCategory("Gated")]
        public void CapturedLocalBecomesConstant()
        {
            int x = 3;
            Expression<Func<int, int>> f = e => e + x;

            var body = (BinaryExpression)f.InlineCapturedValues().Body;
            var constant = body.Right as ConstantExpression;
            Assert.IsNotNull(constant);
            Assert.AreEqual(typeof(int), constant.Type);
            Assert.AreEqual(3, constant.Value);
        }

        [TestMethod, TestCategory("Gated")]
        public void NothingToReplaceReturnsTheSameInstance()
        {
            Expression<Func<int, int>> f = e => e + 1;
            Assert.AreSame(f, f.InlineCapturedValues());
            Assert.IsNull(((Expression<Func<int, int>>)null).InlineCapturedValues());
        }

        // A field of this is reached through the closure's reference to this, which is not a closure.
        [TestMethod, TestCategory("Gated")]
        public void FieldOfThisIsLeftAlone()
        {
            int x = 1;
            Expression<Func<int, int>> f = e => e + this.instanceField + x;

            var body = (BinaryExpression)f.InlineCapturedValues().Body;
            Assert.IsInstanceOfType(body.Right, typeof(ConstantExpression));
            var inner = (BinaryExpression)body.Left;
            Assert.IsInstanceOfType(inner.Right, typeof(MemberExpression));
        }

        // A struct local is not a literal type; its field is read through the closure and stays a field read.
        [TestMethod, TestCategory("Gated")]
        public void FieldOfCapturedStructIsLeftAlone()
        {
            var pair = new CapturedInput { Key = 4 };
            Expression<Func<int, int>> f = e => e + pair.Key;

            Assert.AreSame(f, f.InlineCapturedValues());
        }

        [TestMethod, TestCategory("Gated")]
        public void CompilerGeneratedClassWithOrdinaryNameIsNotAClosure()
        {
            var holder = new CompilerGeneratedHolder { Value = 5 };
            var e = Expression.Parameter(typeof(int), "e");
            var f = Expression.Lambda<Func<int, int>>(
                Expression.Add(e, Expression.Field(Expression.Constant(holder), nameof(CompilerGeneratedHolder.Value))), e);

            Assert.AreSame(f, f.InlineCapturedValues());
        }

        // A type named like a closure class but without the compiler-generated attribute is not a closure.
        [TestMethod, TestCategory("Gated")]
        public void ClosureNameWithoutAttributeIsNotAClosure()
        {
            var assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("InlinerNameProbe"), AssemblyBuilderAccess.Run);
            var type = assembly.DefineDynamicModule("InlinerNameProbe").DefineType("<>c__DisplayClass0_0", TypeAttributes.Public | TypeAttributes.Sealed);
            type.DefineField("x", typeof(int), FieldAttributes.Public);
            var built = type.CreateTypeInfo().AsType();
            var instance = Activator.CreateInstance(built);
            built.GetField("x").SetValue(instance, 6);

            var e = Expression.Parameter(typeof(int), "e");
            var f = Expression.Lambda<Func<int, int>>(Expression.Add(e, Expression.Field(Expression.Constant(instance), "x")), e);

            Assert.AreSame(f, f.InlineCapturedValues());
        }

        // A captured variable that the expression writes keeps its field reads, whichever way it is written.
        [TestMethod, TestCategory("Gated")]
        public void OutArgumentIsNotReplaced()
        {
            int parsed = 0;
            Expression<Func<string, int>> f = s => int.TryParse(s, out parsed) ? parsed : -1;

            Assert.AreSame(f, f.InlineCapturedValues());
        }

        [TestMethod, TestCategory("Gated")]
        public void AssignedFieldIsNotReplaced()
            => AssertWriteKeepsField((field, e) => Expression.Assign(field, e));

        [TestMethod, TestCategory("Gated")]
        public void CheckedCompoundAssignedFieldIsNotReplaced()
            => AssertWriteKeepsField((field, e) => Expression.AddAssignChecked(field, e));

        [TestMethod, TestCategory("Gated")]
        public void IncrementedFieldIsNotReplaced()
            => AssertWriteKeepsField((field, e) => Expression.PreIncrementAssign(field));

        [TestMethod, TestCategory("Gated")]
        public void FieldPassedByRefToAConstructorIsNotReplaced()
            => AssertWriteKeepsField((field, e) => Expression.Block(
                Expression.New(typeof(RefConstructed).GetConstructors()[0], field), field));

        [TestMethod, TestCategory("Gated")]
        public void FieldPassedByRefToADelegateIsNotReplaced()
        {
            RefFunc increment = (ref int x) => ++x;
            AssertWriteKeepsField((field, e) => Expression.Invoke(Expression.Constant(increment), field));
        }

        // Expression.Invoke also takes an Expression<TDelegate> as its target. C# cannot write a lambda with a ref
        // parameter as an expression tree, so this one is built with the Expression API.
        [TestMethod, TestCategory("Gated")]
        public void FieldPassedByRefToAnExpressionTreeIsNotReplaced()
        {
            var x = Expression.Parameter(typeof(int).MakeByRefType(), "x");
            var increment = Expression.Lambda<RefFunc>(Expression.PreIncrementAssign(x), x);
            AssertWriteKeepsField((field, e) => Expression.Invoke(Expression.Constant(increment), field));
        }

        // Builds a lambda around a write to a captured int field, which is also read, and checks that the
        // rewriter leaves it alone.
        private static void AssertWriteKeepsField(Func<MemberExpression, ParameterExpression, Expression> write)
        {
            int y = 0;
            Expression<Func<int, int>> source = e => e + y;
            var field = (MemberExpression)((BinaryExpression)source.Body).Right;
            var p = source.Parameters[0];
            var lambda = Expression.Lambda<Func<int, int>>(Expression.Add(write(field, p), field), p);

            Assert.AreSame(lambda, lambda.InlineCapturedValues());
        }
    }
}
