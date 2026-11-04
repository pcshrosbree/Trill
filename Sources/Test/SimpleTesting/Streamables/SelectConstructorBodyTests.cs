using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reflection;
using System.Reflection.Emit;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    public struct CtorBodySource
    {
        public int A;
    }

    public sealed class CtorPlain
    {
        public int A;
        public int B;
    }

    public sealed class CtorEmpty
    {
        public CtorEmpty() { }

        public int A;
        public int B;
    }

    public sealed class CtorSets
    {
        public CtorSets() => this.A = 5;

        public int A;
        public int B;
    }

    public sealed class CtorFieldInitializer
    {
        public int A = 42;
        public int B;
    }

    public class CtorBaseSets
    {
        public CtorBaseSets() => this.A = 7;

        public int A;
    }

    public sealed class CtorDerivedFromSetter : CtorBaseSets
    {
        public int B;
    }

    public class CtorBasePlain
    {
        public int A;
    }

    public sealed class CtorDerivedFromPlain : CtorBasePlain
    {
        public int B;
    }

    public struct CtorStructPlain
    {
        public int A;
        public int B;
    }

    public struct CtorStructSets
    {
        public CtorStructSets()
        {
            this.A = 9;
            this.B = 0;
        }

        public int A;
        public int B;
    }

    public sealed record CtorRecordPlain
    {
        public int A { get; set; }

        public int B { get; set; }
    }

    public sealed record CtorRecordInitializer
    {
        public int A { get; set; } = 3;

        public int B { get; set; }
    }

    public abstract class SelectConstructorBodyTestsBase : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private readonly bool columnar;

        internal SelectConstructorBodyTestsBase(bool columnar)
            : base(new ConfigModifier().ForceRowBasedExecution(!columnar).DontFallBackToRowBasedExecution(false))
            => this.columnar = columnar;

        [TestMethod, TestCategory("Gated")]
        public void PlainClass() => Check(Source().Select(s => new CtorPlain { B = s.A }), r => $"({r.A}, {r.B})", 0, true);

        [TestMethod, TestCategory("Gated")]
        public void EmptyConstructor() => Check(Source().Select(s => new CtorEmpty { B = s.A }), r => $"({r.A}, {r.B})", 0, true);

        [TestMethod, TestCategory("Gated")]
        public void ConstructorSetsAField() => Check(Source().Select(s => new CtorSets { B = s.A }), r => $"({r.A}, {r.B})", 5, false);

        [TestMethod, TestCategory("Gated")]
        public void FieldInitializer()
            => Check(Source().Select(s => new CtorFieldInitializer { B = s.A }), r => $"({r.A}, {r.B})", 42, false);

        [TestMethod, TestCategory("Gated")]
        public void BaseConstructorSetsAField()
            => Check(Source().Select(s => new CtorDerivedFromSetter { B = s.A }), r => $"({r.A}, {r.B})", 7, false);

        [TestMethod, TestCategory("Gated")]
        public void PlainBaseClass()
            => Check(Source().Select(s => new CtorDerivedFromPlain { B = s.A }), r => $"({r.A}, {r.B})", 0, true);

        [TestMethod, TestCategory("Gated")]
        public void PlainStruct() => Check(Source().Select(s => new CtorStructPlain { B = s.A }), r => $"({r.A}, {r.B})", 0, true);

        [TestMethod, TestCategory("Gated")]
        public void StructConstructorSetsAField()
            => Check(Source().Select(s => new CtorStructSets { B = s.A }), r => $"({r.A}, {r.B})", 9, false);

        [TestMethod, TestCategory("Gated")]
        public void PlainRecord() => Check(Source().Select(s => new CtorRecordPlain { B = s.A }), r => $"({r.A}, {r.B})", 0, true);

        [TestMethod, TestCategory("Gated")]
        public void RecordPropertyInitializer()
            => Check(Source().Select(s => new CtorRecordInitializer { B = s.A }), r => $"({r.A}, {r.B})", 3, false);

        // Controls from fork #84: a Select fused into the ingress evaluates the whole new expression.
        [TestMethod, TestCategory("Gated")]
        public void FusedConstructorSetsAField()
            => Check(Fused().Select(s => new CtorSets { B = s.A }), r => $"({r.A}, {r.B})", 5, true);

        [TestMethod, TestCategory("Gated")]
        public void FusedFieldInitializer()
            => Check(Fused().Select(s => new CtorFieldInitializer { B = s.A }), r => $"({r.A}, {r.B})", 42, true);

        [TestMethod, TestCategory("Gated")]
        public void ConstructorWithoutAnInitializer()
            => Check(Source().Select(s => new CtorSets()), r => $"({r.A}, {r.B})", "(5, 0) (5, 0) (5, 0)", true);

        [TestMethod, TestCategory("Gated")]
        public void InitializerSetsEveryField()
            => Check(Source().Select(s => new CtorSets { A = 5, B = s.A }), r => $"({r.A}, {r.B})", 5, false);

        private static IStreamable<Empty, CtorBodySource> Fused()
            => Enumerable.Range(1, 3).Select(i => StreamEvent.CreatePoint(i, new CtorBodySource { A = i })).ToObservable().ToStreamable();

        // The Union keeps the Select out of the ingress.
        private static IStreamable<Empty, CtorBodySource> Source()
            => Fused().Union(Array.Empty<StreamEvent<CtorBodySource>>().ToObservable().ToStreamable());

        private void Check<T>(IStreamable<Empty, T> query, Func<T, string> read, int a, bool staysColumnar)
            => Check(query, read, $"({a}, 1) ({a}, 2) ({a}, 3)", staysColumnar);

        private void Check<T>(IStreamable<Empty, T> query, Func<T, string> read, string expected, bool staysColumnar)
        {
            var output = query.ToStreamEventObservable().ToEnumerable().Where(e => e.IsData).Select(e => read(e.Payload));
            Assert.AreEqual(expected, string.Join(" ", output));
            Assert.AreEqual(this.columnar && staysColumnar, query.Properties.IsColumnar);
        }
    }

    // The test assemblies are built without optimization, so their constructors contain nops. These types are compiled
    // with optimization to check the shape without nops too.
    [TestClass]
    public class TrivialParameterlessConstructorTests
    {
        [TestMethod, TestCategory("Gated")]
        public void OptimizedCode()
        {
            const string source = @"
                public class Plain { public int A; }
                public class Empty { public Empty() { } public int A; }
                public class Sets { public Sets() { A = 5; } public int A; }
                public class Initializer { public int A = 42; }
                public class FromSets : Sets { public int B; }
                public class FromPlain : Plain { public int B; }";
            var references = AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
                .Select(a => MetadataReference.CreateFromFile(a.Location));
            var compilation = CSharpCompilation.Create(
                "TrivialConstructors",
                new[] { CSharpSyntaxTree.ParseText(source) },
                references,
                new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, optimizationLevel: OptimizationLevel.Release));
            using (var stream = new MemoryStream())
            {
                Assert.IsTrue(compilation.Emit(stream).Success);
                var assembly = Assembly.Load(stream.ToArray());
                var result = string.Join(" ", new[] { "Plain", "Empty", "Sets", "Initializer", "FromSets", "FromPlain" }.Select(name =>
                {
                    var constructor = assembly.GetType(name).GetConstructor(Type.EmptyTypes);
                    var il = constructor.GetMethodBody().GetILAsByteArray();
                    return name + ":" + il.Length + ":" + constructor.IsTrivialParameterlessConstructor();
                }));

                // Seven bytes are ldarg.0, call with its token, and ret, with no nop.
                Assert.AreEqual("Plain:7:True Empty:7:True Sets:14:False Initializer:15:False FromSets:7:False FromPlain:7:True", result);
            }
        }

        // No C# compiler emits these bodies, so they are written directly.
        [TestMethod, TestCategory("Gated")]
        public void HandWrittenBodies()
        {
            var builder = new PersistedAssemblyBuilder(new AssemblyName("HandWrittenConstructors"), typeof(object).Assembly);
            var module = builder.DefineDynamicModule("HandWrittenConstructors");
            var objectConstructor = typeof(object).GetConstructor(Type.EmptyTypes);
            var baseConstructor = Define(module, "Base", typeof(object), il => Call(il, objectConstructor));
            var baseType = (Type)baseConstructor.DeclaringType;
            var otherConstructor = Define(module, "Other", typeof(object), il => Call(il, objectConstructor));
            Define(module, "LeadingNop", baseType, il =>
            {
                il.Emit(OpCodes.Nop);
                Call(il, baseConstructor);
            });
            Define(module, "CallsAnotherType", baseType, il => Call(il, otherConstructor));
            using (var stream = new MemoryStream())
            {
                builder.Save(stream);
                var assembly = Assembly.Load(stream.ToArray());
                var result = string.Join(" ", new[] { "Base", "LeadingNop", "CallsAnotherType" }
                    .Select(name => name + ":" + assembly.GetType(name).GetConstructor(Type.EmptyTypes).IsTrivialParameterlessConstructor()));
                Assert.AreEqual("Base:True LeadingNop:True CallsAnotherType:False", result);
            }
        }

        private static ConstructorInfo Define(ModuleBuilder module, string name, Type parent, Action<ILGenerator> body)
        {
            var type = module.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, parent);
            var constructor = type.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            body(constructor.GetILGenerator());
            type.CreateType();
            return constructor;
        }

        private static void Call(ILGenerator il, ConstructorInfo constructor)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Call, constructor);
            il.Emit(OpCodes.Ret);
        }
    }

    [TestClass]
    public class SelectConstructorBodyTestsRow : SelectConstructorBodyTestsBase
    {
        public SelectConstructorBodyTestsRow() : base(false) { }
    }

    [TestClass]
    public class SelectConstructorBodyTestsColumnar : SelectConstructorBodyTestsBase
    {
        public SelectConstructorBodyTestsColumnar() : base(true) { }
    }
}
