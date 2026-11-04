// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Microsoft.StreamProcessing
{
    internal partial class WhereTemplate
    {
        public readonly bool noTransformation;
        public readonly string PARAMETER;
        public readonly string PREDICATE;

        private static int sequenceNumber = 0;
        private readonly string multiStringInit;
        private readonly string multiStringReturns;
        private string multiStringWrapperInit;
        private string vectorOperations;
        private string rebuiltPayload;

        private WhereTemplate(
            string className, Type keyType, Type payloadType, string predicate, bool noTransformation, string parameterName, string multiStringInit, string multiStringReturns)
            : base(className, keyType, payloadType, payloadType)
        {
            this.fields = payloadType.GetAnnotatedFields().Item1; // Do we need this special behavior?
            this.PREDICATE = predicate;
            this.noTransformation = noTransformation;
            this.PARAMETER = parameterName;
            this.multiStringInit = multiStringInit;
            this.multiStringReturns = multiStringReturns;
        }

        internal static Tuple<Type, string> Generate<TKey, TPayload>(WhereStreamable<TKey, TPayload> stream)
        {
            string generatedClassName;
            string expandedCode;
            var assemblyReferences = new List<Assembly>();
            string errorMessages = null;

            try
            {
                var keyType = typeof(TKey);
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(keyType));

                var sourceType = typeof(TPayload);
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(sourceType));
                assemblyReferences.AddRange(Transformer.AssemblyReferencesNeededFor(stream.Predicate));

                generatedClassName = string.Format("WhereUnaryPipeGeneratedFrom_{0}_{1}_{2}", keyType.GetValidIdentifier(), sourceType.GetValidIdentifier(), sequenceNumber++);

                var noTransformation = false;

                var p = stream.Predicate.Body;

                var transformedPredicate = p;
                var multiStringInit = string.Empty;
                var multiStringReturns = string.Empty;
                var multiStringWrapperInit = string.Empty;
                string vectorOperations = null;
                string rebuiltPayload = null;

                Contract.Assume(stream.Predicate.Parameters.Count() == 1);
                var parameter = stream.Predicate.Parameters.First();
                var parameterName = parameter.ToString();

                // A predicate that uses its parameter other than to read a column would rebuild the payload from the
                // columns at each such use. It runs on one value per row instead, as in row mode. That value is built
                // before the predicate runs, so a type whose constructor has effects of its own keeps the rebuild at
                // each use, where the predicate reaches it. A class predicate that writes into a column keeps it too:
                // row mode keeps that write in the stored object, as the column does, and a rebuilt local would not.
                var constructor = sourceType.GetConstructor(Type.EmptyTypes);
                var reconstructsPayload = Extensions.ReconstructsPayload<TKey, TPayload>(stream.Predicate)
                    && (constructor == null || constructor.IsTrivialParameterlessConstructor())
                    && (sourceType.GetTypeInfo().IsValueType || !ColumnWriteFinder.Find(stream.Predicate, new ColumnarRepresentation(sourceType)));

                if (!reconstructsPayload && Config.UseMultiString && Config.MultiStringTransforms != Config.CodegenOptions.MultiStringFlags.None)
                {
                    var result = MultiStringTransformer.Transform(sourceType, transformedPredicate);
                    if (result.transformedExpression != null)
                        transformedPredicate = result.transformedExpression;

                    multiStringInit = string.Join("\n", result.wrapperTable.Select(e => string.Format(
                        "var {0} = new MultiString.MultiStringWrapper({1}{2}_col);",
                        e.Value.Name, Transformer.ColumnFieldPrefix, e.Key.Name)));
                    multiStringWrapperInit = string.Join("\n", result.wrapperTable.Select(e => string.Format("{0}.rowIndex = i;", e.Value.Name)));

                    vectorOperations = result.vectorOperation;
                }

                var x = reconstructsPayload ? null : Extensions.TransformUnaryFunction<TKey, TPayload>(Expression.Lambda(transformedPredicate, parameter));
                noTransformation = x == null;
                if (noTransformation)
                {
                    // The generated code declares the payload as a local, named apart from the generated method's
                    // other locals. The name occurs nowhere in the code generated for the predicate, the rebuild or
                    // the two types, so it differs from every parameter there, nested lambdas' included, and never
                    // hides a type or namespace that code names.
                    rebuiltPayload = Extensions.TransformUnaryFunction<TKey, TPayload>(Expression.Lambda(parameter, parameter)).Body.ExpressionToCSharp();
                    var text = DecodeIdentifiers(string.Join(" ", p.ExpressionToCSharp(), rebuiltPayload, keyType, sourceType));
                    var name = "rowPayload";
                    for (int n = 1; text.Contains(name); n++) name = "rowPayload" + n;
                    var payload = Expression.Parameter(sourceType, name);
                    transformedPredicate = ParameterSubstituter.Replace(parameter, payload, p);
                    parameterName = payload.Name;
                }
                else
                {
                    transformedPredicate = x.Body;
                }

                var template = new WhereTemplate(
                    generatedClassName,
                    keyType,
                    sourceType,
                    transformedPredicate.ExpressionToCSharp(),
                    noTransformation,
                    parameterName,
                    multiStringInit,
                    multiStringReturns)
                {
                    multiStringWrapperInit = multiStringWrapperInit,
                    vectorOperations = vectorOperations,
                    rebuiltPayload = rebuiltPayload
                };

                generatedClassName = generatedClassName.AddNumberOfNecessaryGenericArguments(keyType, sourceType);
                expandedCode = template.TransformText();

                assemblyReferences.Add(Transformer.GeneratedStreamMessageAssembly<TKey, TPayload>());
                assemblyReferences.Add(typeof(IStreamable<,>).GetTypeInfo().Assembly);

                var a = Transformer.CompileSourceCode(expandedCode, assemblyReferences, out errorMessages);
                var t = a.GetType(generatedClassName);
                t = t.InstantiateAsNecessary(typeof(TKey), typeof(TPayload));
                return Tuple.Create(t, errorMessages);
            }
            catch (Exception e)
            {
                if (Config.CodegenOptions.DontFallBackToRowBasedExecution)
                {
                    throw CodegenFailure.Create("Where", errorMessages, e);
                }
                return CodegenFailure.Fallback("Where", errorMessages, e);
            }
        }

        // In an identifier, C# reads a \u or \U escape as the character it names and ignores formatting characters,
        // as the C# specification's section on identifiers says. Decoding them lets a plain search of the text find
        // every spelling of a name. A leading '@' needs nothing, since the name follows it in the text.
        private static string DecodeIdentifiers(string name)
        {
            var key = new StringBuilder();
            for (int i = 0; i < name.Length; i++)
            {
                var digits = name[i] == '\\' && i + 1 < name.Length ? (name[i + 1] == 'u' ? 4 : name[i + 1] == 'U' ? 8 : 0) : 0;
                if (digits > 0 && i + 2 + digits <= name.Length
                    && int.TryParse(name.Substring(i + 2, digits), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var code)
                    && (digits == 4 || (code >= 0 && code <= 0x10FFFF && (code < 0xD800 || code > 0xDFFF))))
                {
                    key.Append(digits == 4 ? ((char)code).ToString() : char.ConvertFromUtf32(code));
                    i += 1 + digits;
                }
                else
                {
                    key.Append(name[i]);
                }
            }

            return new string(key.ToString().Where(c => CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.Format).ToArray());
        }
    }
}
