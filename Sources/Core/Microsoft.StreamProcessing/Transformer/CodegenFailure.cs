// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.CompilerServices;
using System.Text;

namespace Microsoft.StreamProcessing
{
    internal static class CodegenFailure
    {
        private const string DiagnosticsKey = "Microsoft.StreamProcessing.CodegenDiagnostics";

        // With fallback to row mode disabled, a failed code generation throws this exception. It names the
        // operator, and keeps the compiler's diagnostics and the exception that stopped code generation.
        internal static InvalidOperationException Create(string operatorName, string errorMessages, Exception inner)
        {
            // A transformer that calls another generator, as the join transformers do, catches that generator's
            // exception before it has any diagnostics of its own. Take them from the inner exception instead.
            if (string.IsNullOrEmpty(errorMessages) && inner?.Data[DiagnosticsKey] is string innerDiagnostics)
                errorMessages = innerDiagnostics;

            var message = new StringBuilder("Code Generation failed when it wasn't supposed to! Operator: ")
                .Append(operatorName)
                .Append('.');
            if (!string.IsNullOrEmpty(errorMessages))
                message.Append(" Compiler diagnostics:\n").Append(errorMessages);
            var exception = new InvalidOperationException(message.ToString(), inner);
            if (!string.IsNullOrEmpty(errorMessages))
                exception.Data[DiagnosticsKey] = errorMessages;
            return exception;
        }

        // The generators' results are cached, failures included, so a second build of the same query
        // gets the same tuple back without running the generator. Keep the operator name and the exception
        // beside the tuple, so that a row-mode report from any build can carry the exception, and a build
        // with fallback disabled can throw what the generator would have thrown.
        private static readonly ConditionalWeakTable<Tuple<Type, string>, Failure> Failures
            = new ConditionalWeakTable<Tuple<Type, string>, Failure>();

        // With fallback to row mode allowed, a failed code generation returns this result.
        internal static Tuple<Type, string> Fallback(string operatorName, string errorMessages, Exception exception)
        {
            var result = Tuple.Create((Type)null, errorMessages);
            Failures.Add(result, new Failure(operatorName, exception));
            return result;
        }

        internal static Exception ExceptionOf(Tuple<Type, string> generated)
            => generated != null && Failures.TryGetValue(generated, out var failure) ? failure.Exception : null;

        // The generator checks the setting only when it runs, but a failure cached while fallback was allowed
        // reaches later builds without running it. With fallback disabled, such a failure throws here, as the
        // generator would have. A generator that declined without an exception made no Fallback result, and
        // still falls back.
        internal static void ThrowIfFallbackIsDisabled(Tuple<Type, string> generated)
        {
            if (Config.CodegenOptions.DontFallBackToRowBasedExecution
                && generated != null
                && Failures.TryGetValue(generated, out var failure))
            {
                throw Create(failure.OperatorName, generated.Item2, failure.Exception);
            }
        }

        private sealed class Failure
        {
            internal Failure(string operatorName, Exception exception)
            {
                this.OperatorName = operatorName;
                this.Exception = exception;
            }

            internal string OperatorName { get; }

            internal Exception Exception { get; }
        }
    }
}
