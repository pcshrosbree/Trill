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
        // gets the same tuple back without running the generator. Keep the exception beside the tuple
        // so that a row-mode report from any build can carry it.
        private static readonly ConditionalWeakTable<Tuple<Type, string>, Exception> Exceptions
            = new ConditionalWeakTable<Tuple<Type, string>, Exception>();

        // With fallback to row mode allowed, a failed code generation returns this result.
        internal static Tuple<Type, string> Fallback(string errorMessages, Exception exception)
        {
            var result = Tuple.Create((Type)null, errorMessages);
            Exceptions.Add(result, exception);
            return result;
        }

        internal static Exception ExceptionOf(Tuple<Type, string> generated)
            => generated != null && Exceptions.TryGetValue(generated, out var exception) ? exception : null;
    }
}
