// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
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
    }
}
