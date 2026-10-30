// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Text;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Why an operator falls back to row mode.
    /// </summary>
    public enum RowModeReason
    {
        /// <summary>
        /// Generating the operator's columnar code failed. See <see cref="RowModeReport.Diagnostics"/>
        /// and <see cref="RowModeReport.Exception"/>.
        /// </summary>
        CodeGenerationFailed,

        /// <summary>
        /// A type the operator would hold in columns cannot be represented as columns.
        /// See <see cref="RowModeReport.NonColumnarType"/> and <see cref="RowModeReport.Detail"/>.
        /// </summary>
        TypeNotColumnar,

        /// <summary>
        /// The stream is partitioned, and the operator has no columnar form for partitioned streams.
        /// </summary>
        PartitionedStream,

        /// <summary>
        /// The operator has no columnar form for this query. <see cref="RowModeReport.Detail"/> says
        /// why, when the operator records a reason.
        /// </summary>
        NotSupported,
    }

    /// <summary>
    /// Describes one operator falling back to row mode. Passed to <see cref="Config.RowModeReporter"/>.
    /// </summary>
    public sealed class RowModeReport
    {
        internal RowModeReport(object @operator, RowModeRefusal refusal, bool atSubscribe)
        {
            this.Operator = @operator;
            this.Reason = refusal.Reason;
            this.NonColumnarType = refusal.NonColumnarType;
            this.Detail = refusal.Detail;
            this.Diagnostics = refusal.Diagnostics;
            this.Exception = refusal.Exception;
            this.AtSubscribe = atSubscribe;
        }

        /// <summary>
        /// The operator that falls back: an <see cref="IStreamable{TKey, TPayload}"/>, or, for an
        /// egress site, the observable the egress method returned.
        /// </summary>
        public object Operator { get; }

        /// <summary>
        /// Why the operator falls back.
        /// </summary>
        public RowModeReason Reason { get; }

        /// <summary>
        /// For <see cref="RowModeReason.TypeNotColumnar"/>, the type that failed the check;
        /// otherwise null. It may be the operator's input, output or another type it holds.
        /// </summary>
        public Type NonColumnarType { get; }

        /// <summary>
        /// A plain-language reason, or null if the operator recorded none. For
        /// <see cref="RowModeReason.TypeNotColumnar"/> it names the rule the type broke.
        /// </summary>
        public string Detail { get; }

        /// <summary>
        /// The diagnostics from code generation, including the compiler's when compilation ran, or null if there
        /// are none.
        /// </summary>
        public string Diagnostics { get; }

        /// <summary>
        /// The exception that stopped code generation, when one was thrown; otherwise null.
        /// The same instance can appear in reports from several builds of the same query.
        /// </summary>
        public Exception Exception { get; }

        /// <summary>
        /// True when the operator decided while the query was being subscribed to, false when it decided while the
        /// query was being built. Map-reduce builds its query again at Subscribe, so its reports from then are true.
        /// So is the report of an ingress whose code generation is still deferred when egress or Cache() follows it
        /// directly. An ingress that refuses its payload type, and a partitioned ingress, decide while they are built.
        /// </summary>
        /// <remarks>
        /// Subscription is tracked through the egress methods and Cache(). A query subscribed another
        /// way, such as by calling <see cref="IStreamable{TKey, TPayload}.Subscribe"/> directly, which is for
        /// internal use, can report false for a decision made during that call.
        /// </remarks>
        public bool AtSubscribe { get; }

        /// <summary>
        /// Describes the report in one line.
        /// </summary>
        /// <returns>A description of the report.</returns>
        public override string ToString()
        {
            var text = new StringBuilder("Row mode: ").Append(this.Operator).Append(" (").Append(this.Reason).Append(')');
            if (this.Detail != null) text.Append(": ").Append(this.Detail);
            return text.ToString();
        }
    }

    internal static class RowMode
    {
        [ThreadStatic]
        private static int subscribing;

        // A decision can be made during Subscribe by an operator that usually decides while the query is built: an
        // ingress whose deferred code generation egress or Cache() asks for first, or an operator built by
        // map-reduce's second build. Its report must still say it came from Subscribe. So every egress Subscribe
        // runs inside this, and so does Cache(); map-reduce's Subscribe is reached only through one of them.
        internal static T WhileSubscribing<T>(Func<T> subscribe)
        {
            subscribing++;
            try
            {
                return subscribe();
            }
            finally
            {
                subscribing--;
            }
        }

        // Calls the hook, if one is set. Never throws: the query must still be built.
        internal static void Report(object @operator, RowModeRefusal refusal, bool atSubscribe)
        {
            if (Config.ForceRowBasedExecution) return;
            var reporter = Config.RowModeReporter;
            if (reporter == null) return;
            try
            {
                reporter(new RowModeReport(@operator, refusal ?? RowModeRefusal.Unrecorded, atSubscribe || subscribing > 0));
            }
            catch (Exception)
            {
                Config.CountRowModeReporterFailure();
            }
        }
    }

    // Why an operator refused columnar code, recorded where the refusal is made and reported where the
    // operator switches to row mode.
    internal sealed class RowModeRefusal
    {
        internal static readonly RowModeRefusal Unrecorded = new RowModeRefusal(RowModeReason.NotSupported, null, null, null, null);

        internal RowModeRefusal(RowModeReason reason, Type nonColumnarType, string detail, string diagnostics, Exception exception)
        {
            this.Reason = reason;
            this.NonColumnarType = nonColumnarType;
            this.Detail = detail;
            this.Diagnostics = diagnostics;
            this.Exception = exception;
        }

        internal RowModeReason Reason { get; }

        internal Type NonColumnarType { get; }

        internal string Detail { get; }

        internal string Diagnostics { get; }

        internal Exception Exception { get; }

        internal static RowModeRefusal TypeNotColumnar(Type type)
            => new RowModeRefusal(RowModeReason.TypeNotColumnar, type, type.ColumnarRefusal(), null, null);

        internal static RowModeRefusal NotSupported(string detail)
            => new RowModeRefusal(RowModeReason.NotSupported, null, detail, null, null);

        internal static readonly RowModeRefusal Partitioned
            = new RowModeRefusal(RowModeReason.PartitionedStream, null, "partitioned streams run in row mode", null, null);

        // A failed generation: Item1 is null, Item2 holds any compiler diagnostics.
        internal static RowModeRefusal CodeGenerationFailed(Tuple<Type, string> generated)
        {
            var exception = CodegenFailure.ExceptionOf(generated);
            var detail = exception != null
                ? "code generation threw " + exception.GetType().Name + ": " + exception.Message
                : "code generation failed";
            return new RowModeRefusal(RowModeReason.CodeGenerationFailed, null, detail, string.IsNullOrEmpty(generated.Item2) ? null : generated.Item2, exception);
        }
    }
}
