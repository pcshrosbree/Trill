// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.ComponentModel;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// An operator represents a stream query.
    /// </summary>
    /// <typeparam name="TKey">Group key type.</typeparam>
    /// <typeparam name="TPayload">Output payload type.</typeparam>
    [ContractClass(typeof(StreamableContract<,>))]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class Streamable<TKey, TPayload> : IStreamable<TKey, TPayload>
    {
        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public StreamProperties<TKey, TPayload> Properties => this.properties;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected StreamProperties<TKey, TPayload> properties;

        internal Streamable(StreamProperties<TKey, TPayload> properties)
        {
            Contract.Requires(properties != null);

            this.properties = properties;
        }

        #region IStreamable

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        /// <param name="observer"></param>
        /// <returns></returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public abstract IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer);

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public string ErrorMessages => this.errorMessages;

        /// <summary>
        /// Currently for internal use only - do not use directly.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        protected string errorMessages;
        #endregion

        // Why CanGenerateColumnar last refused, for the row-mode report. Null if it recorded nothing.
        internal RowModeRefusal rowModeRefusal;

        // Each records a refusal and returns false, so that CanGenerateColumnar can "return Refuse...(...)".
        internal bool RefuseType(Type type)
        {
            this.rowModeRefusal = RowModeRefusal.TypeNotColumnar(type);
            return false;
        }

        internal bool RefusePartitioned()
        {
            this.rowModeRefusal = RowModeRefusal.Partitioned;
            return false;
        }

        internal bool RefuseUnsupported(string detail)
        {
            this.rowModeRefusal = RowModeRefusal.NotSupported(detail);
            return false;
        }

        // Takes the result of a generator: keeps its diagnostics, and records a refusal if it failed.
        internal bool Generated(Tuple<Type, string> generated)
        {
            this.errorMessages = generated.Item2;
            if (generated.Item1 != null) return true;
            this.rowModeRefusal = RowModeRefusal.CodeGenerationFailed(generated);
            return false;
        }

        // Called where the operator switches to row mode.
        internal void ReportRowMode(bool atSubscribe = false) => RowMode.Report(this, this.rowModeRefusal, atSubscribe);
    }

    [ContractClassFor(typeof(Streamable<,>))]
    internal abstract class StreamableContract<TKey, TPayload> : Streamable<TKey, TPayload>
    {
        public StreamableContract(StreamProperties<TKey, TPayload> properties)
            : base(properties)
        { }

        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
        {
            Contract.Requires(observer != null);
            return null; // Dummy return
        }
    }
}