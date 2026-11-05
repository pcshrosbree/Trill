// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Exception specific to an error occurring during data ingress.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2237:MarkISerializableTypesWithSerializable", Justification = "SerializableAttribute not yet supported by .Net Core")]
    public sealed class IngressException : Exception
    {
        /// <summary>
        /// Create an Ingress exception with no underlying message.
        /// </summary>
        public IngressException() { }

        /// <summary>
        /// Create an Ingress exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        public IngressException(string message) : base(message) { }

        /// <summary>
        /// Create an Ingress exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        /// <param name="innerException">The additional exception being bundled with this exception.</param>
        public IngressException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Represents a user exception somewhere within the
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2237:MarkISerializableTypesWithSerializable", Justification = "SerializableAttribute not yet supported by .Net Core")]
    public sealed class StreamProcessingException : Exception
    {
        /// <summary>
        /// Create a Stream Processing exception with no underlying message.
        /// </summary>
        public StreamProcessingException() : base() { }

        /// <summary>
        /// Create a Stream Processing exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        public StreamProcessingException(string message) : base(message) { }

        /// <summary>
        /// Create a Stream Processing exception with the given message and given inner exception.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        /// <param name="innerException">The additional exception being bundled with this exception.</param>
        public StreamProcessingException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Thrown by the binary <c>Query</c> of a sharded streamable with more than one shard when its two inputs were
    /// redistributed with different key equality comparers.
    /// </summary>
    /// <remarks>
    /// <c>ReDistribute</c> (and <c>Shuffle</c>, which calls it) sends each event to a shard chosen by its key's hash,
    /// and the binary <c>Query</c> pairs shard <c>i</c> of one input with shard <c>i</c> of the other. If the two
    /// inputs have different comparers, equal keys can be on different shards, and the query could miss
    /// matches. To avoid this, give both inputs the same key equality comparer before they are redistributed, or
    /// give neither one.
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2237:MarkISerializableTypesWithSerializable", Justification = "SerializableAttribute not yet supported by .Net Core")]
    public sealed class ShardedKeyComparerMismatchException : Exception
    {
        /// <summary>
        /// Create a sharded key comparer mismatch exception with no underlying message.
        /// </summary>
        public ShardedKeyComparerMismatchException() : base() { }

        /// <summary>
        /// Create a sharded key comparer mismatch exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        public ShardedKeyComparerMismatchException(string message) : base(message) { }

        /// <summary>
        /// Create a sharded key comparer mismatch exception with the given message and given inner exception.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        /// <param name="innerException">The additional exception being bundled with this exception.</param>
        public ShardedKeyComparerMismatchException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Exception when Trill detects an out of order event outside of ingress
    /// </summary>
    public sealed class StreamProcessingOutOfOrderException : Exception
    {
        /// <summary>
        /// Create ann out-of-order exception with no underlying message.
        /// </summary>
        public StreamProcessingOutOfOrderException() : base() { }

        /// <summary>
        /// Create an out-of-order exception with the given message.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        public StreamProcessingOutOfOrderException(string message) : base(message) { }

        /// <summary>
        /// Create an out-of-order exception with the given message and given inner exception.
        /// </summary>
        /// <param name="message">The error message associated with the exception.</param>
        /// <param name="innerException">The additional exception being bundled with this exception.</param>
        public StreamProcessingOutOfOrderException(string message, Exception innerException) : base(message, innerException) { }
    }
}
