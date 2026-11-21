// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Diagnostics.Contracts;

namespace Microsoft.StreamProcessing
{
    internal static class RehashExtensions
    {
        /// <summary>
        /// Returns the stream with the given key equality comparer. A join finds candidate matches by the key hash
        /// stored in each batch, so if the stream has another comparer, its key hashes are recomputed with this one.
        /// The stored hashes come from the stream's comparer as it runs, so the rehash is skipped only when the two
        /// comparers are the same trees as built (<see cref="ComparerDifference.SameAsBuilt"/>).
        /// </summary>
        internal static IStreamable<TKey, TPayload> WithKeyEqualityComparer<TKey, TPayload>(
            this IStreamable<TKey, TPayload> source, IEqualityComparerExpression<TKey> comparer)
            => ComparerDifference.SameAsBuilt(source.Properties.KeyEqualityComparer, comparer)
                ? source
                : new RehashStreamable<TKey, TPayload>(source, comparer);
    }

    internal sealed class RehashStreamable<TKey, TPayload> : Streamable<TKey, TPayload>
    {
        private readonly IStreamable<TKey, TPayload> source;

        public RehashStreamable(IStreamable<TKey, TPayload> source, IEqualityComparerExpression<TKey> comparer)
            : base(source.Properties.ToKeyEqualityComparer(comparer))
        {
            Contract.Requires(source != null);
            this.source = source;
        }

        public override IDisposable Subscribe(IStreamObserver<TKey, TPayload> observer)
            => this.source.Subscribe(new RehashPipe<TKey, TPayload>(this, observer));
    }
}
