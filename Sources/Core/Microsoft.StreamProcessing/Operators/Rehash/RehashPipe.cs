// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    /// <summary>
    /// Recomputes the key hash of each event with the stream's key equality comparer. It reads only keys and hashes,
    /// so it passes row and columnar batches alike.
    /// </summary>
    [DataContract]
    internal sealed class RehashPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly Func<TKey, int> hashFunc;
        private readonly MemoryPool<TKey, TPayload> pool;

        [Obsolete("Used only by serialization. Do not call directly.")]
        public RehashPipe() { }

        public RehashPipe(RehashStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer)
            : base(stream, observer)
        {
            this.hashFunc = stream.Properties.KeyEqualityComparer.GetGetHashCodeExpr().Compile();
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
        }

        public override void ProduceQueryPlan(PlanNode previous) => this.Observer.ProduceQueryPlan(previous);

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;
            batch.hash = batch.hash.MakeWritable(this.pool.intPool);

            fixed (long* bv = batch.bitvector.col)
            fixed (int* hash = batch.hash.col)
            {
                var key = batch.key.col;
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) != 0) continue;
                    hash[i] = this.hashFunc(key[i]);
                }
            }

            this.Observer.OnNext(batch);
        }

        public override int CurrentlyBufferedOutputCount => 0;

        public override int CurrentlyBufferedInputCount => 0;
    }
}
