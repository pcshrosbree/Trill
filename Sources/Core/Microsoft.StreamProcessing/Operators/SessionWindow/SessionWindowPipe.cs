// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Microsoft.StreamProcessing.Internal;
using Microsoft.StreamProcessing.Internal.Collections;

namespace Microsoft.StreamProcessing
{
    [DataContract]
    internal sealed class SessionWindowPipe<TKey, TPayload> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;

        [SchemaSerialization]
        private readonly long sessionTimeout;
        [SchemaSerialization]
        private readonly long maximumDuration;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        private LinkedList<TKey> orderedKeys = new LinkedList<TKey>();
        [DataMember]
        private FastDictionary2<TKey, long> windowEndTimeDictionary = new FastDictionary2<TKey, long>();
        [DataMember]
        private FastDictionary2<TKey, long> lastDataTimeDictionary = new FastDictionary2<TKey, long>();
        [DataMember]
        private FastDictionary2<TKey, Queue<ActiveEvent>> stateDictionary = new FastDictionary2<TKey, Queue<ActiveEvent>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public SessionWindowPipe() { }

        public SessionWindowPipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long sessionTimeout, long maximumDuration)
            : base(stream, observer)
        {
            this.sessionTimeout = sessionTimeout;
            this.maximumDuration = maximumDuration;
            this.pool = MemoryManager.GetMemoryPool<TKey, TPayload>(stream.Properties.IsColumnar);
            this.errorMessages = stream.ErrorMessages;
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override void ProduceQueryPlan(PlanNode previous)
            => this.Observer.ProduceQueryPlan(new SessionWindowPlanNode(
                previous, this,
                typeof(TKey), typeof(TPayload), this.sessionTimeout, this.maximumDuration,
                false, this.errorMessages));

        // Sets the cut point of the key's session from the time of a data event, if the session has none: either
        // the key has no session (InfinitySyncTime), or its session was cut and this event continues it (MaxSyncTime).
        // Called after ReachTime has closed the sessions due by the event's time, so the event that starts or
        // continues a session sets that session's cut point.
        private void SetCutPoint(int pIndex, long timestamp)
        {
            if (this.maximumDuration < StreamEvent.InfinitySyncTime)
            {
                if (this.windowEndTimeDictionary.entries[pIndex].value == StreamEvent.InfinitySyncTime)
                {
                    long mod = timestamp % this.maximumDuration;
                    this.windowEndTimeDictionary.entries[pIndex].value = timestamp - mod + ((mod == 0 ? 1 : 2) * this.maximumDuration);
                }
                else if (this.windowEndTimeDictionary.entries[pIndex].value == StreamEvent.MaxSyncTime)
                {
                    // A session closed at its cut point before its timeout stays marked as continuing, even when a
                    // punctuation or another key closed it. If its timeout has passed since its last event, this event
                    // starts a new session instead.
                    long mod = timestamp % this.maximumDuration;
                    this.windowEndTimeDictionary.entries[pIndex].value = timestamp >= this.lastDataTimeDictionary.entries[pIndex].value + this.sessionTimeout
                        ? timestamp - mod + ((mod == 0 ? 1 : 2) * this.maximumDuration)
                        : timestamp - mod + this.maximumDuration;
                }
            }
        }

        private void ReachTime(long timestamp)
        {
            var current = this.orderedKeys.First;
            while (current != null)
            {
                this.lastDataTimeDictionary.Lookup(current.Value, out int cIndex);
                var threshold = this.lastDataTimeDictionary.entries[cIndex].value == long.MinValue
                    ? this.windowEndTimeDictionary.entries[cIndex].value
                    : Math.Min(this.lastDataTimeDictionary.entries[cIndex].value + this.sessionTimeout, this.windowEndTimeDictionary.entries[cIndex].value);
                if (timestamp >= threshold)
                {
                    var queue = this.stateDictionary.entries[cIndex].value;
                    while (queue.Any())
                    {
                        var active = queue.Dequeue();

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = threshold;
                        this.output.vother.col[ind] = active.Sync;
                        this.output.key.col[ind] = active.Key;
                        this.output[ind] = active.Payload;
                        this.output.hash.col[ind] = active.Hash;

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                    if (timestamp < this.lastDataTimeDictionary.entries[cIndex].value + this.sessionTimeout)
                        this.windowEndTimeDictionary.entries[cIndex].value = StreamEvent.MaxSyncTime;
                    else
                    {
                        this.windowEndTimeDictionary.Remove(current.Value);
                        this.lastDataTimeDictionary.Remove(current.Value);
                        this.stateDictionary.Remove(current.Value);
                    }

                    this.orderedKeys.RemoveFirst();
                    current = this.orderedKeys.First;
                }
                else break;
            }
        }

        // Inserts the key's node into orderedKeys after every key whose session ends at or before the given
        // threshold. ReachTime closes keys from the front of the list and stops at the first one not yet due, so
        // the list must stay in the order in which the sessions end, or end edges come out of time order.
        private void InsertInOrder(LinkedListNode<TKey> node, long threshold)
        {
            var previous = this.orderedKeys.Last;
            while (previous != null)
            {
                this.lastDataTimeDictionary.Lookup(previous.Value, out int pIndex);
                var previousThreshold = this.lastDataTimeDictionary.entries[pIndex].value == long.MinValue
                    ? this.windowEndTimeDictionary.entries[pIndex].value
                    : Math.Min(this.lastDataTimeDictionary.entries[pIndex].value + this.sessionTimeout, this.windowEndTimeDictionary.entries[pIndex].value);
                if (previousThreshold <= threshold) break;
                previous = previous.Previous;
            }

            if (previous == null) this.orderedKeys.AddFirst(node);
            else this.orderedKeys.AddAfter(previous, node);
        }

        private int AllocatePartition(TKey pKey)
        {
            this.windowEndTimeDictionary.Insert(pKey, StreamEvent.InfinitySyncTime);
            this.lastDataTimeDictionary.Insert(pKey, long.MinValue);
            return this.stateDictionary.Insert(pKey, new Queue<ActiveEvent>());
        }

        public override unsafe void OnNext(StreamMessage<TKey, TPayload> batch)
        {
            var count = batch.Count;

            fixed (long* bv = batch.bitvector.col)
            fixed (long* vsync = batch.vsync.col)
            fixed (long* vother = batch.vother.col)
            fixed (int* hash = batch.hash.col)
            {
                for (int i = 0; i < count; i++)
                {
                    if ((bv[i >> 6] & (1L << (i & 0x3f))) == 0)
                    {
                        if (vsync[i] > vother[i]) // We have an end edge
                        {
                            ReachTime(vsync[i]);
                        }
                        else
                        {
                            // Close the sessions due to end by this event's time first, so that the event that
                            // starts or continues the key's session sets its cut point
                            ReachTime(vsync[i]);

                            // Check to see if the key is being tracked: advancing time may have removed it
                            if (!this.lastDataTimeDictionary.Lookup(batch.key.col[i], out int keyIndex))
                                keyIndex = AllocatePartition(batch.key.col[i]);
                            SetCutPoint(keyIndex, vsync[i]);

                            var newThreshold = Math.Min(vsync[i] + this.sessionTimeout, this.windowEndTimeDictionary.entries[keyIndex].value);
                            if (!this.stateDictionary.entries[keyIndex].value.Any())
                                InsertInOrder(new LinkedListNode<TKey>(batch.key.col[i]), newThreshold);
                            else
                            {
                                var oldThreshold = Math.Min(this.lastDataTimeDictionary.entries[keyIndex].value + this.sessionTimeout, this.windowEndTimeDictionary.entries[keyIndex].value);
                                if (newThreshold > oldThreshold)
                                {
                                    var node = this.orderedKeys.Find(batch.key.col[i]);
                                    this.orderedKeys.Remove(node);
                                    InsertInOrder(node, newThreshold);
                                }
                            }

                            this.lastDataTimeDictionary.entries[keyIndex].value = vsync[i];
                            this.stateDictionary.entries[keyIndex].value.Enqueue(new ActiveEvent
                            {
                                Key = batch.key.col[i],
                                Sync = vsync[i],
                                Hash = hash[i],
                                Payload = batch.payload.col[i],
                            });

                            int ind = this.output.Count++;
                            this.output.vsync.col[ind] = vsync[i];
                            this.output.vother.col[ind] = StreamEvent.InfinitySyncTime;
                            this.output.key.col[ind] = batch.key.col[i];
                            this.output[ind] = batch.payload.col[i];
                            this.output.hash.col[ind] = hash[i];

                            if (this.output.Count == Config.DataBatchSize) FlushContents();
                        }
                    }
                    else if (vother[i] == long.MinValue)
                    {
                        ReachTime(vsync[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = vsync[i];
                        this.output.vother.col[ind] = long.MinValue;
                        this.output.key.col[ind] = batch.key.col[i];
                        this.output[ind] = batch.payload.col[i];
                        this.output.hash.col[ind] = hash[i];
                        this.output.bitvector.col[ind >> 6] |= (1L << (ind & 0x3f));

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                }
            }

            batch.Free();
        }

        protected override void FlushContents()
        {
            if (this.output.Count == 0) return;
            this.output.Seal();
            this.Observer.OnNext(this.output);
            this.pool.Get(out this.output);
            this.output.Allocate();
        }

        public override int CurrentlyBufferedOutputCount => this.output.Count;

        public override int CurrentlyBufferedInputCount
        {
            get
            {
                int count = 0;
                int iter = FastDictionary<TKey, Queue<ActiveEvent>>.IteratorStart;
                while (this.stateDictionary.Iterate(ref iter)) count += this.stateDictionary.entries[iter].value.Count();
                return count;
            }
        }

        protected override void UpdatePointers()
        {
            int iter = FastDictionary<TKey, long>.IteratorStart;
            var temp = new List<Tuple<TKey, long>>();
            while (this.lastDataTimeDictionary.Iterate(ref iter))
            {
                if (this.stateDictionary.entries[iter].value.Any())
                {
                    temp.Add(Tuple.Create(
                        this.windowEndTimeDictionary.entries[iter].key,
                        Math.Min(this.lastDataTimeDictionary.entries[iter].value + this.sessionTimeout, this.windowEndTimeDictionary.entries[iter].value)));
                }
            }
            foreach (var item in temp.OrderBy(o => o.Item2)) this.orderedKeys.AddLast(new LinkedListNode<TKey>(item.Item1));
            base.UpdatePointers();
        }

        protected override void DisposeState()
        {
            this.output.Free();
            this.windowEndTimeDictionary.Clear();
            this.lastDataTimeDictionary.Clear();
            this.stateDictionary.Clear();
        }

        [DataContract]
        private struct ActiveEvent
        {
            [DataMember]
            public TPayload Payload;
            [DataMember]
            public TKey Key;
            [DataMember]
            public int Hash;
            [DataMember]
            public long Sync;

            public override string ToString() => "Key='" + this.Key + "', Payload='" + this.Payload;
        }
    }
}