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
    internal sealed class PartitionedSessionWindowPipe<TKey, TPayload, TPartitionKey> : UnaryPipe<TKey, TPayload, TPayload>
    {
        private readonly MemoryPool<TKey, TPayload> pool;
        private readonly string errorMessages;
        private readonly Func<TKey, TPartitionKey> getPartitionKey = GetPartitionExtractor<TPartitionKey, TKey>();

        [SchemaSerialization]
        private readonly long sessionTimeout;
        [SchemaSerialization]
        private readonly long maximumDuration;

        [DataMember]
        private StreamMessage<TKey, TPayload> output;

        private Dictionary<TPartitionKey, LinkedList<TKey>> orderedKeysDictionary = new Dictionary<TPartitionKey, LinkedList<TKey>>();
        [DataMember]
        private FastDictionary2<TKey, long> windowEndTimeDictionary = new FastDictionary2<TKey, long>();
        [DataMember]
        private FastDictionary2<TKey, long> lastDataTimeDictionary = new FastDictionary2<TKey, long>();
        [DataMember]
        private FastDictionary2<TKey, Queue<ActiveEvent>> stateDictionary = new FastDictionary2<TKey, Queue<ActiveEvent>>();

        [Obsolete("Used only by serialization. Do not call directly.")]
        public PartitionedSessionWindowPipe() { }

        public PartitionedSessionWindowPipe(IStreamable<TKey, TPayload> stream, IStreamObserver<TKey, TPayload> observer, long sessionTimeout, long maximumDuration)
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

        private void ReachTime(long timestamp)
        {
            foreach (var pKey in this.orderedKeysDictionary.Keys) ReachTime(timestamp, pKey);
        }

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

        private void ReachTime(long timestamp, TPartitionKey pKey)
        {
            var orderedKeys = this.orderedKeysDictionary[pKey];
            var current = orderedKeys.First;
            while (current != null)
            {
                this.lastDataTimeDictionary.Lookup(current.Value, out int cIndex);

                var threshhold = this.lastDataTimeDictionary.entries[cIndex].value == long.MinValue
                    ? this.windowEndTimeDictionary.entries[cIndex].value
                    : Math.Min(this.lastDataTimeDictionary.entries[cIndex].value + this.sessionTimeout, this.windowEndTimeDictionary.entries[cIndex].value);
                if (timestamp >= threshhold)
                {
                    var queue = this.stateDictionary.entries[cIndex].value;
                    while (queue.Any())
                    {
                        var active = queue.Dequeue();

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = threshhold;
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
                    orderedKeys.RemoveFirst();
                    current = orderedKeys.First;
                }
                else break;
            }
        }

        private int AllocatePartition(TKey key, TPartitionKey pKey)
        {
            if (!this.orderedKeysDictionary.ContainsKey(pKey)) this.orderedKeysDictionary.Add(pKey, new LinkedList<TKey>());
            this.windowEndTimeDictionary.Insert(key, StreamEvent.InfinitySyncTime);
            this.lastDataTimeDictionary.Insert(key, long.MinValue);
            return this.stateDictionary.Insert(key, new Queue<ActiveEvent>());
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
                        var partition = this.getPartitionKey(batch.key.col[i]);
                        if (vsync[i] > vother[i]) // We have an end edge
                        {
                            ReachTime(vsync[i], partition);
                        }
                        else
                        {
                            // Close the partition's sessions due to end by this event's time first, so that the
                            // event that starts or continues the key's session sets its cut point
                            if (this.orderedKeysDictionary.ContainsKey(partition)) ReachTime(vsync[i], partition);

                            // Check to see if the key is being tracked: advancing time may have removed it
                            if (!this.lastDataTimeDictionary.Lookup(batch.key.col[i], out int keyIndex))
                                keyIndex = AllocatePartition(batch.key.col[i], partition);
                            SetCutPoint(keyIndex, vsync[i]);

                            if (!this.stateDictionary.entries[keyIndex].value.Any())
                                this.orderedKeysDictionary[partition].AddLast(new LinkedListNode<TKey>(batch.key.col[i]));
                            else
                            {
                                var oldThreshhold = Math.Min(this.lastDataTimeDictionary.entries[keyIndex].value + this.sessionTimeout, this.windowEndTimeDictionary.entries[keyIndex].value);
                                var newThreshhold = Math.Min(vsync[i] + this.sessionTimeout, this.windowEndTimeDictionary.entries[keyIndex].value);
                                if (newThreshhold > oldThreshhold)
                                {
                                    var orderedKeys = this.orderedKeysDictionary[partition];
                                    var node = orderedKeys.Find(batch.key.col[i]);
                                    orderedKeys.Remove(node);
                                    orderedKeys.AddLast(node);
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
                    else if (vother[i] == PartitionedStreamEvent.LowWatermarkOtherTime)
                    {
                        ReachTime(vsync[i]);

                        int ind = this.output.Count++;
                        this.output.vsync.col[ind] = vsync[i];
                        this.output.vother.col[ind] = PartitionedStreamEvent.LowWatermarkOtherTime;
                        this.output.key.col[ind] = default;
                        this.output[ind] = default;
                        this.output.hash.col[ind] = 0;
                        this.output.bitvector.col[ind >> 6] |= (1L << (ind & 0x3f));

                        if (this.output.Count == Config.DataBatchSize) FlushContents();
                    }
                    else if (vother[i] == PartitionedStreamEvent.PunctuationOtherTime)
                    {
                        var partition = this.getPartitionKey(batch.key.col[i]);
                        if (!this.lastDataTimeDictionary.Lookup(batch.key.col[i], out int keyIndex))
                        {
                            keyIndex = AllocatePartition(batch.key.col[i], partition);
                        }

                        ReachTime(vsync[i], partition);

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
            var temp = new List<Tuple<TKey, long, TPartitionKey>>();
            while (this.lastDataTimeDictionary.Iterate(ref iter))
            {
                if (this.stateDictionary.entries[iter].value.Any())
                {
                    temp.Add(Tuple.Create(
                        this.lastDataTimeDictionary.entries[iter].key,
                        Math.Min(this.lastDataTimeDictionary.entries[iter].value + this.sessionTimeout, this.windowEndTimeDictionary.entries[iter].value), this.getPartitionKey(this.lastDataTimeDictionary.entries[iter].key)));
                }
            }
            foreach (var item in temp.OrderBy(o => o.Item2))
            {
                if (!this.orderedKeysDictionary.TryGetValue(item.Item3, out var orderedKeys))
                {
                    orderedKeys = new LinkedList<TKey>();
                    this.orderedKeysDictionary.Add(item.Item3, orderedKeys);
                }
                orderedKeys.AddLast(new LinkedListNode<TKey>(item.Item1));
            }
            base.UpdatePointers();
        }

        protected override void DisposeState()
        {
            this.output.Free();
            this.orderedKeysDictionary.Clear();
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