# SelectManyByKey

Turns each event into zero or more events using the stream's key as well; each keeps the input event's lifetime.

<div class="trill-marbles" data-op="select-many-by-key"></div>

`SelectManyByKey` is to [SelectMany](select-many.md) what [SelectByKey](select-by-key.md) is to [Select](select.md). Its selector receives the stream's key with the payload, `(key, payload) => sequence`. Each payload in the sequence becomes an output event with the input event's lifetime. Another overload takes the start time as well, `(start, key, payload)`.

The diagram uses `GroupApply` to make each event's `Key` field the stream key. Inside it, the key is a `CompoundGroupKey`, and its `InnerGroup` is the group's key. Each event becomes as many events as its value, and each names its group: `b:2` gives `2.1 in b` and `2.2 in b`.

**See also:** [SelectByKey](select-by-key.md), [SelectMany](select-many.md), [GroupApply](group-apply.md)
