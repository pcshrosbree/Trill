# SelectByKey

Transforms each event's payload using the stream's key as well; lifetimes are unchanged.

<div class="trill-marbles" data-op="select-by-key"></div>

Every Trill stream has a key type, the `TKey` in `IStreamable<TKey, TPayload>`, and each event carries a key of that type alongside its payload. `SelectByKey` passes that key to the selector: `(key, payload) => result`. On a stream from `ToStreamable()` the key type is `Empty`, so the key says nothing. Inside a grouping operator such as `GroupApply`, the key holds the group: there it is a `CompoundGroupKey`, and its `InnerGroup` is the group's key.

The diagram uses `GroupApply` to make each event's `Key` field the stream key that the selector receives. Each output names its group: `a:1` becomes `1 in a`.

Two related forms take the start time as well, `(start, key, payload)`. `SelectManyByKey` is the same for [SelectMany](select-many.md), with `(key, payload)` or `(start, key, payload)`.

**See also:** [GroupApply](group-apply.md), [Select](select.md), [SelectMany](select-many.md)
