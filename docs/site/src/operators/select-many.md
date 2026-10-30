# SelectMany

Turns each event into zero or more events, each with the input event's lifetime.

<div class="trill-marbles" data-op="select-many"></div>

The selector returns a sequence of payloads for each input payload. Each payload in the sequence becomes an output event with the input event's lifetime. An empty sequence drops the event, so `SelectMany` can filter as well as expand.

In the diagram, each event becomes as many events as its value: `a:1` gives one, `b:2` two and `a:3` three, all with their input's lifetime.

An overload takes `(start, payload)`, as [Select (with start time)](select-start-time.md) does. On a grouped stream, `SelectManyByKey` also passes the grouping key, as [SelectByKey](select-by-key.md) does.

**See also:** [Select](select.md), [Where](where.md), [SelectByKey](select-by-key.md)
