# Min (hopping window)

Finds the smallest value among the events alive at each moment; after a hopping window, that is the minimum per hop.

<div class="trill-marbles" data-op="min"></div>

After `HoppingWindowLifetime(4, 2)` each event lives for 4 ticks starting on a 2-tick boundary, so an event counts in two hops. In the diagram the minimum is 1 in `[2,4)`, while `a:1` and `b:2` are alive. When `a:3` joins at 4 the minimum stays 1, but the set of events has changed, so a new result starts. From 6, only `a:3` is alive, and the minimum is 3. It is then 4 in `[8,10)` and `[10,12)`, while `b:4` is alive, and 5 in `[12,14)`.

Each result is emitted as a start edge and an end edge.

A second argument gives the order as a comparison: `Min(e => e.Value, (x, y) => y.CompareTo(x))` reverses it and gives the largest value. With a selector that returns a nullable value, nulls are left out, and a window whose values are all null gives null.

**See also:** [Max](max.md), [HoppingWindowLifetime](hopping-window.md), [TopK (tumbling window)](top-k.md), [Aggregate (several aggregates)](aggregate.md)
