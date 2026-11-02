# Max

Finds the largest value among the events alive at each moment.

<div class="trill-marbles" data-op="max"></div>

`Max(selector)` is a snapshot aggregate: there is no window here, so the result at each moment is the largest value among the events alive at that moment. After `TumblingWindowLifetime(4)`, the same query gives one maximum per window.

In the diagram, only `a:1` is alive in `[1,3)`, so the maximum is 1. It is 2 from 3, when `a:2` starts, and 3 from 4, when `b:3` starts. At 6, `a:1` ends: the maximum is still 3, but a new result starts because the set of events changed. At 7, `b:3` ends and the maximum falls back to 2 until `a:2` ends at 8. The key plays no part: `Max` compares all the events alive.

Each result is emitted as a start edge and an end edge.

A second argument gives the order as a comparison: `Max(e => e.Value, (x, y) => y.CompareTo(x))` reverses it and gives the smallest value. With a selector that returns a nullable value, nulls are left out, and a window whose values are all null gives null.

**See also:** [Min (hopping window)](min.md), [TopK (tumbling window)](top-k.md), [TumblingWindowLifetime](tumbling-window.md), [Aggregate (several aggregates)](aggregate.md)
