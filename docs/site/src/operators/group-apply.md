# GroupApply

Splits a stream by key, runs the same query on each group, and merges the results.

<div class="trill-marbles" data-op="group-apply"></div>

`GroupApply(keySelector, g => query, resultSelector)` runs `query` once per key and gives the result selector the group key, so each output can say which group it came from. This is how per-key windowed statistics are written.

In the diagram, `TumblingWindowLifetime(5)` puts the events at 1, 2 and 4 in `[5,10)` and those at 7 and 9 in `[10,15)`. Per key: `a` averages 1 and 3 to 2 in `[5,10)`; `b` has only 2 there; in `[10,15)` `b` is 4 and `a` is 5.

**See also:** [Count (tumbling window)](count.md), [Sum (hopping window)](sum.md), [TumblingWindowLifetime](tumbling-window.md), [SelectByKey](select-by-key.md)
