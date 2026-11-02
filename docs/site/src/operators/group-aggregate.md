# GroupAggregate

Splits a stream by key and computes one or more aggregates per group, in one call.

<div class="trill-marbles" data-op="group-aggregate"></div>

`GroupAggregate(keySelector, aggregate1, ..., merger)` groups the events by the key selector. Each aggregate argument picks an aggregate from a window `w`, such as `w => w.Count()` or `w => w.Sum(e => e.Value)`, and is computed over the group's events alive at each moment. The merger receives the group, whose key is `g.Key`, and the aggregates' results in the order given. It takes from one to fifteen aggregates.

The output is the same as `GroupApply` with `Aggregate` on each group: `GroupApply(e => e.Key, g => g.Aggregate(...), ...)`. `GroupAggregate` is shorter when every group needs only aggregates; use `GroupApply` when a group needs any other query.

In the diagram, `TumblingWindowLifetime(5)` puts the events at 1, 2 and 4 in `[5,10)` and those at 7 and 9 in `[10,15)`. In `[5,10)` group `a` has `a:1` and `a:3`, so its count is 2 and its sum 4, and group `b` has only `b:2`. In `[10,15)` each group has one event: `b:4` and `a:5`.

**See also:** [GroupApply](group-apply.md), [Aggregate (several aggregates)](aggregate.md), [TumblingWindowLifetime](tumbling-window.md), [Count (tumbling window)](count.md)
