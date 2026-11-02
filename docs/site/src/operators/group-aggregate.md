# GroupAggregate

Splits a stream by key and computes one or more aggregates per group, in one call.

<div class="trill-marbles" data-op="group-aggregate"></div>

`GroupAggregate(keySelector, aggregate1, ..., merger)` groups the events by the key selector. Each aggregate argument picks an aggregate from a window `w`, such as `w => w.Count()` or `w => w.Sum(e => e.Value)`, and is computed over the group's events alive at each moment. The merger receives the group, whose key is `g.Key`, and the aggregates' results in the order given. It takes from one to fifteen aggregates.

The output is the same as `GroupApply` with `Aggregate` on each group: `GroupApply(e => e.Key, g => g.Aggregate(...), ...)`, except in the columnar case described under the known issue below. `GroupAggregate` is shorter when every group needs only aggregates; use `GroupApply` when a group needs any other query.

In the diagram, `TumblingWindowLifetime(5)` puts the events at 1, 2 and 4 in `[5,10)` and those at 7 and 9 in `[10,15)`. In `[5,10)` group `a` has `a:1` and `a:3`, so its count is 2 and its sum 4, and group `b` has only `b:2`. In `[10,15)` each group has one event: `b:4` and `a:5`.

**Known issue in this fork.** In columnar mode, on a stream that Trill knows has only events that never end, such as the output of `AlterEventDuration(StreamEvent.InfinitySyncTime)`, the `GroupApply` form gives each later result of a group the start time of the group's first result, so the two forms differ there. Row mode, as in the diagram, gives the right answer. With the diagram's input events made endless and a count per key, `GroupAggregate` starts the later results at 4 (`a`), 7 (`b`) and 9 (`a`); the `GroupApply` form starts the same results at 1, 2 and 1. The cause is in the engine, and the fix belongs there.

**See also:** [GroupApply](group-apply.md), [Aggregate (several aggregates)](aggregate.md), [TumblingWindowLifetime](tumbling-window.md), [Count (tumbling window)](count.md)
