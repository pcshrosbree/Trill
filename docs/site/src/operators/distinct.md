# Distinct

Removes duplicates among the events alive at each moment.

<div class="trill-marbles" data-op="distinct"></div>

`Distinct` works on snapshots. At every moment, the output holds one event for each distinct value among the events alive then. With a selector, such as `e => e.Key`, two events are duplicates when the selector gives the same result for both, and the output payload is that result. Without a selector, `Distinct` compares whole payloads.

In the diagram, two `a` events overlap from 3 to 6, but the output has only one `a` at any moment. That `a` is split at 3 and at 6, where the number of `a` events alive changes. `Distinct` is built from `GroupApply` and `Count`, and each change in a group's count starts a new output event. The output arrives as start and end edges.

**See also:** [Where](where.md), [GroupApply](group-apply.md), [Count (tumbling window)](count.md)
