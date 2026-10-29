# Join

Pairs left and right events with equal keys whose lifetimes overlap; each pair lives for the overlap.

<div class="trill-marbles" data-op="join"></div>

For every left event and right event with the same join key, if their lifetimes overlap, the output has one event whose lifetime is the **intersection** of the two, with a payload made by the result selector.

In the diagram, `a:1 [1,5)` and `a:10 [2,4)` overlap in `[2,4)`; `b:2 [3,9)` and `b:20 [4,7)` overlap in `[4,7)`. `a:3 [6,8)` has no overlapping `a` on the right, so it produces nothing. Drag `a:30` left until it overlaps `a:3` to see a third pair appear.

To join events that happen within some span of each other, first give the inputs that span as their lifetime with [AlterEventDuration](alter-event-duration.md).

**See also:** [LeftOuterJoin](left-outer-join.md), [WhereNotExists](where-not-exists.md), [AlterEventDuration](alter-event-duration.md)
