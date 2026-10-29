# WhereNotExists

Keeps each left event only for the times when no right event with the same key is alive.

<div class="trill-marbles" data-op="where-not-exists"></div>

An anti-semi-join: the output is the left payload, alive for the parts of its lifetime not covered by any right event with an equal key. Trill has no `WhereExists` counterpart. A [Join](join.md) that projects the left payload comes close, but it emits one copy of the left event for each overlapping right event with the same key, where a semi-join would emit one.

In the diagram, `a:1 [1,5)` survives as `[1,2)` and `[4,5)` around `a:10 [2,4)`; `b:2 [3,9)` survives as `[3,4)` and `[7,9)` around `b:20 [4,7)`; `a:3 [6,8)` survives whole.

As with [LeftOuterJoin](left-outer-join.md), slices whose end Trill cannot know when they start are emitted as **start and end edges**.

**See also:** [Join](join.md), [LeftOuterJoin](left-outer-join.md), [ClipEventDuration](clip-event-duration.md)
