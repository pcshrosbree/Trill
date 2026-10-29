# ClipEventDuration

Ends each left event when a right event with the same key starts.

<div class="trill-marbles" data-op="clip-event-duration"></div>

For each left event, the first right event with the same key that starts after the left event's start cuts it short: the left event ends there. Left events with no such right event keep their lifetime.

In the diagram, `a:1 [1,5)` is clipped by `a:10` at 2 and becomes `[1,2)`; `b:2 [3,9)` is clipped by `b:20` at 4 and becomes `[3,4)`; `a:3 [6,8)` keeps its lifetime, because the next `a` on the right starts at 10.

The clipped events are emitted as a **start edge and an end edge**: when the left event starts, Trill cannot yet know whether it will be clipped. See [reading the diagrams](../introduction.md#how-trill-emits-an-event-intervals-and-edges).

**See also:** [AlterEventDuration](alter-event-duration.md), [WhereNotExists](where-not-exists.md)
