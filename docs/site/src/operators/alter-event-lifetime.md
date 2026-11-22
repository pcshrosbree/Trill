# AlterEventLifetime

Gives every event a new start time and a new duration, both computed from its original start time.

<div class="trill-marbles" data-op="alter-event-lifetime"></div>

`AlterEventLifetime(startSelector, durationSelector)` calls both functions with the event's original start time. The first gives the new start, and the second gives the new duration.

In the diagram, the new start is `start + 1` and the new duration is `start`. So `a:1` at 1 becomes `[2,3)`, `b:2` at 2 becomes `[3,5)`, `a:3` at 4 becomes `[5,9)`, `b:4` at 7 becomes `[8,15)`, and `a:5` at 9 becomes `[10,19)`.

Two other overloads take a duration function of the original start and end times, `(start, end) => ...`, or a fixed duration.

**See also:** [ShiftEventLifetime](shift-event-lifetime.md), [AlterEventDuration](alter-event-duration.md), [ExtendLifetime](extend-lifetime.md)
