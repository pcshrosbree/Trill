# ShiftEventLifetime

Moves every event by the same amount of time, keeping its duration.

<div class="trill-marbles" data-op="shift-event-lifetime"></div>

`ShiftEventLifetime(amount)` adds `amount` to each event's start and to its end.

In the diagram (amount 2), `a:1 [1,5)` becomes `[3,7)`, `b:2 [3,9)` becomes `[5,11)`, and `a:3 [6,8)` becomes `[8,10)`.

A second overload, `ShiftEventLifetime(start => ...)`, takes a function of each event's start time, so each event can move by its own amount.

**See also:** [AlterEventLifetime](alter-event-lifetime.md), [AlterEventDuration](alter-event-duration.md)
