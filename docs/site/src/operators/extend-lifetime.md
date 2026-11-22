# ExtendLifetime

Makes every event last longer by a fixed amount, keeping its start time.

<div class="trill-marbles" data-op="extend-lifetime"></div>

`ExtendLifetime(amount)` adds `amount` to each event's end time.

In the diagram (amount 2), `a:1 [1,5)` becomes `[1,7)`, `b:2 [3,9)` becomes `[3,11)`, and `a:3 [6,8)` becomes `[6,10)`.

A negative amount shortens each event instead. An event that would no longer outlast its start is dropped: with an amount of -3, `[1,5)` becomes `[1,2)`, while `[3,6)` and `[6,8)` disappear.

**See also:** [AlterEventDuration](alter-event-duration.md), [AlterEventLifetime](alter-event-lifetime.md), [ShiftEventLifetime](shift-event-lifetime.md)
