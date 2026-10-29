# Detect

Finds sequences of events that match a pattern within a maximum duration.

<div class="trill-marbles" data-op="detect"></div>

`Detect` runs an automaton (Trill's AFA) over the stream. `SingleElement(predicate)` steps match one event each, in order. A match is reported when its last event arrives, and it lives until the **first** event of the match plus `maxDuration`.

In the diagram the pattern is "an `a`, then a `b`, within 4". `a` at 1 and `b` at 2 give a match `[2,5)`: it starts at 2, when the `b` completes it, and ends at 1 + 4. `a` at 4 and `b` at 7 give `[7,8)`, ending at 4 + 4. The `a` at 9 has no `b` after it.

Drag the `b` at 7 to 9 and the second match disappears: a match starting with the `a` at 4 must complete before 4 + 4. Drag it to 10, 11 or 12 and a new match forms instead, with the `a` at 9. From 13 on, that match has expired too, and only the first match remains.

**See also:** [AlterEventDuration](alter-event-duration.md), [Join](join.md)
