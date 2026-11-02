# PointAtEnd

Replaces each event with a one-tick event at its end.

<div class="trill-marbles" data-op="point-at-end"></div>

`PointAtEnd()` takes no arguments. Each event becomes an event with the same payload that starts at the input event's end and lasts one tick. The output is in the order of the ends, not the starts.

In the diagram, `a:1` lives in `[1,5)` and becomes `[5,6)`, `a:3` lives in `[6,8)` and becomes `[8,9)`, and `b:2` lives in `[3,9)` and becomes `[9,10)`. So `a:3` comes out before `b:2`, although it started later.

An event that never ends gives no output. An event sent as a separate start edge and end edge gives its point when the end edge arrives.

A common use is after a windowed aggregate, to report each window's result once, when the window closes: `TumblingWindowLifetime(4).Count().PointAtEnd()` gives one point per window, at the window's end.

**See also:** [AlterEventDuration](alter-event-duration.md), [TumblingWindowLifetime](tumbling-window.md), [Count (tumbling window)](count.md)
