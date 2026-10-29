# AlterEventDuration

Gives every event a new duration, keeping its start time.

<div class="trill-marbles" data-op="alter-event-duration"></div>

`AlterEventDuration(d)` sets each event's lifetime to `[start, start + d)`. It is the usual way to turn one-tick events into intervals, so that later operators such as joins and patterns can see them overlap.

In the diagram each one-tick input becomes a three-tick interval: `a:1` at 1 becomes `[1,4)`.

**See also:** [ClipEventDuration](clip-event-duration.md), [TumblingWindowLifetime](tumbling-window.md), [Detect](detect.md)
