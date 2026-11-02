# ConsecutivePairs

Pairs each event with the event before it.

<div class="trill-marbles" data-op="consecutive-pairs"></div>

`ConsecutivePairs(resultSelector)` calls the result selector with each event and the next one, `(first, second)`. The result lives for one tick at the start of the second event. Only the start times count: the events' ends play no part, so an event pairs with the next one to start even if it has already ended, or is still alive.

In the diagram the result is labelled `first,second` with the two values, and coloured by the second event's key. The events at 1 and 2 give `1,2` at 2; then come `2,3` at 4, `3,4` at 7 and `4,5` at 9. The first event has no event before it. Each result is emitted as a start edge and an end edge.

Events that start at the same time are not paired with each other. Each of them is paired with each event of the previous start time, so two events at 1 and two at 3 give four pairs at 3.

On a stream with no grouping key, as here, all events are paired regardless of key. Inside `GroupApply`, pairs are formed within each key: `a` would give `1,3` at 4 and `3,5` at 9, and `b` would give `2,4` at 7.

`ConsecutivePairs` clips the stream with itself. In columnar mode, when Trill's columnar code cannot use the payload type's equality, as for the diagram's `Ev`, that clip runs in [row mode](../row-mode.md), and the pairs are the same as in row mode.

**See also:** [Detect](detect.md), [ClipEventDuration](clip-event-duration.md), [GroupApply](group-apply.md)
