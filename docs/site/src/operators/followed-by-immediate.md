# FollowedByImmediate

Finds an event that meets one condition, followed directly by an event that meets another, within a time limit.

<div class="trill-marbles" data-op="followed-by-immediate"></div>

> **Columnar mode differs:** in columnar mode this query throws `InvalidOperationException` while it is built. `FollowedByImmediate` clips the stream with itself, and the clip refuses `Ev`, a class with default equality. Row mode, which the diagram uses, finds the matches shown. (#159)

`FollowedByImmediate(firstMatch, secondMatch, resultSelector, withinDuration)` looks for an event for which `firstMatch` is true, followed by an event for which `secondMatch` is true. The second must be the next event to start, and it must start less than `withinDuration` after the first. The result selector builds the result from the two events, `(first, second)`. Each result lives for one tick, at the start of the second event.

In the diagram the pattern is "an `a`, then a `b`, within 4". The result is labelled `first,second` with the two values. The events at 1 and 2 give `1,2` at 2. `a:3` at 4 and `b:4` at 7 give `3,4` at 7, because 7 − 4 = 3 is less than 4. The `a` at 9 has no `b` after it.

Drag `b:4` from 7 to 8 and its match disappears: 8 − 4 = 4 is not less than 4. Drag `a:5` from 9 to 5 or 6 instead and the match moves to it: `a:5` now starts between `a:3` and `b:4`, so `b:4` no longer follows `a:3` directly, but it follows `a:5`.

"Directly" counts every event, not only the ones that match: an event of any kind that starts between the two prevents the match. With a time limit, only start times count: the events' own ends play no part.

`withinDuration` must be at least 2: the call refuses 0 and 1 with an `ArgumentException`. A negative `withinDuration` sets no time limit. The first event's own lifetime limits the match instead: the second event must start no later than the moment the first ends.

**See also:** [FollowedByImmediate (two streams)](followed-by-immediate-two-streams.md), [ConsecutivePairs](consecutive-pairs.md), [Detect](detect.md)
