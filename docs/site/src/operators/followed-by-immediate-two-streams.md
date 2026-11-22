# FollowedByImmediate (two streams)

Finds an event on the left followed directly by an event on the right, within a time limit.

<div class="trill-marbles" data-op="followed-by-immediate-two-streams"></div>

> **Columnar mode differs:** in columnar mode this query throws `InvalidOperationException` while it is built. `FollowedByImmediate` clips the left stream with the right one, and the clip refuses `Ev`, a class with default equality. Row mode, which the diagram uses, finds the match shown. (#159)

This overload of `FollowedByImmediate` takes a second stream: `left.FollowedByImmediate(right, firstMatch, secondMatch, resultSelector, withinDuration)`. The first event comes from the left stream and must meet `firstMatch`. The second comes from the right stream, must meet `secondMatch`, and must be the next event on the right to start after the first. It must also start less than `withinDuration` after the first. Each result lives for one tick, at the start of the right event.

Only the right stream's events count for "directly": an event on the right that starts between the two prevents the match, but an event on the left does not.

In the diagram the pattern is "an `a` on the left, then an `a` on the right, within 4". The result is labelled `left+right` with the two values. `a:1` at 1 and `a:10` at 2 give `1+10` at 2. `a:3` at 6 has no match: the next event on the right, `a:30` at 10, starts 4 later, and 4 is not less than 4. Drag `a:30` to 9 and `3+30` appears at 9.

`withinDuration` must be at least 2, as for the one-stream form, and a negative value sets no time limit.

**See also:** [FollowedByImmediate](followed-by-immediate.md), [ClipEventDuration](clip-event-duration.md), [Join](join.md)
