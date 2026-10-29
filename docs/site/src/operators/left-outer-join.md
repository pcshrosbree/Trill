# LeftOuterJoin

Like Join, but a left event also appears, through a separate selector, for the times when nothing matches it.

<div class="trill-marbles" data-op="left-outer-join"></div>

The output covers each left event's whole lifetime. Where a matching right event overlaps it, the inner selector makes the payload; for the times when no matching right event is alive, the outer selector does. A left event is therefore cut into time slices.

In the diagram, `a:1 [1,5)` becomes `a:1+none [1,2)`, `a:1+10 [2,4)` and `a:1+none [4,5)`; `b:2 [3,9)` becomes `[3,4)` unmatched, `[4,7)` matched and `[7,9)` unmatched; `a:3 [6,8)` is unmatched throughout.

The first unmatched slice of `a:1` and of `b:2` is emitted as a **start edge and an end edge**: when it starts, Trill cannot yet know when a match will arrive.

**See also:** [Join](join.md), [WhereNotExists](where-not-exists.md)
