# Chop

Cuts every event at each multiple of a period, so that no piece crosses a boundary.

<div class="trill-marbles" data-op="chop"></div>

> **Columnar mode differs:** in columnar mode this query throws `InvalidOperationException` while it is built, because `Ev` is a class with default equality. Row mode, which the diagram uses, gives the events shown. (#164)

`Chop(offset, period)` cuts each event at every boundary inside it. The boundaries are `offset` plus each multiple of `period`. Each piece keeps the event's payload. The period must be positive.

In the diagram (offset 0, period 3), `a:1 [1,5)` is cut at 3 into `[1,3)` and `[3,5)`, and `b:2 [3,9)` is cut at 6 into `[3,6)` and `[6,9)`. `a:3 [6,8)` crosses no boundary and stays whole.

With offset 1 and the same period, the boundaries are 1, 4 and 7, so `b:2 [3,9)` becomes `[3,4)`, `[4,7)` and `[7,9)`.

[Stitch](stitch.md) joins the pieces back together.

**See also:** [Stitch](stitch.md), [RepetitiveHoppingWindowLifetime](repetitive-hopping-window.md)
