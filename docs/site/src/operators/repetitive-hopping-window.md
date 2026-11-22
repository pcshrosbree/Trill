# RepetitiveHoppingWindowLifetime

Like HoppingWindowLifetime, but cuts each event into one piece per hop, so an aggregate reports a result at every hop.

<div class="trill-marbles" data-op="repetitive-hopping-window"></div>

> **Columnar mode differs:** in columnar mode this query throws `InvalidOperationException` while it is built, because `Ev` is a class with default equality. Row mode, which the diagram uses, gives the events shown. (#164)

`RepetitiveHoppingWindowLifetime(size, period)` is [HoppingWindowLifetime](hopping-window.md) with the same arguments, followed by [Chop](chop.md) at every multiple of `period`. The period must be positive.

In the diagram (size 4, period 2), HoppingWindowLifetime gives `a:1` and `b:2` the lifetime `[2,6)`, and Chop cuts it into `[2,4)` and `[4,6)`. Likewise `a:3` at 4 gives `[4,6)` and `[6,8)`, `b:4` at 7 gives `[8,10)` and `[10,12)`, and `a:5` at 9 gives `[10,12)` and `[12,14)`.

After HoppingWindowLifetime, an aggregate reports only when its result changes. After this operator, it reports at every hop, even when the result is the same.

**See also:** [HoppingWindowLifetime](hopping-window.md), [Chop](chop.md), [Sum (hopping window)](sum.md)
