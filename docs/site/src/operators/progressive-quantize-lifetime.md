# ProgressiveQuantizeLifetime

Like QuantizeLifetime, but moves each start back only as far as the progress interval.

<div class="trill-marbles" data-op="progressive-quantize-lifetime"></div>

> **Columnar mode differs:** in columnar mode this query throws `NullReferenceException` when it runs, for a class payload with fields such as `Ev`. Row mode, which the diagram uses, gives the events shown. (#163)

`ProgressiveQuantizeLifetime(size, period, progress)` moves each start back to the nearest multiple of `progress` at or before it. It moves each end forward as [QuantizeLifetime](quantize-lifetime.md) does.

In the diagram (size 4, period 4, progress 2), `a:1 [1,5)` becomes `[0,8)`, `b:2 [3,9)` becomes `[2,12)`, and `a:3 [6,8)` stays `[6,8)`. Compare QuantizeLifetime, which moves `b:2` back to 0 and `a:3` back to 4.

`progress` must divide `period` exactly and be smaller than it. Otherwise the call throws `ArgumentException`.

**See also:** [QuantizeLifetime](quantize-lifetime.md), [HoppingWindowLifetime](hopping-window.md)
