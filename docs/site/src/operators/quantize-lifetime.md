# QuantizeLifetime

Widens every event to window boundaries: its start moves back to one, and its end moves forward to one.

<div class="trill-marbles" data-op="quantize-lifetime"></div>

> **Columnar mode differs:** in columnar mode this query throws `NullReferenceException` when it runs, for a class payload with fields such as `Ev`. Row mode, which the diagram uses, gives the events shown. (#163)

`QuantizeLifetime(size, period)` never makes an event shorter: no start moves later and no end moves earlier. When `size` equals `period`, as in the diagram, each start moves back to the nearest multiple of `period` at or before it, and each end moves forward to the nearest multiple at or after it.

In the diagram (size 4, period 4), `a:1 [1,5)` becomes `[0,8)`, `b:2 [3,9)` becomes `[0,12)`, and `a:3 [6,8)` becomes `[4,8)`.

**See also:** [ProgressiveQuantizeLifetime](progressive-quantize-lifetime.md), [HoppingWindowLifetime](hopping-window.md), [TumblingWindowLifetime](tumbling-window.md)
