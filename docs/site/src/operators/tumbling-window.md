# TumblingWindowLifetime

Moves each event into a fixed, non-overlapping window, ready for an aggregate.

<div class="trill-marbles" data-op="tumbling-window"></div>

`TumblingWindowLifetime(size)` is `HoppingWindowLifetime(size, size)`. It snaps each event's start **up** to the next multiple of `size` (or keeps it, if it is already on a boundary) and gives it a lifetime of `size`. An event with start in `(k·size − size, k·size]` therefore lives in `[k·size, k·size + size)`: everything that happened in one window becomes visible together, when that window closes.

In the diagram (size 4), the events at 1, 2 and 4 all live in `[4,8)`; the event at 7 lives in `[8,12)` and the one at 9 in `[12,16)`. Note that the event at exactly 4 lands with the events at 1 and 2, not with 7.

Follow it with an aggregate to compute per-window results: see [Count](count.md).

**See also:** [HoppingWindowLifetime](hopping-window.md), [Count (tumbling window)](count.md), [GroupApply](group-apply.md)
