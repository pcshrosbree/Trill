# HoppingWindowLifetime

Moves each event into overlapping windows of a fixed size that start every period.

<div class="trill-marbles" data-op="hopping-window"></div>

When `size` is a multiple of `period`, as in the diagram, `HoppingWindowLifetime(size, period)` snaps each event's start **up** to the next multiple of `period` (a start already on a boundary stays) and gives it a lifetime of `size`. The event then stays alive across several consecutive hops, so an aggregate over the result sees it in each overlapping window.

Two other cases behave differently (see `HoppingWindowLifetime` in `LifetimeExtensions.cs`):

- When `period` is larger than `size`, the windows have gaps between them. Events whose start falls in a gap are dropped. The others are snapped up as above and get a lifetime of `size`.
- When `size` is larger than `period` but not a multiple of it, Trill snaps the start and the original start plus `size` to the period grid separately, so lifetimes can differ from `size`.

In the diagram (size 4, period 2), `a:1` at 1 and `b:2` at 2 both live in `[2,6)`, `a:3` at 4 in `[4,8)`, `b:4` at 7 in `[8,12)` and `a:5` at 9 in `[10,14)`.

Follow it with an aggregate to compute per-window results: see [Sum](sum.md).

**See also:** [TumblingWindowLifetime](tumbling-window.md), [Sum (hopping window)](sum.md)
