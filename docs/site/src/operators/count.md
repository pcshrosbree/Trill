# Count (tumbling window)

Counts the events alive at each moment; after a tumbling window, that is a count per window.

<div class="trill-marbles" data-op="count"></div>

Trill aggregates are **snapshot** aggregates: the result at each moment is computed over the events alive at that moment, and a new output event starts whenever that set changes. Windowing operators set lifetimes so that "alive at the same moment" means "in the same window".

In the diagram, `TumblingWindowLifetime(4)` puts the events at 1, 2 and 4 in `[4,8)`, so the count is 3 there; the window `[8,12)` holds one event and `[12,16)` one.

**See also:** [TumblingWindowLifetime](tumbling-window.md), [Sum (hopping window)](sum.md), [GroupApply](group-apply.md)
