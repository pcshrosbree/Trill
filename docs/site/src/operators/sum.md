# Sum (hopping window)

Sums a value over the events alive at each moment; after a hopping window, that is a sum per hop.

<div class="trill-marbles" data-op="sum"></div>

After `HoppingWindowLifetime(4, 2)` each event lives for 4 ticks starting on a 2-tick boundary, so consecutive hops overlap and an event counts in two of them.

In the diagram the result is 3 in `[2,4)` (`a:1` and `b:2`), 6 in `[4,6)` (`a:3` joins them), 3 in `[6,8)` (only `a:3`), 4 in `[8,10)`, 9 in `[10,12)` (`b:4` and `a:5`) and 5 in `[12,14)`.

Each result is emitted as a **start edge and an end edge**: the aggregate cannot know when the set of alive events will next change.

**See also:** [HoppingWindowLifetime](hopping-window.md), [Count (tumbling window)](count.md), [GroupApply](group-apply.md)
