# TopK (tumbling window)

Lists the k events with the largest values among the events alive at each moment, with their ranks.

<div class="trill-marbles" data-op="top-k"></div>

`TopK(selector, k)` orders the events alive at each moment by the selected value, largest first, and returns the first `k` of them as a list. Each item of the list is a `RankedEvent`, with the rank in `Rank` and the whole input payload in `Payload`. The diagram shows each item as `rank. key:value`.

In the diagram, `TumblingWindowLifetime(5)` puts `a:1`, `b:2` and `a:3` in `[5,10)`. The top two are `a:3` and `b:2`, and `a:1` is left out. In `[10,15)` the top two are `a:5` and `b:4`.

When fewer than `k` events are alive, the list is shorter. A comparison as an extra argument changes the order: `TopK(e => e.Value, (x, y) => y.CompareTo(x), 2)` lists the two smallest values instead.

Equal values share a rank, and the next rank skips, as SQL's `RANK` does: the values 5, 3, 3 and 1 rank 1, 2, 2 and 4. All the values tied for the last place are kept, so the list can be longer than `k`: with `k` of 2, those values give 5, 3 and 3.

**Known issue in this fork.** When two live events have the same value, `TopK` orders them by their payloads. If Trill has no comparison for the payload type, as for the diagram's `Ev`, the query throws an `ArgumentException` ("At least one object must implement IComparable") when the values tie. The diagram's values are all different, which avoids the problem. Payloads that Trill can compare work, such as `int`, `string`, an anonymous type or a `Tuple`. The cause is in the engine, and the fix belongs there.

**See also:** [Max](max.md), [Min (hopping window)](min.md), [TumblingWindowLifetime](tumbling-window.md), [Aggregate (several aggregates)](aggregate.md)
