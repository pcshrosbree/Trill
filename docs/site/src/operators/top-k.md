# TopK (tumbling window)

Lists the k events with the largest values among the events alive at each moment, with their ranks.

<div class="trill-marbles" data-op="top-k"></div>

`TopK(selector, k)` orders the events alive at each moment by the selected value, largest first, and returns the first `k` of them as a list. Each item of the list is a `RankedEvent`, with the rank in `Rank` and the whole input payload in `Payload`. The diagram shows each item as `rank. key:value`.

In the diagram, `TumblingWindowLifetime(5)` puts `a:1`, `b:2` and `a:3` in `[5,10)`. The top two are `a:3` and `b:2`, and `a:1` is left out. In `[10,15)` the top two are `a:5` and `b:4`.

When fewer than `k` events are alive, the list is shorter. A comparison as an extra argument changes the order: `TopK(e => e.Value, (x, y) => y.CompareTo(x), 2)` lists the two smallest values instead.

Equal values share a rank, and the next rank skips, as SQL's `RANK` does: the values 5, 3, 3 and 1 rank 1, 2, 2 and 4. All the values tied for the last place are kept, so the list can be longer than `k`: with `k` of 2, those values give 5, 3 and 3.

Tied events are listed in the order of their payloads. A payload type that Trill can compare keeps its own order, such as `int`, `string`, an anonymous type, a `Tuple` or a type with `IComparable`. So does a payload whose value implements `IComparable` at run time, such as a boxed `int` in a stream of `object`. For a type with no comparison, such as the diagram's `Ev`, tied payloads are ordered by their public fields and then their public properties that have a public getter and no index parameters, compared one after another: `Ev`'s `Key`, then its `Value`. Each member is compared by its own type's comparison, with null first. Tied payloads that are equal in all of these count as copies of one payload, as two equal `int` values do, so the list shows that payload twice.

Some ties can still throw an `ArgumentException` ("At least one object must implement IComparable"). The comparison does not look inside a member, so it can throw when two tied payloads are equal up to a member whose values cannot be compared, such as two different objects of a class with no comparison. It can also throw when the payload type has no public fields or properties to compare, such as `object` holding plain objects, or has a member that cannot be compared at all, such as a `ReadOnlySpan<char>` property.

**See also:** [Max](max.md), [Min (hopping window)](min.md), [TumblingWindowLifetime](tumbling-window.md), [Aggregate (several aggregates)](aggregate.md)
