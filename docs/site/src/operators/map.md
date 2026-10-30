# Map

Starts a map-reduce: runs a query on the input, ready for a `Reduce`.

<div class="trill-marbles" data-op="map"></div>

`Map` and [Reduce](reduce.md) are Trill's map-reduce operators. `Map` does not return a stream. It returns a map definition, and a `Reduce` always follows it.

- `Map(mapper).Reduce()` runs the mapper and merges its output back into one stream.
- `Map(mapper, keySelector)` also groups the mapper's output by the key, so that the `Reduce` after it can run a query on each group.

In the diagram, the mapper is a `Where`, so the result is the same as [Where](where.md)'s.

**See also:** [Reduce](reduce.md), [GroupApply](group-apply.md)
