# Reduce

Runs a query on each group that a `Map` formed, and merges the results.

<div class="trill-marbles" data-op="reduce"></div>

`Reduce(reducer, resultSelector)` follows a `Map(mapper, keySelector)`. It runs the reducer on each group's stream, then passes the group and each result to the result selector. The selector's first argument holds the group's key as `Key`.

In the diagram, the mapper passes the input through, the key is `Key`, and the reducer counts the events alive in each group at each moment. The count for `a` is 1, then 2 while both `a` events are alive, then 1 again. The count for `b` is 1. The output arrives as start and end edges.

`GroupApply(keySelector, apply, resultSelector)` is built on these operators: it is `Map(keySelector)` followed by `Reduce(apply, resultSelector)`, with a mapper that passes the input through. `Reduce()` with no arguments passes the mapper's output through and merges it back into one stream, as on the [Map](map.md) page. After a `Map` with a key selector it does the same. Another overload reduces the groups of two `Map`s with the same key type together.

**See also:** [Map](map.md), [GroupApply](group-apply.md), [Count (tumbling window)](count.md)
