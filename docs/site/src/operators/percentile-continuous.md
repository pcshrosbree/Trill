# PercentileContinuous (tumbling window)

Computes a percentile of a value over the events alive at each moment, interpolating between values.

<div class="trill-marbles" data-op="percentile-continuous"></div>

The stream has a shorthand for each percentile: `PercentileContinuous(percentile, selector)` is the same as `Aggregate(w => w.PercentileContinuous(percentile, selector))`, and likewise for `PercentileDiscrete`. To compute several aggregates together, as the diagram does with both percentiles, use them inside [`Aggregate`](aggregate.md). The percentile is a number from 0 to 1, such as 0.5 for the median. The selector returns a `double`. A percentile outside 0 to 1 throws an `ArgumentException` when the query is built.

There are two definitions. For `n` values in increasing order:

- `PercentileContinuous(p, ...)` takes the position `p × (n − 1)`, counting from 0, and interpolates between the two values either side of it. The result need not be one of the values.
- `PercentileDiscrete(p, ...)` returns the value at position `ceil(p × n)`, counting from 1, or the first value when `p` is 0: the smallest value with at least a fraction `p` of the values at or below it, up to the tolerance described next. The result is always one of the values. When `p × n` would be a whole number without rounding, rounding error does not move the position, for any position up to 2^31 − 1: a product no more than 2^-51, relative, above a whole number counts as that whole number. So `0.07 × 100`, which comes out of `double` arithmetic as 7.000000000000001, gives the 7th of 100 values. The tolerance cannot tell rounding error from a percentile set that close on purpose: `0.5 + 2^-52` over two values also gives the 1st.

The diagram computes both medians. `TumblingWindowLifetime(5)` puts 1, 2 and 3 in `[5,10)`, and both medians are 2. In `[10,15)` the values are 4 and 5: the continuous median is 4.5, halfway between them, and the discrete median is 4. At 0.25 the results would be 1.5 and 1 in the first window, and 4.25 and 4 in the second.

A comparison as the first argument changes the order: `PercentileContinuous((x, y) => y.CompareTo(x), 0.25, e => e.Value)` counts from the largest value, which gives the same result as 0.75 in the usual order.

At 0, both percentiles give the smallest value, as `Min` does.

**See also:** [Aggregate (several aggregates)](aggregate.md), [Average (tumbling window)](average.md), [Min (hopping window)](min.md), [TumblingWindowLifetime](tumbling-window.md)
