# Aggregate (several aggregates)

Computes several aggregates over the events alive at each moment and combines their results into one payload.

<div class="trill-marbles" data-op="aggregate"></div>

`Aggregate(aggregate1, aggregate2, ..., resultSelector)` takes from two to fifteen aggregate arguments and a result selector. Each aggregate argument picks an aggregate from a window `w`, such as `w => w.Min(e => e.Value)` or `w => w.Count()`. The result selector receives the results in the order the aggregates are given. All of them are computed over the same events, so the results always describe the same moment.

With a single aggregate, `Aggregate(w => w.Average(e => e.Value))` gives the same output as `Average(e => e.Value)`; the stream's own `Average`, `Sum` and similar operators are shorthands for it. Some aggregates exist only on the window, such as `StandardDeviation`, so `Aggregate` is how they are used.

In the diagram, `TumblingWindowLifetime(5)` puts the events at 1, 2 and 4 in `[5,10)`, where the values are 1, 2 and 3: the minimum is 1, the maximum 3 and the count 3. The window `[10,15)` holds 4 and 5, so the minimum is 4, the maximum 5 and the count 2.

**See also:** [GroupAggregate](group-aggregate.md), [Min (hopping window)](min.md), [Max](max.md), [Count (tumbling window)](count.md), [PercentileContinuous (tumbling window)](percentile-continuous.md)
