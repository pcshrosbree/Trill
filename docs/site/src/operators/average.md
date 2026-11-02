# Average (tumbling window)

Averages a value over the events alive at each moment; after a tumbling window, that is an average per window.

<div class="trill-marbles" data-op="average"></div>

`Average(selector)` averages the selected value over the events alive at each moment. The average of `int` values is a `double`, so it can have a fractional part.

In the diagram, `TumblingWindowLifetime(5)` puts the values 1, 2 and 3 in `[5,10)`, whose average is 2, and the values 4 and 5 in `[10,15)`, whose average is 4.5.

A window with no events in it gives no output. With a selector that returns a nullable value, such as `int?`, the nulls are left out of the average, and a window whose values are all null gives null.

**See also:** [Sum (hopping window)](sum.md), [Count (tumbling window)](count.md), [Aggregate (several aggregates)](aggregate.md), [GroupApply](group-apply.md)
