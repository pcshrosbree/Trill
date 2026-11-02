# Operators by category

Each operator has a page with an interactive diagram computed by Trill. It covers filtering and projection, joins, pattern detection, windows and aggregates, and the operators they build on.

## Filtering and projection

| Operator | What it does |
|---|---|
| [Where](operators/where.md) | Keeps the events whose payload satisfies a predicate. |
| [Select](operators/select.md) | Transforms each event's payload; lifetimes are unchanged. |
| [Select (with start time)](operators/select-start-time.md) | Transforms each event's payload using its start time as well. |
| [SelectMany](operators/select-many.md) | Turns each event into zero or more events with the same lifetime. |
| [SelectByKey](operators/select-by-key.md) | Transforms each event's payload using the stream's key as well. |
| [Distinct](operators/distinct.md) | Removes duplicates among the events alive at each moment. |
| [SelectManyByKey](operators/select-many-by-key.md) | Turns each event into zero or more events using the stream's key as well. |
| [Pivot](operators/pivot.md) | Turns the events alive at each moment into one payload per group, with one field per attribute. |
| [Unpivot](operators/unpivot.md) | Turns each event into one event per value field. |
| [Map](operators/map.md) | Starts a map-reduce: runs a query on the input, ready for a Reduce. |
| [Reduce](operators/reduce.md) | Runs a query on each group that a Map formed, and merges the results. |

## Lifetimes and windows

| Operator | What it does |
|---|---|
| [AlterEventDuration](operators/alter-event-duration.md) | Gives every event a new duration, keeping its start time. |
| [ClipEventDuration](operators/clip-event-duration.md) | Ends each left event when a right event with the same key starts. |
| [TumblingWindowLifetime](operators/tumbling-window.md) | Moves each event into a fixed, non-overlapping window, ready for an aggregate. |
| [HoppingWindowLifetime](operators/hopping-window.md) | Moves each event into overlapping windows of a fixed size that start every period. |
| [SessionTimeoutWindow](operators/session-timeout-window.md) | Groups events into sessions that end after a quiet period, and makes every event of a session last until the session ends. |
| [PointAtEnd](operators/point-at-end.md) | Replaces each event with a one-tick event at its end. |

## Combining streams

| Operator | What it does |
|---|---|
| [Union](operators/union.md) | Merges two streams of the same type into one. |
| [Multicast](operators/multicast.md) | Lets one query use the same input stream more than once. |

## Joins

| Operator | What it does |
|---|---|
| [Join](operators/join.md) | Pairs left and right events with equal keys whose lifetimes overlap; each pair lives for the overlap. |
| [LeftOuterJoin](operators/left-outer-join.md) | Like Join, but a left event also appears, through a separate selector, for the times when nothing matches it. |
| [WhereNotExists](operators/where-not-exists.md) | Keeps each left event only for the times when no right event with the same key is alive. |

## Aggregates

| Operator | What it does |
|---|---|
| [Count (tumbling window)](operators/count.md) | Counts the events alive at each moment; after a tumbling window, that is a count per window. |
| [Sum (hopping window)](operators/sum.md) | Sums a value over the events alive at each moment; after a hopping window, that is a sum per hop. |
| [GroupApply](operators/group-apply.md) | Splits a stream by key, runs the same query on each group, and merges the results. |
| [GroupAggregate](operators/group-aggregate.md) | Splits a stream by key and computes one or more aggregates per group, in one call. |
| [Aggregate (several aggregates)](operators/aggregate.md) | Computes several aggregates over the events alive at each moment and combines their results into one payload. |
| [Average (tumbling window)](operators/average.md) | Averages a value over the events alive at each moment; after a tumbling window, that is an average per window. |
| [Min (hopping window)](operators/min.md) | Finds the smallest value among the events alive at each moment; after a hopping window, that is the minimum per hop. |
| [Max](operators/max.md) | Finds the largest value among the events alive at each moment. |
| [TopK (tumbling window)](operators/top-k.md) | Lists the k events with the largest values among the events alive at each moment, with their ranks. |
| [PercentileContinuous (tumbling window)](operators/percentile-continuous.md) | Computes a percentile of a value over the events alive at each moment, interpolating between values. |

## Patterns

| Operator | What it does |
|---|---|
| [Detect](operators/detect.md) | Finds sequences of events that match a pattern within a maximum duration. |
| [ConsecutivePairs](operators/consecutive-pairs.md) | Pairs each event with the event before it. |
