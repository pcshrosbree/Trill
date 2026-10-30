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

## Lifetimes and windows

| Operator | What it does |
|---|---|
| [AlterEventDuration](operators/alter-event-duration.md) | Gives every event a new duration, keeping its start time. |
| [ClipEventDuration](operators/clip-event-duration.md) | Ends each left event when a right event with the same key starts. |
| [TumblingWindowLifetime](operators/tumbling-window.md) | Moves each event into a fixed, non-overlapping window, ready for an aggregate. |
| [HoppingWindowLifetime](operators/hopping-window.md) | Moves each event into overlapping windows of a fixed size that start every period. |

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

## Patterns

| Operator | What it does |
|---|---|
| [Detect](operators/detect.md) | Finds sequences of events that match a pattern within a maximum duration. |
