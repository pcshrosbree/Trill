# Reading the diagrams

This is an operator reference for [this fork of Trill](https://github.com/pcshrosbree/Trill), in the style of the [ReactiveX operator pages](https://reactivex.io/documentation/operators.html), with interactive diagrams in the manner of [RxMarbles](https://rxmarbles.com/).

Every diagram is computed by Trill itself. The page first draws a recorded run of the query, and then loads Trill into your browser (row mode, under .NET WebAssembly). From then on, dragging an input event re-runs the query and redraws the output. The `docs/site/DiagramTests` project checks every recorded run against the engine, and a browser check requires each page's live run to match its recording. Those checks cover the data a diagram draws; the drawing itself is checked only for clipping, so report anything that looks wrong.

<div class="trill-marbles" data-op="join"></div>

## Events have lifetimes

A Reactive Extensions marble is a point in time. A Trill event is not: it has a **lifetime**, a half-open interval `[start, end)` of application time, and the event is "alive" for that whole interval. Operators such as `Join`, `Union` and the windows work on how lifetimes overlap, so the diagrams draw events as bars from start to end, except for one-tick inputs, which are circles.

- **Bar:** an event with lifetime `[start, end)`. Its label is `key:value`.
- **Circle:** an input event whose lifetime is a single tick, `[t, t+1)`. Trill's `StreamEvent.CreatePoint` makes these.
- **Colour:** the event's key. Joins, grouping and patterns match on keys, so colour shows which events can meet. A legend under each diagram names the keys, and every bar carries its key in its label too.

Hover over any event to see its exact lifetime.

## How Trill emits an event: intervals and edges

Trill can deliver an output event in two ways, and the diagram shows which one it used:

- **Interval (rounded bar):** the whole lifetime arrives in one message.
- **Start edge and end edge (square bar with black ticks):** Trill first emits a *start edge*, an event whose end is not known yet, and later an *end edge* that closes it. The diagram pairs the two into one bar, with a tick at each end. A start edge with no end edge yet is drawn running off the right with `→∞`.

The logical lifetime is the same either way. The difference matters to code that consumes Trill's output through `ToStreamEventObservable`, which sees the edges as separate messages. Operators that cannot know an output's end when its start is decided, such as `ClipEventDuration`, `LeftOuterJoin` and `WhereNotExists`, emit edges.

## Interacting

- **Drag** an input event left or right to move it.
- **Drag a bar's right end** to make it longer or shorter. Circles (one-tick inputs) can only be moved.
- The axis stays fixed while you drag, so the event stays under the pointer. If an output then lies past the right of the axis, the axis widens to show it when you let go.
- The status line under the diagram says whether you are looking at the recorded run or live Trill, and how long the last run took.

The inputs use small integer times so that the diagrams stay readable. Trill's times are `long` ticks, and nothing here depends on their unit.
