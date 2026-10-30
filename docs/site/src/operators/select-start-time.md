# Select (with start time)

Transforms each event's payload using its start time as well; lifetimes are unchanged.

<div class="trill-marbles" data-op="select-start-time"></div>

This overload of `Select` takes a selector of the form `(start, payload) => result`, where `start` is the event's start time, a `long`. Like the plain `Select`, it changes only the payload: each output event keeps its input's lifetime.

In the diagram, each label gains the time its event starts: `a:1` starts at 1 and becomes `1 at 1`. Drag an input event and its label follows its new start time.

`SelectMany` and `SelectByKey` have the same form, with the start time as the first parameter.

**See also:** [Select](select.md), [SelectMany](select-many.md), [SelectByKey](select-by-key.md)
