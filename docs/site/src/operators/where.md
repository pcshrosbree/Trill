# Where

Keeps the events whose payload satisfies a predicate.

<div class="trill-marbles" data-op="where"></div>

Lifetimes pass through unchanged: an event either appears in the output with its own lifetime, or not at all.

In the diagram, `a:1` fails `Value > 1` and is dropped; `b:2` and `a:3` keep their lifetimes `[3,9)` and `[6,8)`.

**See also:** [Select](select.md), [Distinct](distinct.md), [SelectMany](select-many.md), [WhereNotExists](where-not-exists.md)
