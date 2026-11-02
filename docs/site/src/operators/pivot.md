# Pivot

Turns the events alive at each moment into one payload per group, with one field per attribute.

<div class="trill-marbles" data-op="pivot"></div>

`Pivot` takes five arguments:

- a constructor for the output type, such as `() => new Totals()`;
- a key selector, such as `e => e.Key`, which groups the events and fills the output field or property of the same name;
- an attribute selector, which returns the name of the output field that the event's value goes to;
- a value selector;
- an aggregate over the values, such as `w => w.Sum(v => v)`.

For each group, at each moment, every output field or property that can hold the aggregate's result receives the aggregate of the values of that attribute's live events. A field with no live events receives the aggregate of nothing, which for `Sum` is 0. The key selector must name fields or properties of the input, such as `e => e.Key`, and the output type needs fields or properties with the same names. If the key's type can also hold the aggregate's result, Pivot fills the key as an attribute field too, which overwrites it: an `int` key with `Sum` comes out as 0 when no event's attribute is the key's name. A different type is not enough, because an `object` key can also hold an `int`. Give the key a type that cannot hold the aggregate's result, as the diagram's `string` key does.

In the diagram, `Totals` has the fields `Key`, `Odd` and `Even`. The attribute is `"Odd"` or `"Even"` by the value's parity. The two `a` events are summed into separate fields: `a` is `odd 1, even 0` until `a:2` starts at 3, then `odd 1, even 2`. The group `b` has only an odd value. As with other aggregates, the output changes whenever the set of live events in a group changes, and it arrives as start and end edges.

**See also:** [Unpivot](unpivot.md), [GroupApply](group-apply.md), [Sum (hopping window)](sum.md)
