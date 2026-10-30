# Pivot

Turns the events alive at each moment into one payload per group, with one field per attribute.

<div class="trill-marbles" data-op="pivot"></div>

`Pivot` takes five arguments:

- a constructor for the output type, such as `() => new Totals()`;
- a key selector, such as `e => e.Key`, which groups the events and fills the output field of the same name;
- an attribute selector, which returns the name of the output field that the event's value goes to;
- a value selector;
- an aggregate over the values, such as `w => w.Sum(v => v)`.

For each group, at each moment, every output field that can hold the aggregate's result receives the aggregate of the values of that attribute's live events. A field with no live events receives the aggregate of nothing, which for `Sum` is 0. The key selector must name fields of the input, such as `e => e.Key`, and the output type needs fields with the same names. If the key field's type can also hold the aggregate's result, Pivot fills that field as an attribute field too, which overwrites the key: an `int` key with `Sum` comes out as 0 when no event's attribute is the key field's name. Give the key a different type from the aggregate's result, as the diagram's `string` key has.

In the diagram, `Totals` has the fields `Key`, `Odd` and `Even`. The attribute is `"Odd"` or `"Even"` by the value's parity. The two `a` events are summed into separate fields: `a` is `odd 1, even 0` until `a:2` starts at 3, then `odd 1, even 2`. The group `b` has only an odd value. As with other aggregates, the output changes whenever the set of live events in a group changes, and it arrives as start and end edges.

**Known issue in this fork.** Suppose events arrive as intervals, and one ends while another event of its group is still alive. The group's result after that end can be wrong: the engine removes the ended event from the group's aggregate state incorrectly. Whether the result is wrong, and by how much, depends on the aggregate, the values, and any event of the group that starts at the same moment. For example, take only `a:1 [1,6)` and `a:3 [3,8)`. With `Sum`, `Pivot` reports `odd 0` from 6 to 8, where the right answer is `odd 3`. With `Average`, it reports `NaN` there, where the right answer is 3. The same events sent as separate start and end edges give the right answer. The cause is in the engine, and the fix belongs there. The diagram's `a` events end together, which avoids the problem. Until it is fixed, `GroupApply` with a `Where` per attribute and the aggregate gives the right values.

**See also:** [Unpivot](unpivot.md), [GroupApply](group-apply.md), [Sum (hopping window)](sum.md)
