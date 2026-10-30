# Select

Transforms each event's payload; lifetimes are unchanged.

<div class="trill-marbles" data-op="select"></div>

Each input event produces exactly one output event with the same lifetime and a new payload.

In the diagram every value is multiplied by ten. Move an input event and its output moves with it.

Three more overloads build a payload of a new type from the old one without listing every field. You pass the new type's constructor, such as `() => new Reading()`, and then either one or two pairs of a field of the new type and an expression for its value, or a dictionary from field name to expression. Public fields and properties with the same name in both types are copied, and the named fields are computed.

**See also:** [Where](where.md), [Select (with start time)](select-start-time.md), [SelectMany](select-many.md), [SelectByKey](select-by-key.md)
