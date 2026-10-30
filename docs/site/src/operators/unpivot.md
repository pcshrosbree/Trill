# Unpivot

Turns each event into one event per value field, each holding the key, the field's name and its value.

<div class="trill-marbles" data-op="unpivot"></div>

`Unpivot` is the reverse of [Pivot](pivot.md). It takes four arguments:

- a constructor for the output type, such as `() => new Triple()`;
- a key selector, such as `p => p.Key`, which copies the input's key field into the output field of the same name;
- an attribute selector, `t => t.Attribute`, naming the output field that receives the name of the input field;
- a value selector, `t => t.Value`, naming the output field that receives the input field's value.

With a value field of a non-nullable value type, such as the `int Value` here, each other input field of the matching nullable type (`int?`) becomes one output event with the input event's lifetime, unless it is null. A field of another type, such as `string`, is ignored. Other kinds of value field follow other rules, and some combinations fail: an `int` input field with an `object` value field throws when the query is built.

In the diagram, a `Select` first builds a `Pair` with the nullable fields `Low` and `High`. `Unpivot` then turns each `Pair` into two events: `a:1` becomes `High 10` and `Low 1`, both over `[1,5)`.

**Known issues in this fork.** With an `int` value field, an input field of type `int` is always skipped, although an `int?` field is unpivoted. Nullable properties are handled the wrong way round: an `int?` property with a value is skipped, and a null one throws an `InvalidOperationException`. The causes are in the engine, and the fixes belong there. The diagram uses nullable fields, which work.

**See also:** [Pivot](pivot.md), [SelectMany](select-many.md)
