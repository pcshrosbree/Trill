# Stitch

Joins an event that ends to an event with an equal payload that starts at the same moment, which reverses Chop.

<div class="trill-marbles" data-op="stitch"></div>

> **Columnar mode differs:** in columnar mode this query throws `InvalidOperationException` while it is built, because `Ev` is a class with default equality. Chop refuses it first, and Stitch on its own refuses it too. Row mode, which the diagram uses, gives the events shown. (#164)

`Stitch()` looks for an event's end and the start of an event with an equal payload at the same time. It removes both, so the two events become one.

The diagram first [chops](chop.md) the input at every multiple of 3, then stitches the pieces. Each event comes back with its original lifetime: `a:1 [1,5)`, `b:2 [3,9)` and `a:3 [6,8)`.

The events come back as a **start edge and an end edge**. When an event starts, Stitch cannot yet know whether another piece will continue it. See [reading the diagrams](../introduction.md#how-trill-emits-an-event-intervals-and-edges).

**See also:** [Chop](chop.md)
