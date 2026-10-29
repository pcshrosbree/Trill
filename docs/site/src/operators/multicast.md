# Multicast

Lets one query use the same input stream more than once.

<div class="trill-marbles" data-op="multicast"></div>

`Multicast(s => ...)` passes the input to the function once, as `s`, so the function can build several branches from it without subscribing to the source twice.

In the diagram, one branch keeps key `a` and the other keeps values above 2, and the two are unioned. `a:3` passes both filters, so it appears **twice** in the output: `Union` does not deduplicate.

**See also:** [Union](union.md), [Where](where.md)
