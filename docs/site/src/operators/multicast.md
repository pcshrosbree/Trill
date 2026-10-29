# Multicast

Lets one query use the same input stream more than once.

<div class="trill-marbles" data-op="multicast"></div>

`Multicast(s => ...)` gives the function a shared stream `s`. Every branch the function builds from `s` shares one subscription to the source, so the source is read once however many branches use it.

In the diagram, one branch keeps key `a` and the other keeps values above 2, and the two are unioned. `a:3` passes both filters, so it appears **twice** in the output: `Union` does not deduplicate.

**See also:** [Union](union.md), [Where](where.md)
