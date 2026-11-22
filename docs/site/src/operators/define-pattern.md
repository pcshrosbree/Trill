# DefinePattern

Starts a pattern on a stream, with a default register, to run with `Detect`.

<div class="trill-marbles" data-op="define-pattern"></div>

`DefinePattern(defaultRegister)` starts a pattern. Steps such as `SingleElement` build it, one event each, and `.Detect(maxDuration)` runs it, as [Detect](detect.md) does. The register is a value that each partial match carries from step to step. It starts at `defaultRegister`, and a match's result is its register when the last step completes. A step's aggregator, such as `(e, r) => r + e.Value`, gives the register its new value; a step without one leaves the register unchanged.

In the diagram the register starts at 100, and the pattern is "an `a`, then a `b`, within 4", where each step adds its event's value. `a:1` and `b:2` give 103, from 100 + 1 + 2, and `a:3` and `b:4` give 107. Each match lives from its last event until its first event plus 4, as on the Detect page: `[2,5)` and `[7,8)`.

`source.Detect(100, p => ..., maxDuration: 4)` gives the same result, and so does `DefinePattern(100).Detect(p => ..., maxDuration: 4)`. With no argument, `DefinePattern()` has no register, and each match is reported as `Empty`, as with `Detect(p => ...)`. `DefinePattern(defaultRegister, defaultAccumulator)` also sets the accumulator with which each multi-element step starts.

Without `maxDuration`, `Detect` takes the duration from a stream that Trill knows has a constant event duration, such as the result of `AlterEventDuration(4)`. On any other stream it throws `InvalidOperationException`, even when its events happen to have the same length, as the diagram's do.

**See also:** [Detect](detect.md), [FollowedByImmediate](followed-by-immediate.md)
