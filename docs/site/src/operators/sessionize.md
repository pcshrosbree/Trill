# Sessionize

Pairs each session's start event with its end event, per session key, and emits one result when the session ends.

<div class="trill-marbles" data-op="sessionize"></div>

`Sessionize(isStart, isEnd, sessionKey, result)` takes two predicates that mark the events that start and end a session, a function that gives each event's session key, and a function of the start and end events that builds the result. The result is a one-tick event at the time of the end event.

In the diagram, a value of 2 or less starts a session, a value of 4 or more ends one, and the key is the session key. For key `b`, `b:2` at 2 starts a session and `b:4` at 7 ends it, so the result `2 to 4` appears at 7. For key `a`, `a:1` at 1 and `a:5` at 9 give `1 to 5` at 9. `a:3` neither starts nor ends a session, so it adds nothing.

The results are emitted as a **start edge and an end edge**. See [reading the diagrams](../introduction.md#how-trill-emits-an-event-intervals-and-edges).

**See also:** [SessionTimeoutWindow](session-timeout-window.md), [ClipEventDuration](clip-event-duration.md), [GroupApply](group-apply.md)
