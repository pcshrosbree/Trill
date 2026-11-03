# SessionTimeoutWindow

Groups events into sessions that end after a quiet period, and makes every event of a session last until the session ends.

<div class="trill-marbles" data-op="session-timeout-window"></div>

`SessionTimeoutWindow(timeout)` starts a session with the first event. The session continues as long as each event starts less than `timeout` after the previous one, and it ends `timeout` after the start of its last event. Every event of the session then lasts from its own start to the end of the session. An event's own end is ignored: a long event is cut short and a short one is extended.

In the diagram the timeout is 3. The events at 1, 2 and 4 form one session, which ends at 4 + 3 = 7, so all three end at 7. The event at 7 starts exactly when that session ends, so it begins a new one. With the event at 9 that session ends at 12.

The stream has no grouping key here, so `a` and `b` share their sessions. Inside `GroupApply`, each key has its own sessions: `a` and `b` would then end at 4, 5, 7, 10 and 12.

Each event gets the same lifetime in every mode, but not the same kind of output event. On a stream with no grouping key, row mode, which the diagram uses, emits each event as one interval event. Columnar mode, and both modes inside `GroupApply`, emit each event as a start edge and a later end edge.

The optional second argument, `maxDuration`, cuts long sessions at a multiple of `maxDuration`. A session ends at its timeout or at its cut point, whichever comes first. A session's first event, at time `t`, sets its cut point:

- If the session begins after a timeout, as the stream's first session does, the cut point is the first multiple of `maxDuration` that is at least `maxDuration` after `t`. This holds even when the session's first event is what ends the previous session at its timeout.
- An event at or after the cut point that starts less than `timeout` after the previous event continues the session in a new one. That session's cut point is the first multiple of `maxDuration` after `t`, and it ends at its own timeout or cut point.

Punctuations do not change where a session is cut.

For example, take a timeout of 3 and a `maxDuration` of 4. With events at every tick from 1 to 9, the first session's cut point is 8, the first multiple of 4 at least 4 after 1, so the events from 1 to 7 end at 8. The event at 8 continues the session in a new one, whose cut point is 12, so 8 and 9 end at 12. With events from 0 to 8, the cuts fall at 4 and 8, and the event at 8 alone ends at its timeout, 11. With events from 1 to 7 and then at 9, 10 and 11, the events from 1 to 7 again end at 8. The event at 9 continues the session with the cut point 12, so 9, 10 and 11 end at 12, although their timeout would end them at 14. With events at 1, 2 and 4, at 7, and at every tick from 9 to 15, the event at 7 ends the first session at its timeout, 7, and starts the next one, whose cut point is 12. The events from 7 to 11 end at 12, and the events from 12 to 15 end at 16.

The timeout must be positive, and `maxDuration` must be greater than the timeout. A `maxDuration` of 0, the default, sets no limit, and so does a negative one: either stands for `StreamEvent.InfinitySyncTime`, so the timeout must then be less than that. `SessionTimeoutWindow` throws an `ArgumentException` naming the parameter when it is called with any other values, before the query runs.

**See also:** [TumblingWindowLifetime](tumbling-window.md), [HoppingWindowLifetime](hopping-window.md), [AlterEventDuration](alter-event-duration.md), [GroupApply](group-apply.md)
