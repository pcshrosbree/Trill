# SessionTimeoutWindow

Groups events into sessions that end after a quiet period, and makes every event of a session last until the session ends.

<div class="trill-marbles" data-op="session-timeout-window"></div>

`SessionTimeoutWindow(timeout)` starts a session with the first event. The session continues as long as each event starts less than `timeout` after the previous one, and it ends `timeout` after the start of its last event. Every event of the session then lasts from its own start to the end of the session. An event's own end is ignored: a long event is cut short and a short one is extended.

In the diagram the timeout is 3. The events at 1, 2 and 4 form one session, which ends at 4 + 3 = 7, so all three end at 7. The event at 7 starts exactly when that session ends, so it begins a new one. With the event at 9 that session ends at 12.

The stream has no grouping key here, so `a` and `b` share their sessions. Inside `GroupApply`, each key has its own sessions: `a` and `b` would then end at 4, 5, 7, 10 and 12.

The optional second argument, `maxDuration`, cuts long sessions at a multiple of `maxDuration`. A session ends at its timeout or at its cut point, whichever comes first. One event of the session, at time `t`, sets the cut point:

- The stream's first session takes it from its first event: the first multiple of `maxDuration` that is at least `maxDuration` after `t`.
- A later session takes it from its second event when its first event is what ends the previous session, at its timeout or its cut point. That is the case when nothing else, such as a punctuation, has moved time to the previous session's end before the first event arrives. When something has, the first event can set the cut point instead; but in row mode, on a stream with no grouping key, a session that begins after a timeout still takes it from its second event.
- If the session began after a timeout, the cut point is the first multiple of `maxDuration` that is at least `maxDuration` after `t`, as for the first session. If it continues a session that was cut, the cut point is the first multiple of `maxDuration` after `t`.

An event at or after the cut point that starts less than `timeout` after the previous event continues the session in a new one, which ends at its own timeout or cut point.

For example, take a timeout of 3 and a `maxDuration` of 4, with no punctuations in the input. With events at every tick from 1 to 9, the first session's cut point is 8, the first multiple of 4 at least 4 after 1, so the events from 1 to 7 end at 8. The event at 8 continues the session, and the event at 9 sets the next cut point, 12, so 8 and 9 end at 12. With events from 0 to 8, the cuts fall at 4 and 8, and the event at 8 alone ends at its timeout, 11. With events from 1 to 7 and then at 9, 10 and 11, the events from 1 to 7 again end at 8. The event at 9 continues the session, and the event at 10 sets the cut point to 12, so 9, 10 and 11 end at 12, although their timeout would end them at 14.

**Known issues in this fork.** A timeout of 0 is not refused. In row mode with no grouping key, as in the diagram, the output is then empty. Inside `GroupApply`, and in columnar mode, every event comes out, but each one ends as soon as it starts.

Because a later session can take its cut point from its second event rather than its first, it can last more than twice `maxDuration`. With a timeout of 3, a `maxDuration` of 4 and events at 1, 2 and 4, at 7, and at every tick from 9 to 15, the event at 7 ends the first session, so the session that starts at 7 takes its cut point from 9. That is 16, so the session lasts 9 ticks, until its cut point rather than its timeout at 18. With its second event at 8 instead, its cut point is 12. With a punctuation at 7 just before the event at 7, the same session takes its cut point from 7 and ends at 12, inside `GroupApply` or in columnar mode; in row mode on a stream with no grouping key it still ends at 16.

The causes are in the engine, and the fixes belong there.

**See also:** [TumblingWindowLifetime](tumbling-window.md), [HoppingWindowLifetime](hopping-window.md), [AlterEventDuration](alter-event-duration.md), [GroupApply](group-apply.md)
