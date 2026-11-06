# Explicit subscription loss handling

`AlloySubscriber::with_gap_policy(SubscriberGapPolicy::FailClosed)` opts into
stopping on canonical log or header notification loss. The bounded raw
subscription still detects actual channel overflow and undecodable payloads.
The subscriber revokes its coverage candidate, clears pending coverage controls,
records a sticky typed error and issues no repair requests. A repeated poll or
policy change cannot resume the damaged generation. Only an explicit full
registration reset clears the failure and establishes a new baseline.

The default remains `Reconcile`: anchored log gaps fetch the bounded missed
window, unanchored gaps fail, header gaps are reported, and speculative gaps
invalidate the preview. No existing default or source ordering changes.

This option alone is not a zero-RPC subscriber profile. Polling transports,
reconnect, log block verification, pending-transaction hydration, preview
sampling, owner reconciliation and lifecycle catch-up retain their separate
controls. A consumer requiring no ordinary-loop requests must also disable or
fence those paths and prove the whole composed transport boundary.

`planned_log_subscription_filters()` returns the current pure provider-filter
plan after exact owner fan-in and configured address splitting. It does not
subscribe, attest completeness, or add a cross-stream barrier. A later log on
one physical stream cannot close another. A later header or elapsed timer never
proves a bloom-positive log stream is complete. A single ordered, loss-detecting
stream can close a previous block when it delivers a later-block log; this can
wait indefinitely during quiet periods. Bloom absence remains a valid proof of
no matching events for the specific block.

Focused regressions drive real broadcast overflow and decode failure through
the normal subscriber polling entry, counting attempted transport elements.
They check zero requests, revoked coverage, sticky repeated rejection, explicit
reset and preserved default recovery. These are deterministic local tests, not
live-provider delivery or latency qualification.
