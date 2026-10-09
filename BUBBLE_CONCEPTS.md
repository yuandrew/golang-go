# Bubble concept inventory

Working inventory of proposed additions, not features accepted into Go.
Quinn means baseline `8dea6502fad43cc3ae715806fcb93e575027c8d0`;
our additions are the changes after that baseline on this branch.

## Existing Go mechanisms we reuse

Native `go`, channels, `select`, timers, defer/recover, GC and goroutine teardown
already exist. Testing/synctest already has execution bubbles, ownership and fake
time. We change how these mechanisms operate inside an explicitly selected
execution mode; we do not introduce new syntax or replace the ordinary scheduler.

## New execution contract — introduced by Quinn

- **Host-controlled lifetime:** `New` creates paused execution; `Step` runs it to
  quiescence or completion. Returning the initial function does not complete the
  bubble until its descendants exit. [API](src/runtime/bubble/bubble.go).
- **Reproducible cooperative scheduling:** one owned user goroutine runs at a
  time; supported blocking, yield and exit advance logical turns. Physical
  preemption does not select the next turn. Ready select choices use a seeded
  stream. [Scheduler](src/runtime/bubble.go), [select](src/runtime/select.go).
- **Ordered host delivery:** `Deliver` stages inputs before user code resumes.
  The controller temporarily joins the bubble; the callback must not block or
  yield. This is an input boundary, not a transaction. [API](src/runtime/bubble/bubble.go).
- **Host-controlled timers:** logical time comes from the host; advancing it does
  not authorize positive timers. Firings carry IDs/generations; Stop/Reset and
  obsolete-event validation preserve native timer behavior.
  [Timers](src/runtime/bubble_time.go).
- **Owned application randomness:** package-level math/rand and math/rand/v2
  draws use configured owned state, separate from select choices. Explicit
  generators keep their own sources. [Randomness](src/runtime/bubble_rand.go).

## New lifecycle contract — our additions

- **Terminal panic reporting:** after ordinary defer/recover handling, an
  unrecovered user panic pauses peers and returns a fault to the host. Blocked
  defers can delay reporting; runtime-fatal errors remain process-fatal.
  [Panic path](src/runtime/panic.go), [fault scheduling](src/runtime/bubble.go).
- **Paused discard:** `Close` can detach supported waits and exit remaining
  goroutines without user defers. It validates all owned states before mutation.
  Inspection/detachment pauses the whole process, including unrelated work;
  rejection can also incur that pause. No maximum duration is guaranteed.
  [Disposal](src/runtime/bubble.go), [Close documentation](src/runtime/bubble/bubble.go).
- **Explicit retirement acknowledgment:** the host acknowledges inactive timer
  ID ranges, allowing generation details to be removed. Acknowledged IDs stay
  obsolete; Reset gets a fresh ID. Acknowledgment order must replay. Fragmented
  ranges can still grow. [Retirement](src/runtime/bubble_time.go).

## New private implementation policies — our additions

These affect correctness or cost; they are not new public primitives.

- **Release inactive timer references:** Stop/authorization clears strong timer
  references while preserving stale-event validation. Objects become collectible
  only when no other references remain. [Timer cleanup](src/runtime/bubble_time.go).
- **Compact timer bookkeeping:** keep active registrations, sparse generation
  exceptions and merged acknowledgment ranges instead of full obsolete timer
  objects. Compaction can occur during execution. [Tables](src/runtime/bubble_time.go).
- **Shrink timer capacity at boundaries:** consider resizing when collecting
  timer changes, normally at the end of Step. That boundary already existed;
  deferring this resize to it is our optimization. There is no fixed event count
  or time window defining a batch. Current tuning shrinks at one-quarter capacity
  and allocates twice the remaining length. These constants are implementation
  choices, not API promises. [deterministicTimerChanges](src/runtime/bubble_time.go).
- **GC-safe owned channel headers:** GC scans the owner pointer even for
  pointer-free channel values. This preserves ownership identity; some owned
  buffered channels need an extra allocation. [Channel allocation](src/runtime/chan.go).
- **Ordinary-path refinements:** skip redundant owner checks, move unsupported
  wait checks onto the opted-in path, pack timer fields and streamline ordinary
  random draws. These reduce specific costs, not all overhead. The random refill
  implementation must track upstream changes. [Channels](src/runtime/chan.go),
  [waits](src/runtime/proc.go), [layout](src/runtime/time.go),
  [random draws](src/runtime/bubble_rand.go).

## Separate or deferred work

- **Temporal/Core replay:** implemented in separate research adapters. History
  mapping, activities, signals and durable reconstruction are not Go primitives.
- **Callback timers:** implemented/tested source remains here, but AfterFunc is
  excluded from the initial support promise.
- **Context:** earlier prototypes were removed from this candidate.
- **WaitGroup, general locks, tickers and deterministic map traversal:** deferred;
  investigations do not imply support. Command-sensitive map logic sorts keys.
- **Future-version replay, heap bounds and disposal latency bounds:** unproven;
  these are open requirements, not newly provided guarantees.

When adding a concept, record its origin, whether it changes the public contract
or private implementation, its source and its restriction. A passing test alone
does not establish a new feature.
