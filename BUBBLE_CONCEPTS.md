# Bubble implementation guide

The [package documentation](src/runtime/bubble/bubble.go) defines API behavior,
supported operations and ownership rules. This guide maps private implementation
choices to their code; these choices are not compatibility guarantees.

## Implementation

- **Shared machinery:** bubble ownership and logical time build on
  [synctest](src/runtime/synctest.go). The
  [scheduler](src/runtime/bubble.go) adds a per-bubble runnable queue;
  [select](src/runtime/select.go) uses a seeded stream separate from application
  randomness.
- **Goroutine tracking:** a per-bubble member list avoids a process-wide disposal
  scan. A small index accelerates removal, with a search fallback for larger
  executions. See [membership and disposal](src/runtime/bubble.go) and
  [spawn and exit](src/runtime/proc.go).
- **Channel allocation:** owned channel headers are scanned by GC so the owner
  pointer stays reachable even when elements contain no pointers. Buffered owned
  channels can require an additional allocation. See [channels](src/runtime/chan.go).
- **WaitGroup cleanup:** discard builds a temporary waiter index before taking
  shared semaphore locks. It uses storage proportional to outstanding waits and
  does not change ordinary WaitGroup operations.
  See [WaitGroup cleanup](src/runtime/bubble_waitgroup.go).
- **Stack coordination:** disposal claims paused stacks and records pending
  profiles before rewriting saved frames. Fault delivery waits for runtime
  teardown to finish. See [disposal](src/runtime/bubble.go) and
  [activity release](src/runtime/synctest.go); panic unwinding is handled in
  [panic.go](src/runtime/panic.go).
- **Timer storage:** host registrations hold weak references to channel timers
  and tickers; Sleep and callback timers remain strongly retained until firing
  or cancellation. Scalar registrations, sparse generation records and merged
  acknowledgment ranges preserve stale-event checks. Compaction can run during
  execution; capacity shrinking is considered at Step boundaries.
  See [timer bookkeeping](src/runtime/bubble_time.go).
- **Ordinary execution paths:** ownership checks, timer layout and random-source
  dispatch also affect ordinary Go. The duplicated random refill loop must stay
  aligned with runtime.rand. See [channels](src/runtime/chan.go),
  [timers](src/runtime/time.go) and [randomness](src/runtime/bubble_rand.go).

## Known limitations

- A blocked user defer can delay fault reporting.
- WaitGroup disposal still has quadratic worst-case work. Validating and removing
  a large queue can delay unrelated operations sharing its semaphore lock.
- Collecting an unreferenced native timer does not remove its host registration.
  Registrations remain until authorization, cancellation or disposal. Fragmented
  acknowledgment ranges can also grow.
- Bounded total heap use, bounded discard latency and replay compatibility across
  arbitrary future Go versions are not established. Passing tests cover only the
  tested programs and configurations.
