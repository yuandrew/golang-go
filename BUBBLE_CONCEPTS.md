# Bubble execution contract and implementation

This inventory distinguishes the proposed execution contract from existing Go
mechanisms and private implementation choices. Implementation details are not
additional public primitives or compatibility guarantees.

## Existing mechanisms

Native goroutines, channels, select, timers, defer/recover, garbage collection
and goroutine teardown already exist. Testing/synctest provides bubble ownership
and a logical clock. The proposal reuses that machinery for opt-in, host-controlled
execution outside tests. It does not replace the ordinary scheduler or add syntax.

## Proposed execution contract

- **Host-controlled lifetime:** New creates paused execution. Step runs to
  quiescence, completion or a terminal fault. Returning the initial function does
  not complete the bubble until its descendants exit.
  Close releases execution only when no live goroutines remain.
  [API](src/runtime/bubble/bubble.go).
- **Cooperative scheduling:** one owned user goroutine runs at a time. Supported
  blocking, explicit yield and exit advance logical turns. Physical preemption
  does not choose the next turn. Ready select choices use a seeded stream.
  [Scheduler](src/runtime/bubble.go), [select](src/runtime/select.go).
- **Channel ownership:** inside a deterministic bubble, send, receive, close,
  select, len and cap reject non-nil foreign channels with a panic before the
  forbidden operation takes effect. User code may recover and continue; an
  escaping panic faults the bubble. Earlier expression effects are not rolled
  back. Host send, receive, close and select on an owned channel remain
  process-fatal.
  [Channel checks](src/runtime/bubble.go).
- **Ordered delivery:** Deliver applies host inputs before user goroutines
  resume. The controller temporarily joins the bubble. Its callback must not
  block or yield. Delivery is not a transaction; mutations are not rolled back.
  [Activation and Delivery](src/runtime/bubble/bubble.go).
- **Logical time and timers:** the host supplies time and authorizes positive
  timer firings. Advancing time alone is insufficient. IDs and generations
  distinguish current timers from canceled, reset or already delivered events.
  [Timer bookkeeping](src/runtime/bubble_time.go).
- **Native tickers:** NewTicker, Tick, Stop and Reset use the same authorization
  protocol. Each registration represents one send attempt. Direct handoff rearms
  the next phase; a pending value pauses registration until channel access.
  Slow receivers coalesce elapsed periods without changing a pending timestamp.
  Polling never authorizes a send. Stop leaves the channel open; Reset starts a
  new phase. No general callback-timer support is implied.
  [Ticker polling and delivery](src/runtime/bubble_time.go).
- **Application randomness:** package-level math/rand and math/rand/v2 share
  configured state owned by the bubble. That state is separate from select
  choices. Explicit generators retain their own sources.
  [Randomness](src/runtime/bubble_rand.go).
- **Terminal panic reporting:** normal defer/recover handling precedes fault
  reporting. Unrecovered user panics pause peers and return a fault to the host.
  A blocked defer can delay reporting; runtime-fatal errors remain process-fatal.
  [Panic path](src/runtime/panic.go), [fault scheduling](src/runtime/bubble.go).
- **Owned WaitGroups:** native Add, Done, Wait and Go participate in the owning
  bubble. Active use stays in one domain until all Wait calls return. Discard
  permanently invalidates groups with abandoned waits; completed groups remain
  reusable. Package-level values cannot be associated in this prototype.
  [WaitGroup](src/sync/waitgroup.go), [Cleanup](src/runtime/bubble_waitgroup.go).
- **Paused discard:** Discard validates all owned states before detaching supported
  waits and exiting goroutines without running user code or defers. It does not
  interrupt active code or perform application cancellation. Discard requests no
  process-wide disposal pause; ordinary runtime pauses and StackTrace's separate
  global pause remain possible. No maximum latency is guaranteed.
  [Disposal](src/runtime/bubble.go), [Discard](src/runtime/bubble/bubble.go).
- **Timer retirement acknowledgment:** the host acknowledges inactive ID ranges
  so detailed generation records can be removed. Acknowledged IDs remain obsolete;
  resetting or rearming a retained native timer then assigns a new ID.
  Acknowledgments must be reproduced during replay. Fragmented ranges can still grow.
  [Retirement](src/runtime/bubble_time.go).

## Private implementation choices

- **Live goroutine membership:** each bubble tracks its owned goroutines instead
  of scanning the process-wide list for disposal. A small index accelerates
  removal; larger executions use a search fallback without a concurrency cap.
  [Membership and disposal](src/runtime/bubble.go), [spawn and exit](src/runtime/proc.go).
- **WaitGroup preflight index:** discard builds a temporary waiter index before
  taking shared semaphore locks. Queue validation uses it instead of repeatedly
  searching the registry under those locks. The index adds temporary storage
  proportional to outstanding waits and does not change ordinary operations.
  [WaitGroup cleanup](src/runtime/bubble_waitgroup.go).
- **Stack and profiling coordination:** disposal acquires per-stack scan
  ownership and records pending profiles before rewriting saved frames. Fault
  delivery waits for logical execution and runtime teardown to finish.
  [Stack coordination](src/runtime/bubble.go), [activity release](src/runtime/synctest.go).
- **Channel timer references:** host registrations hold weak native payloads,
  allowing unreferenced channel timers and tickers to be collected. Scalar host
  registrations remain until authorization, cancellation or disposal; GC does
  not choose registration cleanup. Callback and Sleep timers remain strongly
  retained until firing or cancellation. Stop and authorization retire stale
  token state through the existing acknowledgment protocol.
  [Timer cleanup](src/runtime/bubble_time.go).
- **Timer storage:** active registrations, sparse generation exceptions and
  merged acknowledgment ranges replace full records for obsolete timers.
  Compaction can occur during execution. Capacity shrinking is considered while
  collecting timer changes at a Step boundary. Current thresholds and growth
  factors are implementation choices, not API promises.
  [Timer tables](src/runtime/bubble_time.go).
- **Scanned owned channel headers:** GC retains the owner pointer even when
  channel elements contain no pointers. This preserves ownership identity and
  can require an additional allocation for buffered owned channels.
  [Channel allocation](src/runtime/chan.go).
- **Ordinary execution paths:** ownership checks, timer layout and random-source
  dispatch affect code shared with ordinary Go. The random refill loop must stay
  consistent with the ordinary runtime source. These paths require separate cost
  and compatibility validation.
  [Channels](src/runtime/chan.go), [timers](src/runtime/time.go), [random draws](src/runtime/bubble_rand.go).

## Exclusions and remaining limits

External effects, persistence, history mapping and durable reconstruction belong
to the host. General context, locks and reproducible map
traversal are not covered by the initial replay contract. Callback timers have
compatibility code and tests, but are outside that contract.

WaitGroup disposal still has quadratic worst-case work in member lookup, group
validation and normal-return registry maintenance. Temporary indexing removes
repeated registry searches under the shared semaphore lock, but validating and
removing a large queue can still delay unrelated operations sharing that lock.

Replay across arbitrary future Go versions, bounded total heap use and bounded
discard latency are not established. A passing test demonstrates the tested
behavior and configuration; it does not expand the supported execution contract.
