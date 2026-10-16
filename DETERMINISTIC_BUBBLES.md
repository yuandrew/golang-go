# Host-controlled deterministic bubbles

This proposal adds an opt-in execution API backed by the Go runtime. A host can
run native concurrent Go code until it blocks, deliver ordered inputs, and resume
execution under a reproducible scheduling policy. No new language syntax is
proposed. The API and its restrictions remain subject to design review.

## Review order

Start with the [`runtime/bubble` API](src/runtime/bubble/bubble.go) and its
[examples](src/runtime/bubble/example_test.go). The
[concept inventory](BUBBLE_CONCEPTS.md) separates the execution contract from
private implementation choices. Then read the
[scheduler and lifecycle implementation](src/runtime/bubble.go),
[timer bookkeeping](src/runtime/bubble_time.go), and
[application randomness bridge](src/runtime/bubble_rand.go).

Behavioral tests live in [`src/runtime/bubble`](src/runtime/bubble), including
fresh-process replay, ownership rejection, panic handling, supported paused
discard, timer retirement, and race-detector checks.

## Execution contract

- Native descendant goroutines, owned channels and select run cooperatively.
  A bubble runs one user goroutine at a time; separate bubbles may run concurrently.
- The host supplies logical time and ordered input batches. Advancing time alone
  does not authorize positive timers. Sleep and one-shot channel timers use
  explicit timer authorization, IDs and generations.
- Package-level math/rand and math/rand/v2 use a configured, bubble-owned source.
  Explicit generators retain their own sources. Application draws do not change
  select choices.
- Unrecovered user panics are reported after normal defer/recover handling.
  Remaining peers stay paused. A faulted execution cannot resume.
- Close can discard supported paused execution without running user code or
  defers. Discard is distinct from application cancellation or normal completion.

The host owns recording, external effects, input ordering and reconstruction.
The runtime does not provide durable storage, rollback or memory isolation.
Native map iteration remains unspecified; code whose outputs depend on key order
must sort keys explicitly. General context, WaitGroup, locks, tickers and callback
timers are outside the initial replay contract. Callback-timer compatibility code
and tests are present; they do not establish general context support.

Close validates only the bubble's tracked goroutines and coordinates with stack
scanning and profiling. It does not request a process-wide disposal pause.
Ordinary runtime pauses remain possible, and StackTrace separately pauses the
process. Unsupported waits are rejected before detachment. Active CPU loops
cannot be interrupted. Unsupported runtime operations can still be process-fatal.
Neither Close latency nor total retained memory has a general bound.

## Compatibility and costs

Replay requires the same program, initial configuration and ordered host inputs
under a compatible implementation. Matching protocol names do not guarantee
compatibility across Go versions. Hosts must establish that compatibility before
replaying recorded executions.

Tracking live goroutines and timer metadata costs memory and execution time
inside opted-in bubbles. Runtime changes can also affect ordinary Go programs.
Existing measurements do not establish CPU acceptance for the complete current
implementation. Test and measurement results apply only to their recorded source,
program and platform combinations, not arbitrary code or dependencies.

## Build and test

Build this checkout using the repository's normal bootstrap instructions. Then
run the resulting toolchain:

```sh
bin/go test runtime/bubble
bin/go test -race runtime/bubble
```

The API requires the runtime support in this change. Ordinary Go toolchains do
not provide the proposed package.
