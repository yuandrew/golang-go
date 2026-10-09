# Host-controlled deterministic bubbles

Experimental runtime prototype for design review; not an accepted Go proposal or
a production SDK. This branch consolidates the measured candidate and tests,
followed by API documentation changes.

## Review

Read the [concept inventory](BUBBLE_CONCEPTS.md) to separate existing Go machinery,
Quinn's new execution contract, our additions and private optimization policies.

Quinn's baseline is `8dea6502fad43cc3ae715806fcb93e575027c8d0`.
Compare that commit with this branch to review our additions. The imported
candidate contains 34 source/test files identified by source hashes; its manifest
SHA256 is `2c60cc2b4fa916cd849daf518283c6ef482a34a39bea46952e8b19cf1ea24a4b`.
Subsequent documentation commits change no runtime behavior. Frozen measurements
retain their original source identities.

Start with [`runtime/bubble`](src/runtime/bubble/bubble.go), then the
[runtime implementation](src/runtime/bubble.go). Tests live in
[`src/runtime/bubble`](src/runtime/bubble).

## Support and limits

- Owned native goroutines, channels and select; cooperative scheduling and yield.
- Host-controlled logical time, Sleep and one-shot channel timers with Stop/Reset.
- Controlled package-level math/rand and math/rand/v2 sources.
- Scoped panic reporting and restricted discard of paused execution.
- Timer retirement/reclamation and GC-safe channel ownership.

Hosts supply recorded inputs, timer authorization and replay policy. Temporal
history mapping and Core adapters are separate and are not included in this Go
branch. Native map iteration is not reproducible; command-sensitive code must
order keys explicitly. General context, WaitGroup, locks, tickers and callback
timers are outside the initial support promise. Experimental callback source and
tests remain in the imported candidate.

Close skips user defers. When owned goroutines remain, disposal pauses the entire
process, including unrelated goroutines, while inspecting and detaching supported
states. Rejected disposal can also incur that pause; no maximum duration is
guaranteed. Active loops cannot be interrupted. Runtime-fatal misuse remains
process-fatal. The mechanism provides neither rollback nor memory isolation.

Selected CPU measurements leave one multicore representative workload
inconclusive. General retained-memory bounds, disposal latency bounds, native
Linux execution and future-Go replay compatibility are unproven. Replay evidence
applies to specifically tested patched builds and programs.

## Build and test

Build this Go checkout using the repository's normal bootstrap instructions.
Then use its toolchain to run:

```sh
bin/go test runtime/bubble
bin/go test -race runtime/bubble
```

Stock Go does not provide this experimental package. The branch is ready for
source review, not a claim that every Go program or dependency is supported.
