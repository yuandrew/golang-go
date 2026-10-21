// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bubble provides deterministic, host-driven goroutine execution.
//
// Descendants inherit their bubble through ordinary go statements. Each bubble
// runs one user goroutine at a time and has an independent logical clock.
// Step runs until every remaining user goroutine is blocked, then returns to
// the host. Separate bubbles may run concurrently.
//
// The supported synchronization primitives are owned channels, select,
// sync.WaitGroup, and runtime.Gosched. time.Now observes the logical clock. time.Sleep and positive
// one-shot channel timers require explicit host timer authorization. Nonpositive
// timers become ready locally.
// This is not a memory sandbox: callers must avoid ambient I/O,
// mutable state shared across concurrent execution domains, map-dependent
// ordering, and other nondeterministic inputs. General sync, context, Ticker,
// and callback-timer support is outside the replay contract.
// Explicit runtime.GC calls, iterator coroutine transfers, and sync.Cond waits
// inside the bubble are rejected. Garbage collection initiated by the host is
// allowed.
//
// Package-level math/rand and math/rand/v2 calls share the bubble's application
// random source. Options.RandomSource supplies a custom source; by default a
// fresh PCG is seeded with (Options.Seed, 0). Application randomness is separate
// from scheduler choices. Explicitly constructed random generators retain their
// own sources. crypto/rand is unchanged and is not made deterministic.
//
// A WaitGroup must start its active use inside its owning bubble. Add, Done,
// Wait, and Go must stay in that domain until all Wait calls have returned.
// A package-level WaitGroup value cannot be associated in this prototype; a
// heap-allocated group referenced by a package-level pointer can be owned.
//
// An unrecovered user goroutine panic faults the bubble after normal defer
// unwinding and pauses its peers. Close releases execution with no live
// goroutines. Discard abandons paused owned channel, select, nil-channel,
// Sleep, and owned WaitGroup waits, plus queued runnable goroutines, without
// running user defers. Groups with abandoned Wait calls are permanently invalid;
// subsequent Add, Done, Wait, or Go calls panic. It rejects other live waits
// without modifying execution.
// Discard inspects only the bubble's tracked goroutines and does not request a
// process-wide disposal pause. Ordinary runtime pauses can still occur, and
// StackTrace separately pauses the process. Unsupported runtime operations,
// including blocking or yielding during Deliver, may still terminate the process.
// An active CPU loop cannot be disposed of through Discard.
//
// At the maximum logical timestamp (2262-04-11T23:47:16.854775807Z), creating or
// resetting a timer and positive-duration Sleep panic. Nonpositive Sleep still
// returns immediately. Timers armed before that timestamp may still be stopped
// or delivered there. This restriction prevents duration saturation from making
// a positive timer appear immediately ready without host authorization.
//
// Replay requires the same program, initial configuration and ordered host
// inputs under a compatible implementation. Hosts must establish compatibility
// before replaying across Go versions.
package bubble

import (
	"errors"
	runtimebubble "internal/runtime/bubble"
	rand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures a new bubble. Seed zero is a reproducible seed.
type Options struct {
	// StartTime sets the logical clock. Zero selects 2000-01-01 UTC.
	StartTime time.Time
	// Seed controls scheduler choices and seeds the default application source.
	// Supplying RandomSource changes only application randomness.
	Seed uint64
	// RandomSource supplies package-level math/rand and math/rand/v2 draws.
	// Nil selects a fresh rand.NewPCG(Seed, 0). The bubble exclusively owns
	// this source until Close or Discard succeeds; do not share or use it externally.
	// Replaying requires a fresh source in the same initial state.
	//
	// Uint64 must return without blocking, yielding, starting goroutines,
	// calling Goexit, or recursively using package-level randomness. Source
	// calls run on the requesting goroutine inside its bubble. Panics propagate
	// normally; neither source mutations nor consumed values are rolled back.
	RandomSource rand.Source
}

// Bubble represents a host-controlled deterministic execution. It must not be copied.
// Its controller methods reject concurrent use and calls from inside any bubble.
type Bubble struct {
	busy    atomic.Bool
	handle  any
	now     time.Time
	started bool
	closed  bool
	status  Status
}

// Activation supplies host input and an optional clock update to [Bubble.Step].
type Activation struct {
	// Now sets the logical clock for this Step. A zero value keeps the current
	// time. On the first Step, a nonzero Now must match the configured start
	// time. Later calls may keep or advance the clock, but cannot move it back.
	// Advancing Now alone does not authorize positive-duration timers to fire.
	Now time.Time

	// Deliver, if non-nil, runs inside the bubble before Step runs or resumes
	// its goroutines. It lets the host make external events available to bubble
	// code, for example by sending a message to an owned channel with buffer
	// space. It may also close owned channels or authorize timers through
	// [Delivery.FireTimer].
	//
	// Deliver must return without blocking or yielding. It may start goroutines,
	// but they run only after it returns. Calling runtime.Goexit is rejected.
	Deliver func(*Delivery)
}

// Status describes a boundary reached by Step.
type Status uint8

const (
	// Quiescent means all remaining user goroutines are blocked.
	Quiescent Status = iota
	// Completed means every user goroutine has exited.
	Completed
	// Faulted means an unrecovered panic escaped a delivery callback or user goroutine.
	Faulted
)

// State is a snapshot at a Step boundary. RootDone does not imply Completed:
// descendants may outlive the initial function.
type State struct {
	Status         Status
	RootDone       bool
	LiveGoroutines int
	TimerChanges   []TimerChange
}

// TimerID identifies a timer within one bubble. Acknowledged IDs are not reused.
type TimerID uint64

// TimerChangeKind describes a timer registration change reported by Step.
type TimerChangeKind uint8

const (
	// TimerArmed records a positive timer awaiting host authorization.
	TimerArmed TimerChangeKind = iota
	// TimerDisarmed records cancellation of an armed timer by Stop or Reset.
	TimerDisarmed
)

// TimerChange records a timer registration or cancellation in operation order.
// Deadline is meaningful only for TimerArmed. Reset retains the ID and advances
// Generation until the host acknowledges that ID; Reset then assigns a fresh ID.
type TimerChange struct {
	Kind       TimerChangeKind
	ID         TimerID
	Generation uint64
	Deadline   time.Time
}

// Delivery gives an [Activation.Deliver] callback access to its bubble's timer
// controls: [Delivery.FireTimer] authorizes a firing, and
// [Delivery.RetireTimerRange] acknowledges inactive timer IDs.
//
// [Bubble.Step] creates it for the callback. Use it only on that callback's
// goroutine, while the callback is running. Its methods panic if used after
// the callback returns or from another goroutine. A Delivery must not be copied.
type Delivery struct {
	active atomic.Bool
	handle any
}

// FireTimer authorizes a timer generation at or after its deadline. It returns
// false for a known stale, canceled, or already authorized generation. Unknown
// IDs, early authorization, and use outside the owning callback panic.
// Acknowledged IDs ignore positive generations; zero generations remain invalid.
func (d *Delivery) FireTimer(id TimerID, generation uint64) bool {
	if d == nil || !d.active.Load() {
		panic("bubble: delivery is no longer active")
	}
	return runtimebubble.FireTimer(d.handle, uint64(id), generation)
}

// RetireTimerRange acknowledges inclusive inactive IDs.
// Unknown IDs, reversed ranges and active timers reject before mutation.
// Late positive generations are ignored; Reset allocates a fresh ID.
// Acknowledgments are host inputs and must be reproduced in order during replay.
func (d *Delivery) RetireTimerRange(first, last TimerID) {
	if d == nil || !d.active.Load() {
		panic("bubble: delivery is no longer active")
	}
	runtimebubble.RetireTimerRange(d.handle, uint64(first), uint64(last))
}

// New creates a paused bubble without executing f.
// Zero StartTime selects 2000-01-01 UTC. Times must be positive,
// representable signed Unix nanoseconds; monotonic readings are discarded.
func New(opts Options, f func()) (*Bubble, error) {
	if runtimebubble.IsBubbled() {
		return nil, errors.New("bubble: controller called from inside a bubble")
	}
	if f == nil {
		return nil, errors.New("bubble: nil root function")
	}
	now := opts.StartTime
	if now.IsZero() {
		now = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	now, err := normalizeTime(now)
	if err != nil {
		return nil, err
	}
	source := opts.RandomSource
	if source == nil {
		source = rand.NewPCG(opts.Seed, 0)
	}
	random := &randomSource{source: source}
	return &Bubble{handle: runtimebubble.New(now.UnixNano(), opts.Seed, f, random.next), now: now}, nil
}

// randomSource provides real synchronization for shared package-level draws.
// A logical scheduling turn alone does not protect a Source under Go's memory
// model or the race detector. The runtime rejects recursive draws before Lock.
type randomSource struct {
	mu     sync.Mutex
	source rand.Source
}

func (s *randomSource) next() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.source.Uint64()
}

func normalizeTime(t time.Time) (time.Time, error) {
	t = t.Round(0).UTC()
	if t.Before(time.Unix(0, 1)) || t.After(time.Unix(0, 1<<63-1)) {
		return time.Time{}, errors.New("bubble: time outside positive signed Unix nanoseconds")
	}
	return t, nil
}

func (b *Bubble) enter() error {
	if runtimebubble.IsBubbled() {
		return errors.New("bubble: controller called from inside a bubble")
	}
	if b == nil {
		return errors.New("bubble: nil bubble")
	}
	if !b.busy.CompareAndSwap(false, true) {
		return errors.New("bubble: concurrent controller call")
	}
	return nil
}

// Step applies a batch and runs to quiescence, completion or a terminal fault.
// Validation errors leave the bubble usable. Completed, faulted, and closed
// bubbles reject Step.
// Advancing the clock never by itself authorizes a positive timer to fire.
// A delivery panic or unrecovered user panic returns a terminal fault. Delivery
// mutations and consumed inputs are not rolled back.
// Returned errors do not preserve the original panic value. Panic diagnostics
// are bounded, and user formatting methods are not called.
func (b *Bubble) Step(a Activation) (state State, err error) {
	if err := b.enter(); err != nil {
		return State{}, err
	}
	defer b.busy.Store(false)
	if b.closed || b.handle == nil {
		return State{}, errors.New("bubble: closed or uninitialized bubble")
	}
	if b.started && b.status != Quiescent {
		return State{}, errors.New("bubble: execution is terminal")
	}
	now := b.now
	if !a.Now.IsZero() {
		var err error
		now, err = normalizeTime(a.Now)
		if err != nil {
			return State{}, err
		}
		if !b.started && !now.Equal(b.now) {
			return State{}, errors.New("bubble: first activation must match StartTime")
		}
		if now.Before(b.now) {
			return State{}, errors.New("bubble: clock cannot move backward")
		}
	}
	var deliver func()
	if a.Deliver != nil {
		deliver = func() {
			d := &Delivery{handle: b.handle}
			d.active.Store(true)
			defer d.active.Store(false)
			a.Deliver(d)
		}
	}
	b.now, b.started = now, true
	returned := false
	defer func() {
		if !returned {
			v := recover()
			state = publicState(runtimebubble.Snapshot(b.handle))
			state.Status = Faulted
			b.status = Faulted
			err = deliveryPanicError(v)
		}
	}()
	result := runtimebubble.Step(b.handle, now.UnixNano(), deliver)
	state = publicState(result)
	b.status = state.Status
	returned = true
	if state.Status == Faulted {
		return state, errors.New("bubble: " + result.Failure)
	}
	return state, nil
}

func deliveryPanicError(value any) error {
	// Formatting arbitrary panic values could execute their Error or String
	// methods after the controller has left the bubble. Preserve only a bounded
	// built-in string; arbitrary user code must remain paused after a fault.
	if message, ok := value.(string); ok {
		if len(message) > 256 {
			message = message[:256] + "..."
		}
		return errors.New("bubble: delivery panicked: " + message)
	}
	return errors.New("bubble: delivery panicked (non-string value)")
}

func publicState(r runtimebubble.State) State {
	s := State{RootDone: r.RootDone, LiveGoroutines: r.LiveGoroutines}
	if r.LiveGoroutines == 0 {
		s.Status = Completed
	}
	if r.Failure != "" {
		s.Status = Faulted
	}
	for _, change := range r.TimerChanges {
		c := TimerChange{Kind: TimerChangeKind(change.Kind), ID: TimerID(change.ID), Generation: change.Generation}
		if c.Kind == TimerArmed {
			c.Deadline = time.Unix(0, change.Deadline).UTC()
		}
		s.TimerChanges = append(s.TimerChanges, c)
	}
	return s
}

// Close releases a bubble with no live goroutines, including completed and
// never-started execution. It rejects an active Step or any live goroutines
// without modifying execution. Close is idempotent after either Close or
// [Bubble.Discard] succeeds.
func (b *Bubble) Close() error {
	if err := b.enter(); err != nil {
		return err
	}
	defer b.busy.Store(false)
	if b.closed {
		return nil
	}
	if b.handle == nil {
		return errors.New("bubble: uninitialized bubble")
	}
	if !runtimebubble.Close(b.handle) {
		return errors.New("bubble: cannot close with live goroutines")
	}
	b.closed, b.handle = true, nil
	return nil
}

// Discard releases a bubble, abandoning supported paused execution without
// running user code or defers. It supports owned channel, select, nil-channel,
// Sleep, and owned WaitGroup waits, plus queued runnable goroutines. Groups with
// abandoned Wait calls become permanently invalid; later Add, Done, Wait, or Go
// calls panic. Groups whose Wait calls returned normally remain reusable.
// It rejects an active Step or unsupported live waits without modifying execution.
// Discard is not application cancellation and cannot interrupt active code.
//
// Discard validates and detaches only this bubble's tracked goroutines, without
// requesting a process-wide disposal pause. It may wait for stack scanning or
// profiling coordination. Ordinary runtime pauses can still affect unrelated
// goroutines, and Discard has no guaranteed maximum latency.
//
// Discard also releases completed or never-started execution. It is idempotent
// after either Discard or [Bubble.Close] succeeds; no subsequent Close is needed.
func (b *Bubble) Discard() error {
	if err := b.enter(); err != nil {
		return err
	}
	defer b.busy.Store(false)
	if b.closed {
		return nil
	}
	if b.handle == nil {
		return errors.New("bubble: uninitialized bubble")
	}
	if !runtimebubble.Discard(b.handle) {
		return errors.New("bubble: cannot discard unsupported or active goroutines")
	}
	b.closed, b.handle = true, nil
	return nil
}

// StackTrace reports paused goroutines and their block reasons. It pauses the
// entire Go process during inspection, including unrelated goroutines. If
// controller validation fails, it returns the error text instead of a stack trace.
func (b *Bubble) StackTrace() string {
	if err := b.enter(); err != nil {
		return err.Error()
	}
	defer b.busy.Store(false)
	if b.closed || b.handle == nil {
		return "bubble: closed or uninitialized bubble"
	}
	return runtimebubble.StackTrace(b.handle)
}
