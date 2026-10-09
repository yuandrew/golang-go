// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bubble provides experimental deterministic, host-driven goroutine
// execution. This package exists only in this Go runtime fork.
//
// Descendants inherit their bubble through ordinary go statements. Each bubble
// runs one user goroutine at a time and has an independent logical clock.
// Step runs until every remaining user goroutine is blocked, then returns to
// the host. Separate bubbles may run concurrently.
//
// The supported synchronization primitives are owned channels, select, and
// runtime.Gosched. Native Sleep and positive channel or callback timers require
// explicit host timer authorization. Nonpositive timers become ready locally.
// This is not a memory sandbox: callers must avoid ambient I/O,
// shared mutable state, map-dependent ordering, and other nondeterministic
// inputs. General sync, context, and Ticker support is not promised.
// Explicit runtime.GC calls, iterator coroutine transfers, and sync.Cond waits
// inside the bubble are rejected. Garbage collection initiated by the host is
// allowed.
//
// Package-level math/rand and math/rand/v2 calls share the bubble's application
// random source. Options.RandomSource supplies a custom source; by default a
// fresh PCG is seeded with (Options.Seed, 0). Application randomness is separate
// from scheduler choices. Explicitly constructed random generators retain their
// own sources. crypto/rand is unchanged and is not made deterministic.
// An unrecovered user goroutine panic faults the bubble after normal defer
// unwinding and pauses its peers. Close discards paused owned channel, select,
// nil-channel, and Sleep waits, plus queued runnable goroutines, without running
// user defers. It rejects other live waits without modifying execution.
// Close stops the world and scans all goroutines in this prototype. Unsupported
// runtime operations, including blocking or yielding during Deliver, may still
// terminate the process. An active CPU loop cannot be disposed of through Close.
//
// At the maximum logical timestamp (2262-04-11T23:47:16.854775807Z), creating or
// resetting a timer and positive-duration Sleep panic. Nonpositive Sleep still
// returns immediately. Timers armed before that timestamp may still be stopped
// or delivered there. This restriction prevents duration saturation from making
// a positive timer appear immediately ready without host authorization.
package bubble

import (
	"errors"
	"fmt"
	runtimebubble "internal/runtime/bubble"
	rand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// RangeProtocol selects the experimental range-retirement execution policy.
const RangeProtocol = "cooperative-v3-ranges"

// Options configures a new bubble. Seed zero is a reproducible seed.
type Options struct {
	Protocol  string
	StartTime time.Time
	// Seed controls scheduler choices and seeds the default application source.
	// Supplying RandomSource changes only application randomness.
	Seed uint64
	// RandomSource supplies package-level math/rand and math/rand/v2 draws.
	// Nil selects a fresh rand.NewPCG(Seed, 0). The bubble exclusively owns
	// this source until Close; do not share or use it externally. Replaying
	// requires a fresh source in the same initial state.
	//
	// Uint64 must return without blocking, yielding, starting goroutines,
	// calling Goexit, or recursively using package-level randomness. Source
	// calls run on the requesting goroutine inside its bubble. Panics propagate
	// normally; neither source mutations nor consumed values are rolled back.
	RandomSource rand.Source
}

// Bubble is a paused or running deterministic execution. It must not be copied.
// Its controller methods reject concurrent use and calls from inside any bubble.
type Bubble struct {
	busy    atomic.Bool
	handle  any
	now     time.Time
	started bool
	closed  bool
	status  Status
}

// Activation supplies a logical time and an optional input batch. Zero Now
// keeps the previous time. Deliver runs inside the bubble before user goroutines
// resume. It must return without blocking or yielding; it may operate on owned
// channels or start goroutines. Such goroutines run after Deliver returns.
// Calling runtime.Goexit during Deliver is rejected.
type Activation struct {
	Now     time.Time
	Deliver func(*Delivery)
}

// Status describes a boundary reached by Step.
type Status uint8

const (
	// Quiescent means all remaining user goroutines are blocked.
	Quiescent Status = iota
	// Completed means every user goroutine has exited.
	Completed
	// Faulted means a delivery callback or unrecovered user goroutine panicked.
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

type TimerID uint64
type TimerChangeKind uint8

const (
	TimerArmed TimerChangeKind = iota
	TimerDisarmed
)

// TimerChange records a timer registration or cancellation in operation order.
// Deadline is meaningful only for TimerArmed. A timer retains its ID across
// resets, but each registration has a new Generation.
type TimerChange struct {
	Kind       TimerChangeKind
	ID         TimerID
	Generation uint64
	Deadline   time.Time
}

// Delivery is valid only inside its activation callback. It must not be copied.
type Delivery struct {
	active atomic.Bool
	handle any
}

// FireTimer authorizes a timer generation at or after its deadline. It returns
// false for a known stale, canceled, or already authorized generation. Unknown
// IDs, early authorization, and use outside the owning callback panic. Under
// RangeProtocol, acknowledged IDs ignore positive generations; zero
// generations remain invalid.
func (d *Delivery) FireTimer(id TimerID, generation uint64) bool {
	if d == nil || !d.active.Load() {
		panic("bubble: delivery is no longer active")
	}
	return runtimebubble.FireTimer(d.handle, uint64(id), generation)
}

// RetireTimerRange acknowledges inclusive inactive IDs under RangeProtocol.
// Unknown IDs, reversed ranges and active timers reject before mutation.
// Late positive generations are ignored; Reset allocates a fresh ID.
func (d *Delivery) RetireTimerRange(first, last TimerID) {
	if d == nil || !d.active.Load() {
		panic("bubble: delivery is no longer active")
	}
	runtimebubble.RetireTimerRange(d.handle, uint64(first), uint64(last))
}

// New creates a paused bubble without executing f. An empty Protocol selects
// RangeProtocol. Zero StartTime selects 2000-01-01 UTC. Times must be positive,
// representable signed Unix nanoseconds; monotonic readings are discarded.
func New(opts Options, f func()) (*Bubble, error) {
	if runtimebubble.IsBubbled() {
		return nil, errors.New("bubble: controller called from inside a bubble")
	}
	if opts.Protocol != "" && opts.Protocol != RangeProtocol {
		return nil, fmt.Errorf("bubble: unsupported protocol %q", opts.Protocol)
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

// Step applies a batch and runs to quiescence or completion. Validation errors
// leave the bubble usable. Completed, faulted, and closed bubbles reject Step.
// Advancing the clock never by itself authorizes a positive timer to fire.
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

// Close releases a finished or never-started bubble, or discards supported paused
// execution without running user defers. Unsupported live waits are unchanged.
// Close stops the world; it is idempotent after success.
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
		return errors.New("bubble: cannot dispose unsupported or active goroutines")
	}
	b.closed, b.handle = true, nil
	return nil
}

// StackTrace reports paused goroutines and their block reasons. If controller
// validation fails, it returns the error text instead of a stack trace.
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
