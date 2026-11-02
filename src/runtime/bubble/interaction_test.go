// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"runtime/bubble"
	"runtime/trace"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Each slot has one writer. The controller reads only after Step or Discard.
type interactionTrace struct {
	next   atomic.Uint64
	events [64]string
}

func (r *interactionTrace) add(event string) {
	r.events[r.next.Add(1)-1] = event
}

func (r *interactionTrace) boundary(s bubble.State) {
	r.add(fmt.Sprintf("boundary:%d:%t:%d", s.Status, s.RootDone, s.LiveGoroutines))
	for _, timer := range s.TimerChanges {
		r.add(fmt.Sprintf("timer:%d:%d:%d:%d", timer.Kind, timer.ID, timer.Generation, timer.Deadline.UnixNano()))
	}
}

func (r *interactionTrace) result() []string {
	return slices.Clone(r.events[:r.next.Load()])
}

func interactionArmedTimer(t *testing.T, s bubble.State) bubble.TimerChange {
	t.Helper()
	for i := len(s.TimerChanges) - 1; i >= 0; i-- {
		if s.TimerChanges[i].Kind == bubble.TimerArmed {
			return s.TimerChanges[i]
		}
	}
	t.Fatalf("no armed timer: %+v", s)
	return bubble.TimerChange{}
}

type interactionTimerInput uint8

const (
	interactionReset interactionTimerInput = iota
	interactionStop
	interactionOmitFiring
)

func timerBatchTrace(t *testing.T, input interactionTimerInput) []string {
	t.Helper()
	var events interactionTrace
	var command chan bool
	var finish chan struct{}
	b := newBubble(t, func() {
		command = make(chan bool, 1)
		finish = make(chan struct{})
		worker := make(chan struct{})
		timer := time.NewTimer(time.Hour)
		go func() {
			<-worker
			events.add("child:ready")
			events.add(fmt.Sprintf("random:%x", rand.Uint64()))
		}()
		events.add("root:start")
		if <-command {
			events.add(fmt.Sprintf("reset:%t", timer.Reset(time.Hour)))
			close(worker)
			<-timer.C
			events.add("timer:received")
			<-finish
		} else {
			events.add(fmt.Sprintf("stop:%t", timer.Stop()))
			close(worker)
			select {
			case <-timer.C:
				panic("stopped timer received")
			default:
				events.add("timer:suppressed")
			}
		}
		events.add("root:end")
	})
	s := step(t, b, bubble.Activation{})
	events.boundary(s)
	old := interactionArmedTimer(t, s)
	// Authorization and control arrive together. Reset must suppress the pending
	// receive before the root waits for the replacement generation.
	s = step(t, b, bubble.Activation{Now: old.Deadline, Deliver: func(d *bubble.Delivery) {
		events.add("delivery:start")
		if !d.FireTimer(old.ID, old.Generation) {
			panic("old generation not armed")
		}
		command <- input != interactionStop
		events.add("delivery:end")
	}})
	events.boundary(s)
	if input == interactionStop {
		requireCompleted(t, s)
	} else {
		if s.Status != bubble.Quiescent {
			t.Fatalf("reset did not block: %+v", s)
		}
		fresh := interactionArmedTimer(t, s)
		if fresh.ID != old.ID || fresh.Generation <= old.Generation {
			t.Fatalf("bad reset: %+v -> %+v", old, fresh)
		}
		s = step(t, b, bubble.Activation{Now: fresh.Deadline, Deliver: func(d *bubble.Delivery) {
			if d.FireTimer(old.ID, old.Generation) {
				panic("stale generation authorized")
			}
			events.add("late:false")
			if input != interactionOmitFiring {
				if !d.FireTimer(fresh.ID, fresh.Generation) || d.FireTimer(fresh.ID, fresh.Generation) {
					panic("new generation not authorized exactly once")
				}
				events.add("fresh:true")
			}
		}})
		events.boundary(s)
		if s.Status != bubble.Quiescent {
			t.Fatalf("expected paused root: %+v", s)
		}
		if input != interactionOmitFiring {
			s = step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
				d.RetireTimerRange(fresh.ID, fresh.ID)
				events.add("retired")
				if d.FireTimer(old.ID, old.Generation) || d.FireTimer(fresh.ID, fresh.Generation) {
					panic("retired generation authorized")
				}
				events.add("retired:late:false")
				close(finish)
			}})
			events.boundary(s)
			requireCompleted(t, s)
		}
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
	events.add("discard:done")
	result := events.result()
	if slices.Contains(result, "timer:received") != (input == interactionReset) {
		t.Fatalf("unexpected timer receive for input %d: %q", input, result)
	}
	return result
}

func faultBatchTrace(t *testing.T, deliveryFault bool) []string {
	t.Helper()
	var events interactionTrace
	var fail, peer chan struct{}
	b := newBubble(t, func() {
		defer events.add("unexpected:root defer")
		fail, peer = make(chan struct{}), make(chan struct{})
		time.NewTimer(time.Hour)
		go func() {
			defer events.add("panic:defer")
			<-fail
			panic("expected child panic")
		}()
		go func() {
			defer events.add("unexpected:peer defer")
			<-peer
			events.add("unexpected:peer body")
		}()
		events.add("root:park")
		select {}
	})
	s := step(t, b, bubble.Activation{})
	events.boundary(s)
	arm := interactionArmedTimer(t, s)
	s, err := b.Step(bubble.Activation{Now: arm.Deadline, Deliver: func(d *bubble.Delivery) {
		events.add("delivery:start")
		if !d.FireTimer(arm.ID, arm.Generation) {
			panic("timer not armed")
		}
		events.add("fire:true")
		close(fail)
		close(peer)
		go func() { events.add("unexpected:delivery child") }()
		events.add("delivery:mutated")
		if deliveryFault {
			panic("expected delivery panic")
		}
		events.add("delivery:end")
	}})
	if err == nil || s.Status != bubble.Faulted {
		t.Fatalf("missing fault: %+v, %v", s, err)
	}
	wantLive := 3
	if deliveryFault {
		wantLive++
	}
	if s.RootDone || s.LiveGoroutines != wantLive {
		t.Fatalf("unexpected fault boundary: %+v", s)
	}
	events.boundary(s)
	before := events.next.Load()
	if _, err := b.Step(bubble.Activation{}); err == nil {
		t.Fatal("terminal Step accepted")
	}
	if events.next.Load() != before {
		t.Fatal("rejected Step ran user code")
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
	events.add("discard:done")
	result := events.result()
	if slices.Contains(result, "panic:defer") == deliveryFault {
		t.Fatalf("unexpected panic unwinding: %q", result)
	}
	for _, event := range result {
		if strings.HasPrefix(event, "unexpected:") {
			t.Fatal(event)
		}
	}
	return result
}

// Host instrumentation and an independent bubble compete for the same Ps.
func startInteractionNoise(t *testing.T) {
	t.Helper()
	if err := trace.Start(io.Discard); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var workers sync.WaitGroup
	var profiles, collections, peers atomic.Uint64
	errors := make(chan error, 1)
	t.Cleanup(func() {
		stop.Store(true)
		workers.Wait()
		trace.Stop()
		select {
		case err := <-errors:
			t.Error(err)
		default:
		}
	})
	workers.Go(func() {
		records := make([]runtime.StackRecord, 4096)
		for !stop.Load() {
			runtime.GoroutineProfile(records)
			profiles.Add(1)
			runtime.Gosched()
		}
	})
	workers.Go(func() {
		for !stop.Load() {
			runtime.GC()
			collections.Add(1)
			runtime.Gosched()
		}
	})
	workers.Go(func() {
		for !stop.Load() {
			b, err := bubble.New(bubble.Options{}, func() {
				ch := make(chan uint64, 2)
				go func() {
					for range 8 {
						ch <- rand.Uint64()
						runtime.Gosched()
					}
					close(ch)
				}()
				for range ch {
				}
			})
			if err != nil {
				errors <- err
				return
			}
			s, err := b.Step(bubble.Activation{})
			if err == nil && s.Status != bubble.Completed {
				err = fmt.Errorf("peer incomplete: %+v", s)
			}
			if discardErr := b.Discard(); discardErr != nil && err == nil {
				err = discardErr
			}
			if err != nil {
				errors <- err
				return
			}
			peers.Add(1)
		}
	})
	for profiles.Load() == 0 || collections.Load() == 0 || peers.Load() == 0 {
		select {
		case err := <-errors:
			t.Fatal(err)
		default:
		}
		runtime.Gosched()
	}
}

func requireInteractionTrace(t *testing.T, got, want []string) {
	t.Helper()
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			t.Fatalf("trace differs at event %d\nwant %q\n got %q", i, want, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("trace length differs\nwant %q\n got %q", want, got)
	}
}

func TestTimerBatchTrace(t *testing.T) {
	want := timerBatchTrace(t, interactionReset)
	if slices.Equal(want, timerBatchTrace(t, interactionStop)) {
		t.Fatal("trace missed changed control input")
	}
	if slices.Equal(want, timerBatchTrace(t, interactionOmitFiring)) {
		t.Fatal("trace missed omitted firing")
	}
	t.Run("instrumented", func(t *testing.T) {
		startInteractionNoise(t)
		for range 3 {
			requireInteractionTrace(t, timerBatchTrace(t, interactionReset), want)
		}
	})
}

func TestFaultBatchTrace(t *testing.T) {
	for _, deliveryFault := range []bool{false, true} {
		t.Run(fmt.Sprintf("delivery=%t", deliveryFault), func(t *testing.T) {
			want := faultBatchTrace(t, deliveryFault)
			t.Run("instrumented", func(t *testing.T) {
				startInteractionNoise(t)
				for range 3 {
					requireInteractionTrace(t, faultBatchTrace(t, deliveryFault), want)
				}
			})
		})
	}
}
