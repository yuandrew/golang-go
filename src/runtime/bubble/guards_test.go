// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"iter"
	"runtime"
	"runtime/bubble"
	"sync"
	"testing"
	"time"
)

func rejectedOperation(f func()) (rejected bool) {
	defer func() { rejected = recover() != nil }()
	f()
	return false
}

func TestUnsupportedRuntimeEntrypoints(t *testing.T) {
	for _, test := range []struct {
		name string
		call func()
	}{
		{"explicit-GC", runtime.GC},
		{"Cond-Wait", func() {
			cond := sync.NewCond(new(sync.Mutex))
			cond.L.Lock()
			cond.Wait()
		}},
		{"iter-Pull", func() {
			_, stop := iter.Pull(func(yield func(int) bool) { yield(1) })
			stop()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var rejected bool
			b := newBubble(t, func() { rejected = rejectedOperation(test.call) })
			requireCompleted(t, step(t, b, bubble.Activation{}))
			if !rejected {
				t.Fatal("unsupported operation was accepted")
			}
		})
	}
}

func TestExternalIteratorTransferRejected(t *testing.T) {
	var sequenceRan bool
	next, stop := iter.Pull(func(yield func(int) bool) {
		sequenceRan = true
		yield(1)
	})
	defer stop()
	var rejected bool
	b := newBubble(t, func() { rejected = rejectedOperation(func() { next() }) })
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if !rejected || sequenceRan {
		t.Fatalf("external coroutine transfer: rejected=%v sequenceRan=%v", rejected, sequenceRan)
	}
}

func TestCrossBubbleTimerControlRejected(t *testing.T) {
	var timer *time.Timer
	var received time.Time
	a := newBubble(t, func() { timer = time.NewTimer(time.Minute); received = <-timer.C })
	s := step(t, a, bubble.Activation{})
	if len(s.TimerChanges) != 1 {
		t.Fatalf("timer registration: %+v", s)
	}
	arm := s.TimerChanges[0]
	var stopRejected, resetRejected bool
	b := newBubble(t, func() {
		stopRejected = rejectedOperation(func() { timer.Stop() })
		resetRejected = rejectedOperation(func() { timer.Reset(time.Hour) })
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if !stopRejected || !resetRejected {
		t.Fatalf("cross-bubble controls: Stop rejected=%v Reset rejected=%v", stopRejected, resetRejected)
	}
	s = step(t, a, bubble.Activation{})
	if s.Status != bubble.Quiescent || len(s.TimerChanges) != 0 {
		t.Fatalf("cross-bubble control changed owner state: %+v", s)
	}
	requireCompleted(t, step(t, a, bubble.Activation{Now: arm.Deadline, Deliver: func(d *bubble.Delivery) {
		if !d.FireTimer(arm.ID, arm.Generation) {
			panic("original timer registration lost")
		}
	}}))
	if !received.Equal(arm.Deadline) {
		t.Fatalf("original timer changed: received %v want %v", received, arm.Deadline)
	}
}

func TestRepeatedSleepGenerations(t *testing.T) {
	var observations []time.Time
	b := newBubble(t, func() {
		for i := 0; i < 3; i++ {
			time.Sleep(time.Minute)
			observations = append(observations, time.Now())
		}
	})
	s := step(t, b, bubble.Activation{})
	var previous bubble.TimerChange
	for i := 0; i < 3; i++ {
		if s.Status != bubble.Quiescent || len(s.TimerChanges) != 1 {
			t.Fatalf("Sleep %d boundary: %+v", i, s)
		}
		arm := s.TimerChanges[0]
		if arm.Kind != bubble.TimerArmed || !arm.Deadline.Equal(start.Add(time.Duration(i+1)*time.Minute)) {
			t.Fatalf("Sleep %d registration: %+v", i, arm)
		}
		if i > 0 && (arm.ID != previous.ID || arm.Generation <= previous.Generation) {
			t.Fatalf("reused sleep timer lost identity/generation: previous=%+v current=%+v", previous, arm)
		}
		var stale, fired bool
		s = step(t, b, bubble.Activation{Now: arm.Deadline, Deliver: func(d *bubble.Delivery) {
			if i > 0 {
				stale = d.FireTimer(previous.ID, previous.Generation)
			}
			fired = d.FireTimer(arm.ID, arm.Generation)
		}})
		if stale || !fired {
			t.Fatalf("Sleep %d authorization: stale=%v fired=%v", i, stale, fired)
		}
		previous = arm
	}
	requireCompleted(t, s)
	if len(observations) != 3 {
		t.Fatalf("Sleep observations: %v", observations)
	}
	for i, observed := range observations {
		if !observed.Equal(start.Add(time.Duration(i+1) * time.Minute)) {
			t.Errorf("Sleep %d clock: %v", i, observed)
		}
	}
}

func TestTimerClockLimit(t *testing.T) {
	maximum := time.Unix(0, 1<<63-1)
	for _, duration := range []time.Duration{-1, 0, 1} {
		var newRejected, sleepRejected bool
		b, err := bubble.New(bubble.Options{StartTime: maximum}, func() {
			newRejected = rejectedOperation(func() { time.NewTimer(duration) })
			sleepRejected = rejectedOperation(func() { time.Sleep(duration) })
		})
		if err != nil {
			t.Fatal(err)
		}
		requireCompleted(t, step(t, b, bubble.Activation{}))
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if !newRejected || sleepRejected != (duration > 0) {
			t.Errorf("duration %v at clock limit: NewTimer rejected=%v Sleep rejected=%v", duration, newRejected, sleepRejected)
		}
	}
	var timer *time.Timer
	var gate chan struct{}
	b := newBubble(t, func() { timer = time.NewTimer(time.Minute); gate = make(chan struct{}); <-gate })
	step(t, b, bubble.Activation{})
	var resetRejected, stopped bool
	requireCompleted(t, step(t, b, bubble.Activation{Now: maximum, Deliver: func(*bubble.Delivery) {
		resetRejected = rejectedOperation(func() { timer.Reset(time.Second) })
		stopped = timer.Stop()
		close(gate)
	}}))
	if !resetRejected || !stopped {
		t.Fatalf("control at clock limit: Reset rejected=%v Stop=%v", resetRejected, stopped)
	}
}

func TestTimerSaturatesDeadline(t *testing.T) {
	maximum := time.Unix(0, 1<<63-1)
	var received time.Time
	b, err := bubble.New(bubble.Options{StartTime: maximum.Add(-time.Second)}, func() {
		received = <-time.NewTimer(time.Hour).C
	})
	if err != nil {
		t.Fatal(err)
	}
	s := step(t, b, bubble.Activation{})
	if len(s.TimerChanges) != 1 || !s.TimerChanges[0].Deadline.Equal(maximum) {
		t.Fatalf("saturated timer registration: %+v", s)
	}
	arm := s.TimerChanges[0]
	s = step(t, b, bubble.Activation{Now: maximum})
	if s.Status != bubble.Quiescent || !received.IsZero() {
		t.Fatalf("saturated timer fired without authorization: %+v received=%v", s, received)
	}
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) { d.FireTimer(arm.ID, arm.Generation) }}))
	if !received.Equal(maximum) {
		t.Fatalf("saturated timer receive: %v", received)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
