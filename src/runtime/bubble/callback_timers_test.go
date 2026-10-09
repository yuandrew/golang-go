// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"context"
	"reflect"
	"runtime/bubble"
	"testing"
	"time"
)

const candidateInterval = time.Second

func TestCandidateTickerRejected(t *testing.T) {
	var recovered any
	b, _ := bubble.New(bubble.Options{}, func() {
		defer func() { recovered = recover() }()
		time.NewTicker(candidateInterval)
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || recovered == nil || s.Status != bubble.Completed {
		t.Fatalf("ticker: %+v %v panic=%v", s, err, recovered)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateAfterFunc(t *testing.T) {
	var trace []string
	var gate chan struct{}
	var timer *time.Timer
	var recovered any
	b, err := bubble.New(bubble.Options{}, func() {
		defer func() { recovered = recover() }()
		gate = make(chan struct{})
		done := make(chan struct{})
		timer = time.AfterFunc(candidateInterval, func() {
			trace = append(trace, "begin")
			<-gate
			trace = append(trace, "end")
			close(done)
		})
		<-done
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := b.Step(bubble.Activation{})
	if err != nil || recovered != nil {
		t.Fatalf("start: %v panic=%v", err, recovered)
	}
	if len(s.TimerChanges) != 1 {
		t.Fatalf("registrations: %v", s.TimerChanges)
	}
	c := s.TimerChanges[0]
	// Advancing time alone must never start a callback.
	s, err = b.Step(bubble.Activation{Now: c.Deadline.Add(candidateInterval)})
	if err != nil || len(trace) != 0 {
		t.Fatalf("unauthorized: %v %v", err, trace)
	}
	s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) {
		if !d.FireTimer(c.ID, c.Generation) {
			t.Error("fire rejected")
		}
		if d.FireTimer(c.ID, c.Generation) {
			t.Error("duplicate accepted")
		}
		if len(trace) != 0 {
			t.Error("callback ran during delivery")
		}
	}})
	if err != nil || s.Status != bubble.Quiescent || s.LiveGoroutines != 2 || !reflect.DeepEqual(trace, []string{"begin"}) {
		t.Fatalf("blocked callback: %+v %v %v", s, err, trace)
	}
	s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) {
		if timer.Stop() {
			t.Error("Stop claimed to cancel running callback")
		}
		close(gate)
	}})
	if err != nil || s.Status != bubble.Completed || !reflect.DeepEqual(trace, []string{"begin", "end"}) {
		t.Fatalf("completion: %+v %v %v", s, err, trace)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateResetStop(t *testing.T) {
	var timer *time.Timer
	var calls int
	var recovered any
	b, _ := bubble.New(bubble.Options{}, func() {
		defer func() { recovered = recover() }()
		timer = time.AfterFunc(candidateInterval, func() { calls++ })
		select {}
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || recovered != nil {
		t.Fatalf("start: %v panic=%v", err, recovered)
	}
	old := s.TimerChanges[0]
	s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) {
		if !timer.Reset(2 * candidateInterval) {
			t.Error("active Reset returned false")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	next := s.TimerChanges[len(s.TimerChanges)-1]
	s, err = b.Step(bubble.Activation{Now: next.Deadline, Deliver: func(d *bubble.Delivery) {
		if d.FireTimer(old.ID, old.Generation) {
			t.Error("stale generation accepted")
		}
		if !timer.Stop() {
			t.Error("active Stop returned false")
		}
		if d.FireTimer(next.ID, next.Generation) {
			t.Error("stopped generation accepted")
		}
		if timer.Reset(candidateInterval) {
			t.Error("inactive Reset returned true")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	next = s.TimerChanges[len(s.TimerChanges)-1]
	_, err = b.Step(bubble.Activation{Now: next.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(next.ID, next.Generation) }})
	if err != nil || calls != 1 {
		t.Fatalf("reset callback: %v calls=%d", err, calls)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateImmediate(t *testing.T) {
	var trace []string
	var recovered any
	b, _ := bubble.New(bubble.Options{}, func() {
		defer func() { recovered = recover() }()
		events := make(chan string, 2)
		time.AfterFunc(0, func() { events <- "zero" })
		time.AfterFunc(-candidateInterval, func() { events <- "negative" })
		trace = append(trace, "root")
		trace = append(trace, <-events, <-events)
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || recovered != nil {
		t.Fatalf("start: %v panic=%v", err, recovered)
	}
	if s.Status != bubble.Completed || len(s.TimerChanges) != 0 || !reflect.DeepEqual(trace, []string{"root", "zero", "negative"}) {
		t.Fatalf("immediate: %+v %v", s, trace)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateCallbackOrder(t *testing.T) {
	var trace []string
	var recovered any
	b, _ := bubble.New(bubble.Options{}, func() {
		defer func() { recovered = recover() }()
		done := make(chan struct{})
		events := make(chan string, 2)
		time.AfterFunc(candidateInterval, func() { events <- "first"; close(done) })
		time.AfterFunc(candidateInterval, func() { events <- "second" })
		<-done
		trace = append(trace, <-events, <-events)
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || recovered != nil {
		t.Fatalf("start: %v panic=%v", err, recovered)
	}
	if len(s.TimerChanges) != 2 {
		t.Fatalf("registrations: %v", s.TimerChanges)
	}
	a, z := s.TimerChanges[0], s.TimerChanges[1]
	s, err = b.Step(bubble.Activation{Now: z.Deadline, Deliver: func(d *bubble.Delivery) {
		d.FireTimer(z.ID, z.Generation)
		d.FireTimer(a.ID, a.Generation)
	}})
	if err != nil || s.Status != bubble.Completed || !reflect.DeepEqual(trace, []string{"second", "first"}) {
		t.Fatalf("order: %+v %v %v", s, err, trace)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateImmediateReset(t *testing.T) {
	var calls int
	var recovered any
	b, _ := bubble.New(bubble.Options{}, func() {
		defer func() { recovered = recover() }()
		done := make(chan struct{})
		timer := time.AfterFunc(candidateInterval, func() { close(done) })
		if !timer.Reset(0) {
			panic("active reset returned false")
		}
		<-done
		calls++
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || recovered != nil || s.Status != bubble.Completed || calls != 1 {
		t.Fatalf("immediate reset: %+v %v panic=%v calls=%d", s, err, recovered, calls)
	}
	if len(s.TimerChanges) != 2 || s.TimerChanges[1].Kind != bubble.TimerDisarmed {
		t.Fatalf("changes: %v", s.TimerChanges)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateDeadline(t *testing.T) {
	var got error
	var recovered any
	b, _ := bubble.New(bubble.Options{}, func() {
		defer func() { recovered = recover() }()
		ctx, cancel := context.WithTimeout(context.Background(), candidateInterval)
		defer cancel()
		<-ctx.Done()
		got = ctx.Err()
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || recovered != nil {
		t.Fatalf("start: %v panic=%v", err, recovered)
	}
	c := s.TimerChanges[0]
	s, err = b.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(c.ID, c.Generation) }})
	if err != nil || s.Status != bubble.Completed || got != context.DeadlineExceeded {
		t.Fatalf("deadline: %+v %v %v", s, err, got)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCandidateCallbackReset(t *testing.T) {
	var timer *time.Timer
	var gate chan struct{}
	var begun, finished int
	var recovered any
	b, _ := bubble.New(bubble.Options{}, func() {
		defer func() { recovered = recover() }()
		gate = make(chan struct{})
		done := make(chan struct{}, 2)
		timer = time.AfterFunc(candidateInterval, func() {
			begun++
			<-gate
			done <- struct{}{}
		})
		<-done
		finished++
		<-done
		finished++
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || recovered != nil {
		t.Fatalf("start: %v panic=%v", err, recovered)
	}
	c := s.TimerChanges[0]
	s, err = b.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(c.ID, c.Generation) }})
	if err != nil || begun != 1 || finished != 0 {
		t.Fatalf("first: %v %d %d", err, begun, finished)
	}
	s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) {
		if timer.Reset(candidateInterval) {
			t.Error("running callback Reset returned true")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	c = s.TimerChanges[len(s.TimerChanges)-1]
	s, err = b.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(c.ID, c.Generation) }})
	if err != nil || begun != 2 || finished != 0 {
		t.Fatalf("overlap: %v %d %d", err, begun, finished)
	}
	s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) { close(gate) }})
	if err != nil || s.Status != bubble.Completed || finished != 2 {
		t.Fatalf("completion: %+v %v %d", s, err, finished)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
