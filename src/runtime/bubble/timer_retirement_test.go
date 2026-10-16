// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"runtime/bubble"
	"testing"
	"time"
)

func TestRetiredGeneration(t *testing.T) {
	for _, reset := range []string{"once", "reset"} {
		t.Run(reset, func(t *testing.T) {
			b := newBubble(t, func() {
				timer := time.AfterFunc(time.Hour, func() {})
				if reset == "reset" {
					timer.Reset(time.Hour)
				}
				timer.Stop()
				select {}
			})
			s := step(t, b, bubble.Activation{})
			last := s.TimerChanges[len(s.TimerChanges)-1]
			s = step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
				for generation := uint64(1); generation <= last.Generation; generation++ {
					if d.FireTimer(last.ID, generation) {
						t.Error("retired generation fired")
					}
				}
			}})
			s, err := b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) { d.FireTimer(last.ID, last.Generation+1) }})
			if err == nil || s.Status != bubble.Faulted {
				t.Fatalf("future retired generation: %+v %v", s, err)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFailedRegistration(t *testing.T) {
	const clockLimitNanoseconds = 1<<63 - 1
	b, err := bubble.New(bubble.Options{StartTime: time.Unix(0, clockLimitNanoseconds)}, func() {
		func() { defer func() { recover() }(); time.NewTimer(time.Hour) }()
		select {}
	})
	if err != nil {
		t.Fatal(err)
	}
	s := step(t, b, bubble.Activation{})
	if len(s.TimerChanges) != 0 {
		t.Fatalf("failed registration emitted events: %v", s.TimerChanges)
	}
	s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) { d.FireTimer(1, 1) }})
	if err == nil || s.Status != bubble.Faulted {
		t.Fatalf("generation-zero registration: %+v %v", s, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

// Stop and reset across a partially retired batch, then authorize its survivor.
// Compaction must not change IDs, generations, or stale-event behavior.
func TestTimerBatchReset(t *testing.T) {
	const timerCount = 64
	var fired int
	b := newBubble(t, func() {
		timers := make([]*time.Timer, timerCount)
		for i := range timers {
			timers[i] = time.AfterFunc(time.Hour, func() { fired++ })
		}
		// Reset into an existing retired slot before the batch is compacted.
		timers[0].Stop()
		timers[0].Reset(time.Hour)
		for _, timer := range timers {
			timer.Stop()
		}
		// Reset again after every active registration has been retired.
		timers[0].Reset(time.Hour)
		select {}
	})
	s := step(t, b, bubble.Activation{})
	last := s.TimerChanges[len(s.TimerChanges)-1]
	s = step(t, b, bubble.Activation{Now: last.Deadline, Deliver: func(d *bubble.Delivery) {
		for generation := uint64(1); generation < last.Generation; generation++ {
			if d.FireTimer(last.ID, generation) {
				t.Error("stale survivor generation fired")
			}
		}
		if !d.FireTimer(last.ID, last.Generation) {
			t.Error("survivor did not fire")
		}
		if d.FireTimer(last.ID, last.Generation) {
			t.Error("duplicate survivor event fired")
		}
	}})
	if fired != 1 || s.Status != bubble.Quiescent {
		t.Fatalf("callback count/status: %d, %+v", fired, s)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
