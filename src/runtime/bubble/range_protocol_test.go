// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	rand "math/rand/v2"
	"runtime/bubble"
	"testing"
	"time"
)

// An older active timer must survive acknowledgments of newer stopped timers.
func TestRangeRetirement(t *testing.T) {
	var reset chan struct{}
	var fired int
	b, err := bubble.New(bubble.Options{Protocol: bubble.RangeProtocol}, func() {
		reset = make(chan struct{}, 1)
		time.AfterFunc(time.Hour, func() { fired++ })
		timer := time.NewTimer(time.Hour)
		timer.Reset(time.Hour)
		timer.Stop()
		for i := 0; i < 3; i++ {
			other := time.NewTimer(time.Hour)
			other.Reset(time.Hour)
			other.Stop()
		}
		<-reset
		timer.Reset(time.Hour)
		select {}
	})
	if err != nil {
		t.Fatal(err)
	}
	state := step(t, b, bubble.Activation{})
	first, last := state.TimerChanges[0], state.TimerChanges[len(state.TimerChanges)-1]
	old := state.TimerChanges[1]
	var escaped *bubble.Delivery
	state = step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
		escaped = d
		for _, pair := range [][2]bubble.TimerID{{0, last.ID}, {last.ID, old.ID}, {old.ID, last.ID + 1}, {first.ID, last.ID}} {
			if !rejectedOperation(func() { d.RetireTimerRange(pair[0], pair[1]) }) {
				t.Error("invalid range accepted")
			}
		}
		if !rejectedOperation(func() { d.FireTimer(old.ID, old.Generation+10) }) {
			t.Error("rejection mutated validation")
		}
		d.RetireTimerRange(last.ID, last.ID)
		d.RetireTimerRange(old.ID, last.ID-1)
		d.RetireTimerRange(old.ID, last.ID)
		if d.FireTimer(old.ID, old.Generation+10) {
			t.Error("acknowledged event fired")
		}
		if !rejectedOperation(func() { d.FireTimer(old.ID, 0) }) {
			t.Error("zero generation accepted")
		}
		reset <- struct{}{}
	}})
	next := state.TimerChanges[len(state.TimerChanges)-1]
	if next.ID <= last.ID {
		t.Fatal("reset reused acknowledged ID")
	}
	if !rejectedOperation(func() { escaped.RetireTimerRange(old.ID, last.ID) }) {
		t.Error("escaped delivery accepted")
	}
	step(t, b, bubble.Activation{Now: first.Deadline, Deliver: func(d *bubble.Delivery) {
		if !d.FireTimer(first.ID, first.Generation) {
			t.Error("older active timer damaged")
		}
		if !d.FireTimer(next.ID, next.Generation) {
			t.Error("reset timer damaged")
		}
		d.RetireTimerRange(1, next.ID)
		if d.FireTimer(next.ID, next.Generation+10) {
			t.Error("retired prefix fired")
		}
	}})
	if fired != 1 {
		t.Fatalf("callbacks %d", fired)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRangeGaps(t *testing.T) {
	const timers = 5
	var fired [timers]bool
	b, err := bubble.New(bubble.Options{Protocol: bubble.RangeProtocol}, func() {
		for i := 0; i < timers; i++ {
			timer := time.AfterFunc(time.Hour, func() { fired[i] = true })
			if i == 0 || i == 2 {
				continue
			}
			timer.Reset(time.Hour)
			timer.Stop()
		}
		select {}
	})
	if err != nil {
		t.Fatal(err)
	}
	state := step(t, b, bubble.Activation{})
	var arms []bubble.TimerChange
	for _, change := range state.TimerChanges {
		if change.Kind == bubble.TimerArmed && change.Generation == 1 {
			arms = append(arms, change)
		}
	}
	step(t, b, bubble.Activation{Now: arms[0].Deadline, Deliver: func(d *bubble.Delivery) {
		d.RetireTimerRange(arms[3].ID, arms[4].ID)
		d.RetireTimerRange(arms[1].ID, arms[1].ID)
		if !rejectedOperation(func() { d.RetireTimerRange(arms[1].ID, arms[4].ID) }) {
			t.Error("active gap accepted")
		}
		if !rejectedOperation(func() { d.FireTimer(arms[2].ID, 2) }) {
			t.Error("active gap validation lost")
		}
		if !d.FireTimer(arms[2].ID, 1) {
			t.Error("active gap did not fire")
		}
		d.RetireTimerRange(arms[2].ID, arms[2].ID)
		for _, arm := range arms[1:] {
			if d.FireTimer(arm.ID, 3) {
				t.Error("merged range fired")
			}
		}
		if !d.FireTimer(arms[0].ID, 1) {
			t.Error("older active timer did not fire")
		}
		d.RetireTimerRange(1, arms[4].ID)
	}})
	for i, value := range fired {
		if value != (i == 0 || i == 2) {
			t.Fatalf("callback %d fired=%t", i, value)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

// Compare observable late-event validation against a simple acknowledged-ID set.
// Sparse, overlapping and bridging ranges must never absorb unacknowledged IDs.
func TestRangeValidation(t *testing.T) {
	const timerCount = 128
	const deliveries = 100
	for seed := range uint64(5) {
		b, err := bubble.New(bubble.Options{Protocol: bubble.RangeProtocol}, func() {
			for range timerCount {
				timer := time.NewTimer(time.Hour)
				timer.Reset(time.Hour)
				timer.Stop()
			}
			select {}
		})
		if err != nil {
			t.Fatal(err)
		}
		state := step(t, b, bubble.Activation{})
		var ids []bubble.TimerChange
		for _, change := range state.TimerChanges {
			if change.Kind == bubble.TimerDisarmed && change.Generation == 2 {
				ids = append(ids, change)
			}
		}
		known := make([]bool, len(ids))
		source := rand.New(rand.NewPCG(seed, 0))
		prefix := 0
		for turn := range deliveries {
			first := source.IntN(len(ids))
			last := min(first+source.IntN(4), len(ids)-1)
			step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
				d.RetireTimerRange(ids[first].ID, ids[last].ID)
				for i := first; i <= last; i++ {
					known[i] = true
				}
				if turn%10 == 0 {
					prefix = min(prefix+1, len(ids))
					d.RetireTimerRange(1, ids[prefix-1].ID)
					for i := 0; i < prefix; i++ {
						known[i] = true
					}
				}
				for i, timer := range ids {
					rejected := rejectedOperation(func() {
						if d.FireTimer(timer.ID, timer.Generation+1) {
							t.Error("retired event fired")
						}
					})
					if rejected == known[i] {
						t.Fatalf("seed=%d turn=%d id=%d acknowledged=%t rejected=%t", seed, turn, timer.ID, known[i], rejected)
					}
				}
			}})
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
