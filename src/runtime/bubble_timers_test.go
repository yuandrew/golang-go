// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime_test

import (
	"runtime"
	"runtime/bubble"
	"testing"
	"time"
	"weak"
)

func TestBubbleTimerReclamation(t *testing.T) {
	const count = 1000
	const rounds = 20
	var command chan bool
	var refs []weak.Pointer[time.Ticker]
	defers := 0
	b, _ := bubble.New(bubble.Options{}, func() {
		command = make(chan bool, 1)
		anchor := time.NewTicker(2 * time.Hour)
		defer func() { defers++; anchor.Stop() }()
		for range rounds {
			for range count {
				refs = append(refs, weak.Make(time.NewTicker(time.Hour)))
			}
			<-command
		}
		select {
		case <-anchor.C:
			panic("time alone fired anchor")
		default:
		}
		select {}
	})
	defer b.Discard()
	s, e := b.Step(bubble.Activation{})
	if e != nil {
		t.Fatal(e)
	}
	tokens := s.TimerChanges[1:]
	for round := 0; round < rounds; round++ {
		runtime.GC()
		runtime.GC()
		for _, ref := range refs {
			if ref.Value() != nil {
				t.Fatal("abandoned payload retained")
			}
		}
		refs = nil
		s, e = b.Step(bubble.Activation{Now: tokens[0].Deadline, Deliver: func(d *bubble.Delivery) {
			for _, c := range tokens {
				if !d.FireTimer(c.ID, c.Generation) {
					panic("current rejected")
				}
				if d.FireTimer(c.ID, c.Generation) {
					panic("duplicate accepted")
				}
			}
		}})
		if e != nil {
			t.Fatal(e)
		}
		if len(s.TimerChanges) != 0 {
			t.Fatal("unconsumed timers rearmed", s)
		}
		// A generic host acknowledges only after observing the completed boundary.
		s, e = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) {
			d.RetireTimerRange(tokens[0].ID, tokens[len(tokens)-1].ID)
			command <- true
		}})
		if e != nil {
			t.Fatal(e)
		}
		tokens = s.TimerChanges
		registered, retired, ranges := runtime.BubbleTimerRegistry(handleOf(b))
		want := count + 1
		if round == rounds-1 {
			want = 1
		}
		if registered != want || retired != 0 || ranges != 1 {
			t.Fatalf("round=%d registered=%d retired=%d ranges=%d", round, registered, retired, ranges)
		}
	}
	if e = b.Discard(); e != nil {
		t.Fatal(e)
	}
	if defers != 0 {
		t.Fatal("discard ran defer")
	}
}
