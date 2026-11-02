// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"runtime"
	"runtime/bubble"
	"runtime/metrics"
	"strings"
	"testing"
	"time"
)

func otherPauseCount() uint64 {
	samples := []metrics.Sample{{Name: "/sched/pauses/total/other:seconds"}}
	metrics.Read(samples)
	var count uint64
	for _, n := range samples[0].Value.Float64Histogram().Counts {
		count += n
	}
	return count
}

func TestDiscardAddsNoGlobalPause(t *testing.T) {
	const children = 32
	var cleanup [children]bool
	b, err := bubble.New(bubble.Options{}, func() {
		signal := make(chan struct{})
		for index := range children {
			go func() {
				defer func() { cleanup[index] = true }()
				select {
				case <-signal:
				case <-time.After(time.Hour):
				}
			}()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.Step(bubble.Activation{})
	if err != nil || state.Status != bubble.Quiescent || state.LiveGoroutines != children {
		t.Fatalf("paused state: %+v, %v", state, err)
	}
	runtime.GC()
	before := otherPauseCount()
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
	if after := otherPauseCount(); after != before {
		t.Fatalf("Discard added %d non-GC global pause(s)", after-before)
	}
	for _, ran := range cleanup {
		if ran {
			t.Fatal("discard ran a user defer")
		}
	}
}

func TestFaultDuringExit(t *testing.T) {
	const rounds = 4000
	const messageSize = 256
	message := strings.Repeat("x", messageSize)
	for round := range rounds {
		b, err := bubble.New(bubble.Options{}, func() {
			go func() { panic(message) }()
		})
		if err != nil {
			t.Fatal(err)
		}
		state, err := b.Step(bubble.Activation{})
		if err == nil || state.Status != bubble.Faulted {
			t.Fatalf("fault state: %+v, %v", state, err)
		}
		if err := b.Discard(); err != nil {
			t.Fatalf("round %d, state=%+v: %v", round, state, err)
		}
	}
}
