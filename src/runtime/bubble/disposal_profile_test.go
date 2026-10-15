// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"runtime"
	"runtime/bubble"
	"sync/atomic"
	"testing"
)

//go:noinline
func profileDiscardWait(depth int, gate chan []byte) {
	var payload [256]byte
	payload[0] = byte(depth)
	if depth > 0 {
		profileDiscardWait(depth-1, gate)
	} else {
		select {
		case gate <- payload[:]:
		case gate <- payload[:]:
		}
	}
	runtime.KeepAlive(payload)
}

func TestProfileDuringDiscard(t *testing.T) {
	const rounds = 80
	const children = 256
	const depth = 32
	const recordCapacity = 4096
	var stop atomic.Bool
	var profiles atomic.Uint64
	done := make(chan struct{})
	go func() {
		defer close(done)
		records := make([]runtime.StackRecord, recordCapacity)
		for !stop.Load() {
			if _, ok := runtime.GoroutineProfile(records); ok {
				profiles.Add(1)
			}
		}
	}()
	t.Cleanup(func() { stop.Store(true); <-done })

	for range rounds {
		b, err := bubble.New(bubble.Options{}, func() {
			gate := make(chan []byte)
			for range children {
				go profileDiscardWait(depth, gate)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		state, err := b.Step(bubble.Activation{})
		if err != nil || state.Status != bubble.Quiescent || state.LiveGoroutines != children {
			t.Fatalf("paused state: %+v, %v", state, err)
		}
		before := profiles.Load()
		for profiles.Load() == before {
			runtime.Gosched()
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("completed profiles: %d", profiles.Load())
}
