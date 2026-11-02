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

func TestLogicalOwnerRetainedDuringAllocationAndGC(t *testing.T) {
	originalProcs := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(originalProcs)
	for _, delivery := range []bool{false, true} {
		name := "application"
		if delivery {
			name = "delivery"
		}
		t.Run(name, func(t *testing.T) {
			// These atomic flags coordinate the external test harness only.
			// They deliberately expose GC timing, never application inputs.
			var phase atomic.Int32
			var attempts atomic.Int32
			var earlyPeer, stop atomic.Bool
			gcDone := make(chan struct{})
			go func() {
				defer close(gcDone)
				for !stop.Load() {
					if phase.Load() == 1 {
						attempts.Add(1)
						runtime.GC()
					} else {
						runtime.Gosched()
					}
				}
			}()
			defer func() { stop.Store(true); <-gcDone }()

			var done chan struct{}
			allocate := func() {
				done = make(chan struct{}, 1)
				phase.Store(1)
				go func() {
					if phase.Load() != 2 {
						earlyPeer.Store(true)
					}
					done <- struct{}{}
				}()
				// Ensure the external GC worker has started while a peer is
				// queued. This spin does not yield the bubble's logical turn.
				for attempts.Load() == 0 {
				}
				var retained [8][]byte
				for i := 0; i < 256; i++ {
					payload := replayPayload()
					for j := range payload {
						payload[j] = byte(i + j)
					}
					retained[i%len(retained)] = payload
					runtime.KeepAlive(retained)
				}
				phase.Store(2)
			}
			b := newBubble(t, func() {
				if !delivery {
					allocate()
				}
				<-done
			})
			var activation bubble.Activation
			if delivery {
				activation.Deliver = func(*bubble.Delivery) { allocate() }
			}
			requireCompleted(t, step(t, b, activation))
			if earlyPeer.Load() {
				t.Fatal("queued peer ran before the allocating owner ended its logical turn")
			}
			if attempts.Load() == 0 {
				t.Fatal("external GC did not overlap the allocation section")
			}
		})
	}
}
