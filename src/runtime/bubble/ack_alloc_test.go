// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"runtime"
	"runtime/bubble"
	"testing"
	"time"
)

type ackOrder uint8

const (
	ackForward ackOrder = iota
	ackReverse
)
const (
	ackTimers     = 1024
	ackByteBudget = 256 // Per timer: allow slice growth, compaction and activation overhead.
)

// Individual acknowledgments must not copy all generation/range records each
// time. Validate late events and remaining active gaps while measuring churn.
func TestAckAllocation(t *testing.T) {
	for _, order := range []ackOrder{ackForward, ackReverse} {
		b, err := bubble.New(bubble.Options{Protocol: bubble.RangeProtocol}, func() {
			for range ackTimers {
				time.NewTimer(time.Hour)
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
		if len(ids) != ackTimers {
			t.Fatal("missing stopped timers")
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
			for i := range ids {
				index := i
				if order == ackReverse {
					index = len(ids) - 1 - i
				}
				id := ids[index].ID
				d.RetireTimerRange(id, id)
			}
		}})
		runtime.ReadMemStats(&after)
		allocated := after.TotalAlloc - before.TotalAlloc
		if allocated > ackTimers*ackByteBudget {
			t.Errorf("order=%d acknowledgment allocated %d bytes, budget %d", order, allocated, ackTimers*ackByteBudget)
		}
		step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
			for _, timer := range ids {
				if d.FireTimer(timer.ID, timer.Generation+1) {
					t.Error("acknowledged timer fired")
				}
				if !rejectedOperation(func() { d.FireTimer(timer.ID-1, timer.Generation+1) }) {
					t.Error("active gap validation lost")
				}
			}
		}})
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
