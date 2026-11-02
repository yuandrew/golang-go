// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"fmt"
	"runtime/bubble"
	"testing"
	"time"
)

// Each operation includes two host activations and one batch of timers.
func BenchmarkBubbleTimers(b *testing.B) {
	const (
		fire            = "Fire"
		resetStopRetire = "ResetStopRetire"
	)
	for _, mode := range []string{fire, resetStopRetire} {
		for _, count := range []int{1, 100, 1000} {
			b.Run(fmt.Sprintf("%s/timers=%d", mode, count), func(b *testing.B) {
				var inbox chan struct{}
				processed := 0
				workflow, err := bubble.New(bubble.Options{}, func() {
					inbox = make(chan struct{}, 1)
					for range inbox {
						timers := make([]*time.Timer, count)
						for i := range timers {
							timers[i] = time.NewTimer(time.Nanosecond)
							if mode == resetStopRetire {
								if !timers[i].Reset(2*time.Nanosecond) || !timers[i].Stop() {
									panic("active timer did not reset and stop")
								}
							}
						}
						for _, timer := range timers {
							if mode == fire {
								<-timer.C
							}
							processed++
						}
					}
				})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := workflow.Close(); err != nil {
						b.Error(err)
					}
				})
				state, err := workflow.Step(bubble.Activation{})
				if err != nil || state.Status != bubble.Quiescent {
					b.Fatalf("initial Step: %+v, %v", state, err)
				}
				input := bubble.Activation{Deliver: func(*bubble.Delivery) { inbox <- struct{}{} }}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					state, err = workflow.Step(input)
					if err != nil || state.Status != bubble.Quiescent || len(state.TimerChanges) == 0 {
						b.Fatalf("timer batch Step: %+v, %v", state, err)
					}
					changes := state.TimerChanges
					activation := bubble.Activation{Deliver: func(d *bubble.Delivery) {
						for _, change := range changes {
							if change.Kind == bubble.TimerArmed && change.Generation == 1 && mode == fire {
								if !d.FireTimer(change.ID, change.Generation) {
									panic("active timer did not fire")
								}
							}
						}
						d.RetireTimerRange(changes[0].ID, changes[len(changes)-1].ID)
						for _, change := range changes {
							if change.Kind == bubble.TimerArmed && d.FireTimer(change.ID, change.Generation) {
								panic("retired timer fired")
							}
						}
					}}
					if mode == fire {
						activation.Now = changes[0].Deadline
					}
					state, err = workflow.Step(activation)
					if err != nil || state.Status != bubble.Quiescent {
						b.Fatalf("timer delivery Step: %+v, %v", state, err)
					}
				}
				b.StopTimer()
				if processed != b.N*count {
					b.Fatalf("processed %d timers, want %d", processed, b.N*count)
				}
				state, err = workflow.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { close(inbox) }})
				if err != nil || state.Status != bubble.Completed || state.LiveGoroutines != 0 {
					b.Fatalf("final Step: %+v, %v", state, err)
				}
			})
		}
	}
}
