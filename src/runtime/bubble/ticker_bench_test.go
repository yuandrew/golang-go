// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"runtime/bubble"
	"testing"
	"time"
)

// Compare steady periodic delivery using a reset Timer and a Ticker.
func BenchmarkBubblePeriodic(b *testing.B) {
	for _, mode := range []string{"Timer", "Ticker"} {
		b.Run(mode, func(b *testing.B) {
			delivered := 0
			bb, err := bubble.New(bubble.Options{}, func() {
				var timer *time.Timer
				var ticks <-chan time.Time
				if mode == "Ticker" {
					ticker := time.NewTicker(time.Second)
					defer ticker.Stop()
					ticks = ticker.C
				} else {
					timer = time.NewTimer(time.Second)
					defer timer.Stop()
					ticks = timer.C
				}
				for {
					<-ticks
					delivered++
					if timer != nil {
						timer.Reset(time.Second)
					}
				}
			})
			if err != nil {
				b.Fatal(err)
			}
			defer bb.Discard()
			state, err := bb.Step(bubble.Activation{})
			if err != nil {
				b.Fatal(err)
			}
			next := state.TimerChanges[0]
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				state, err = bb.Step(bubble.Activation{Now: next.Deadline, Deliver: func(d *bubble.Delivery) {
					if !d.FireTimer(next.ID, next.Generation) {
						panic("current rejected")
					}
				}})
				if err != nil || len(state.TimerChanges) != 1 {
					b.Fatalf("%+v %v", state, err)
				}
				next = state.TimerChanges[0]
			}
			b.StopTimer()
			if delivered != b.N {
				b.Fatalf("delivered %d, want %d", delivered, b.N)
			}
		})
	}
}

func BenchmarkBubbleTickerLifecycle(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bb, err := bubble.New(bubble.Options{}, func() {
			ticker := time.NewTicker(time.Second)
			<-ticker.C
			ticker.Stop()
		})
		if err != nil {
			b.Fatal(err)
		}
		s, err := bb.Step(bubble.Activation{})
		if err != nil {
			b.Fatal(err)
		}
		c := s.TimerChanges[0]
		s, err = bb.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) {
			d.FireTimer(c.ID, c.Generation)
		}})
		if err != nil || s.Status != bubble.Completed {
			b.Fatalf("%+v %v", s, err)
		}
		if err := bb.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOrdinaryTimerResetStop(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		timer := time.NewTimer(time.Hour)
		timer.Reset(2 * time.Hour)
		timer.Stop()
	}
}
