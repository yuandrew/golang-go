package bubble_test

import (
	"runtime/bubble"
	"testing"
	"time"
)

// A steady period is one authorization, delivery and next registration.
// Timer is the existing one-shot capability, not a substitute correctness test.
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
