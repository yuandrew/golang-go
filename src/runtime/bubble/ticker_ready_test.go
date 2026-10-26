package bubble_test

import (
	"fmt"
	"runtime/bubble"
	"sort"
	"sync"
	"testing"
	"time"
)

// ExampleTicker uses a generic host event queue to run two periodic polls.
// TimerArmed entries are individual attempts, including rearmed tickers.
func ExampleTicker() {
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	var samples [2][]time.Duration
	b, err := bubble.New(bubble.Options{StartTime: start}, func() {
		var done sync.WaitGroup
		for worker, period := range []time.Duration{time.Second, 2 * time.Second} {
			done.Go(func() {
				ticker := time.NewTicker(period)
				defer ticker.Stop()
				for range 3 {
					samples[worker] = append(samples[worker], (<-ticker.C).Sub(start))
				}
			})
		}
		done.Wait()
	})
	if err != nil {
		panic(err)
	}
	pending := make(map[bubble.TimerID]bubble.TimerChange)
	inactive := make(map[bubble.TimerID]bool)
	state, err := b.Step(bubble.Activation{})
	for err == nil {
		// FireTimer consumes its token before these ordered changes are applied.
		for _, change := range state.TimerChanges {
			if change.Kind == bubble.TimerArmed {
				pending[change.ID] = change
				delete(inactive, change.ID)
			} else {
				delete(pending, change.ID)
				inactive[change.ID] = true
			}
		}
		if state.Status == bubble.Completed {
			break
		}
		if state.Status != bubble.Quiescent || len(pending) == 0 {
			panic("polling did not register its next attempt")
		}
		events := make([]bubble.TimerChange, 0, len(pending))
		for _, event := range pending {
			events = append(events, event)
		}
		sort.Slice(events, func(i, j int) bool {
			if events[i].Deadline.Equal(events[j].Deadline) {
				return events[i].ID < events[j].ID
			}
			return events[i].Deadline.Before(events[j].Deadline)
		})
		next := events[0]
		// Retirement is decided from the previous boundary, after all changes.
		retiring := make([]bubble.TimerID, 0, len(inactive))
		for id := range inactive {
			retiring = append(retiring, id)
		}
		sort.Slice(retiring, func(i, j int) bool { return retiring[i] < retiring[j] })
		state, err = b.Step(bubble.Activation{Now: next.Deadline, Deliver: func(d *bubble.Delivery) {
			for _, id := range retiring {
				d.RetireTimerRange(id, id)
				delete(inactive, id)
			}
			if !d.FireTimer(next.ID, next.Generation) {
				panic("current attempt rejected")
			}
			delete(pending, next.ID)
			inactive[next.ID] = true
		}})
	}
	if err != nil {
		panic(err)
	}
	if err := b.Close(); err != nil {
		panic(err)
	}
	fmt.Println("fast", samples[0])
	fmt.Println("slow", samples[1])
	// Output:
	// fast [1s 2s 3s]
	// slow [2s 4s 6s]
}

func TestTickerChannelGuarantees(t *testing.T) {
	var gate chan bool
	var ticker *time.Ticker
	b, err := bubble.New(bubble.Options{}, func() {
		gate = make(chan bool, 1)
		ticker = time.NewTicker(time.Second)
		<-gate
		if len(ticker.C) != 0 || cap(ticker.C) != 0 {
			panic("timer channel exposed its buffer")
		}
		ticker.Stop()
		ticker.Stop()
		select {
		case _, ok := <-ticker.C:
			if !ok {
				panic("Stop closed channel")
			}
			panic("Stop left stale value")
		default:
		}
		ticker.Reset(2 * time.Second)
		tick := <-ticker.C
		if !tick.Equal(time.Now()) {
			panic("reset timestamp changed")
		}
		ticker.Stop()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Discard()
	state, err := b.Step(bubble.Activation{})
	if err != nil {
		t.Fatal(err)
	}
	old := state.TimerChanges[0]
	state, err = b.Step(bubble.Activation{Now: old.Deadline, Deliver: func(d *bubble.Delivery) {
		if !d.FireTimer(old.ID, old.Generation) {
			panic("first rejected")
		}
	}})
	if err != nil || len(state.TimerChanges) != 0 {
		t.Fatalf("buffered %+v %v", state, err)
	}
	state, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) { d.RetireTimerRange(old.ID, old.ID); gate <- true }})
	if err != nil || len(state.TimerChanges) != 1 {
		t.Fatalf("reset %+v %v", state, err)
	}
	fresh := state.TimerChanges[0]
	if fresh.ID == old.ID {
		t.Fatal("acknowledged ID reused")
	}
	state, err = b.Step(bubble.Activation{Now: fresh.Deadline, Deliver: func(d *bubble.Delivery) {
		if d.FireTimer(old.ID, old.Generation) {
			panic("stale accepted")
		}
		if !d.FireTimer(fresh.ID, fresh.Generation) {
			panic("reset rejected")
		}
	}})
	if err != nil || state.Status != bubble.Completed {
		t.Fatalf("final %+v %v", state, err)
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
