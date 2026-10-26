package bubble_test

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/bubble"
	"sort"
	"testing"
	"time"
	"weak"
)

func TestResearchTicker(t *testing.T) {
	seed := uint64(0)
	fmt.Sscan(os.Getenv("TICKER_SEED"), &seed)
	var ticks []int64
	var gate chan string
	var ticker *time.Ticker
	var stoppedClosed, pending bool
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	b, _ := bubble.New(bubble.Options{StartTime: start, Seed: seed}, func() {
		gate = make(chan string, 1)
		ticker = time.NewTicker(time.Second)
		defer func() { panic("discard ran defer") }()
		for {
			switch <-gate {
			case "read":
				ticks = append(ticks, (<-ticker.C).Sub(start).Nanoseconds())
			case "probe":
				select {
				case <-ticker.C:
					pending = true
				default:
				}
			case "stop":
				ticker.Stop()
				select {
				case _, ok := <-ticker.C:
					stoppedClosed = !ok
					pending = ok
				default:
				}
			case "reset":
				ticker.Reset(2 * time.Second)
			}
		}
	})
	defer b.Discard()
	step := func(now time.Time, deliver func(*bubble.Delivery)) bubble.State {
		t.Helper()
		s, err := b.Step(bubble.Activation{Now: now, Deliver: deliver})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := step(start, nil)
	if len(s.TimerChanges) != 1 {
		t.Fatalf("registration %+v", s)
	}
	c := s.TimerChanges[0]
	fire := func(now time.Time, read bool) {
		t.Helper()
		old := c
		s = step(now, func(d *bubble.Delivery) {
			if !d.FireTimer(old.ID, old.Generation) {
				panic("rejected current")
			}
			if d.FireTimer(old.ID, old.Generation) {
				panic("duplicate accepted")
			}
			if read {
				gate <- "read"
			}
		})
		if !read {
			if len(s.TimerChanges) != 0 {
				t.Fatalf("full buffer rearmed: %+v", s)
			}
			return
		}
		if len(s.TimerChanges) != 1 {
			t.Fatalf("rearm %+v", s)
		}
		c = s.TimerChanges[0]
		if c.ID != old.ID || c.Generation != old.Generation+1 || !c.Deadline.After(now) {
			t.Fatalf("next %+v", c)
		}
	}
	// Time alone cannot make even a polled channel ready.
	step(start.Add(time.Second), func(*bubble.Delivery) { gate <- "probe" })
	if pending {
		t.Fatal("unauthorized tick")
	}
	fire(start.Add(time.Second), true)
	fire(start.Add(2*time.Second), false)
	s = step(start.Add(3*time.Second), func(*bubble.Delivery) { gate <- "read" })
	if len(s.TimerChanges) != 1 {
		t.Fatal(s)
	}
	c = s.TimerChanges[0]
	fire(start.Add(100*time.Second+500*time.Millisecond), true) // timestamp 4s, next 101s
	if c.Deadline.Sub(start) != 101*time.Second {
		t.Fatal(c)
	}
	if fmt.Sprint(ticks) != "[1000000000 2000000000 4000000000]" {
		t.Fatal(ticks)
	}
	// Stop drains pending data and leaves the channel open.
	fire(c.Deadline, false)
	old := c
	s = step(c.Deadline, func(*bubble.Delivery) { gate <- "stop" })
	if stoppedClosed || pending {
		t.Fatalf("stop closed=%v pending=%v", stoppedClosed, pending)
	}
	if len(s.TimerChanges) != 0 {
		t.Fatal(s)
	}
	s = step(c.Deadline, func(d *bubble.Delivery) {
		if d.FireTimer(old.ID, old.Generation) {
			panic("stopped fired")
		}
		d.RetireTimerRange(old.ID, old.ID)
		gate <- "reset"
	})
	if len(s.TimerChanges) != 1 {
		t.Fatal(s)
	}
	c = s.TimerChanges[0]
	if c.ID == old.ID || c.Generation != 1 || c.Deadline.Sub(old.Deadline) != 2*time.Second {
		t.Fatal(c)
	}
	step(c.Deadline, func(d *bubble.Delivery) {
		if d.FireTimer(old.ID, 999) {
			panic("retired fired")
		}
		if !d.FireTimer(c.ID, c.Generation) {
			panic("reset failed")
		}
		gate <- "read"
	})
	fmt.Println("SEMANTICS", ticks, c, stoppedClosed, pending)
}

// A general periodic sensor simulation. Inputs are ordered host authorizations.
func TestResearchReplay(t *testing.T) {
	var seed uint64
	fmt.Sscan(os.Getenv("TICKER_SEED"), &seed)
	changed := os.Getenv("TICKER_CHANGED") != ""
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	var trace []string
	var gate chan bool
	b, _ := bubble.New(bubble.Options{StartTime: start, Seed: seed}, func() {
		gate = make(chan bool, 1)
		a := time.NewTicker(time.Second)
		c := time.Tick(time.Second)
		events := make(chan string, 4)
		for worker := 0; worker < 2; worker++ {
			go func(worker int) {
				for i := 0; i < 2; i++ {
					select {
					case tm := <-a.C:
						events <- fmt.Sprint(worker, ":a:", tm.Sub(start))
					case tm := <-c:
						events <- fmt.Sprint(worker, ":b:", tm.Sub(start))
					}
					runtime.Gosched()
				}
			}(worker)
		}
		for i := 0; i < 4; i++ {
			trace = append(trace, <-events)
		}
		a.Stop()
		<-gate
	})
	defer b.Discard()
	s, e := b.Step(bubble.Activation{})
	if e != nil {
		t.Fatal(e)
	}
	active := map[bubble.TimerID]bubble.TimerChange{}
	update := func(changes []bubble.TimerChange) {
		for _, c := range changes {
			if c.Kind == bubble.TimerDisarmed {
				delete(active, c.ID)
			} else {
				active[c.ID] = c
			}
		}
	}
	update(s.TimerChanges)
	var boundaries []string
	for round := 1; round <= 3; round++ {
		now := start.Add(time.Duration(round) * time.Second)
		if round == 3 {
			now = start.Add(50 * time.Second)
		}
		tokens := make([]bubble.TimerChange, 0, len(active))
		for _, c := range active {
			tokens = append(tokens, c)
		}
		sort.Slice(tokens, func(i, j int) bool { return tokens[i].ID < tokens[j].ID })
		s, e = b.Step(bubble.Activation{Now: now, Deliver: func(d *bubble.Delivery) {
			for i, c := range tokens {
				if c.Kind != bubble.TimerArmed {
					continue
				}
				if changed && round == 1 && i == 0 {
					continue
				}
				if !d.FireTimer(c.ID, c.Generation) {
					panic("current rejected")
				}
				delete(active, c.ID)
			}
		}})
		if e != nil {
			t.Fatal(e)
		}
		boundaries = append(boundaries, fmt.Sprintf("%d/%d/%v/%v", s.Status, s.LiveGoroutines, trace, s.TimerChanges))
		update(s.TimerChanges)
	}
	data, _ := json.Marshal(boundaries)
	fmt.Println("TRANSCRIPT", string(data))
}

func TestResearchTickerFaultDiscard(t *testing.T) {
	for _, kind := range []string{"early", "future", "zero", "unknown", "retire-active", "user", "pending", "blocked"} {
		t.Run(kind, func(t *testing.T) {
			var ran, defers int
			var gate chan bool
			b, _ := bubble.New(bubble.Options{}, func() {
				defer func() { defers++ }()
				gate = make(chan bool, 1)
				tk := time.NewTicker(time.Second)
				if kind == "blocked" {
					<-tk.C
					ran++
					return
				}
				<-gate
				if kind == "user" {
					panic("user fault")
				}
				ran++
			})
			s, e := b.Step(bubble.Activation{})
			if e != nil {
				t.Fatal(e)
			}
			c := s.TimerChanges[0]
			if kind != "blocked" {
				now := c.Deadline
				if kind == "early" {
					now = now.Add(-time.Nanosecond)
				}
				s, e = b.Step(bubble.Activation{Now: now, Deliver: func(d *bubble.Delivery) {
					switch kind {
					case "future":
						d.FireTimer(c.ID, c.Generation+1)
					case "zero":
						d.FireTimer(c.ID, 0)
					case "unknown":
						d.FireTimer(c.ID+1, c.Generation)
					case "retire-active":
						d.RetireTimerRange(c.ID, c.ID)
					case "user":
						gate <- true
					default:
						d.FireTimer(c.ID, c.Generation)
					}
				}})
				if kind != "pending" && (e == nil || s.Status != bubble.Faulted) {
					t.Fatalf("fault %v %v", s, e)
				}
				if kind != "pending" {
					if _, e = b.Step(bubble.Activation{}); e == nil {
						t.Fatal("fault resumed")
					}
				}
			}
			if e = b.Discard(); e != nil {
				t.Fatal(e)
			}
			wantDefers := 0
			if kind == "user" {
				wantDefers = 1
			}
			if ran != 0 || defers != wantDefers {
				t.Fatalf("ran=%d defers=%d", ran, defers)
			}
			if e = b.Discard(); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestResearchTickerResetPending(t *testing.T) {
	var gate chan bool
	var got time.Time
	b, _ := bubble.New(bubble.Options{}, func() {
		gate = make(chan bool, 1)
		tk := time.NewTicker(time.Second)
		if cap(tk.C) != 0 || len(tk.C) != 0 {
			panic("ticker channel exposes async buffer")
		}
		<-gate
		if len(tk.C) != 0 {
			panic("pending ticker channel exposes buffer")
		}
		tk.Reset(2 * time.Second)
		select {
		case <-tk.C:
			panic("stale pending survived reset")
		default:
		}
		got = <-tk.C
		tk.Stop()
	})
	defer b.Discard()
	s, e := b.Step(bubble.Activation{})
	if e != nil {
		t.Fatal(e)
	}
	c := s.TimerChanges[0]
	s, e = b.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(c.ID, c.Generation); gate <- true }})
	if e != nil || len(s.TimerChanges) != 1 {
		t.Fatalf("reset %v %v", s, e)
	}
	fresh := s.TimerChanges[0]
	if fresh.ID != c.ID || fresh.Generation != c.Generation+1 {
		t.Fatal(fresh)
	}
	s, e = b.Step(bubble.Activation{Now: fresh.Deadline, Deliver: func(d *bubble.Delivery) {
		if d.FireTimer(c.ID, c.Generation) {
			panic("stale")
		}
		d.FireTimer(fresh.ID, fresh.Generation)
	}})
	if e != nil || s.Status != bubble.Completed || !got.Equal(fresh.Deadline) {
		t.Fatalf("%v %v %v", s, e, got)
	}
}

func TestResearchTickerCycles(t *testing.T) {
	var refs []weak.Pointer[time.Ticker]
	for i := 0; i < 200; i++ {
		var gate chan bool
		b, _ := bubble.New(bubble.Options{}, func() {
			gate = make(chan bool, 1)
			tk := time.NewTicker(time.Second)
			refs = append(refs, weak.Make(tk))
			for j := 0; j < 20; j++ {
				tk.Stop()
				tk.Reset(time.Second)
			}
			<-gate
		})
		s, e := b.Step(bubble.Activation{})
		if e != nil {
			t.Fatal(e)
		}
		if len(s.TimerChanges) != 41 {
			t.Fatal(len(s.TimerChanges))
		}
		if e = b.Discard(); e != nil {
			t.Fatal(e)
		}
	}
	researchHeap()
	for _, ref := range refs {
		if ref.Value() != nil {
			t.Fatal("ticker retained after repeated reset/stop/discard")
		}
	}
}

func researchHeap() runtime.MemStats {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func TestResearchTickerRetention(t *testing.T) {
	for _, stop := range []bool{false, true} {
		for _, n := range []int{4000, 16000} {
			before := researchHeap()
			b, _ := bubble.New(bubble.Options{}, func() {
				for i := 0; i < n; i++ {
					tk := time.NewTicker(time.Hour)
					if stop {
						tk.Stop()
					}
				}
				select {}
			})
			s, e := b.Step(bubble.Activation{})
			if e != nil {
				t.Fatal(e)
			}
			s.TimerChanges = nil
			held := researchHeap()
			if e = b.Discard(); e != nil {
				t.Fatal(e)
			}
			after := researchHeap()
			t.Logf("stop=%v n=%d heldBytes=%d heldObjects=%d afterDiscardBytes=%d allocatedBytes=%d allocations=%d", stop, n, int64(held.HeapAlloc)-int64(before.HeapAlloc), int64(held.HeapObjects)-int64(before.HeapObjects), int64(after.HeapAlloc)-int64(before.HeapAlloc), held.TotalAlloc-before.TotalAlloc, held.Mallocs-before.Mallocs)
			runtime.KeepAlive(b)
		}
	}
}

// Preserve CPU fixtures for a serialized run. No CPU measurements in this investigation.
func BenchmarkResearchTicker(b *testing.B) {
	for i := 0; i < b.N; i++ {
		bb, _ := bubble.New(bubble.Options{}, func() { tk := time.NewTicker(time.Second); <-tk.C; tk.Stop() })
		s, e := bb.Step(bubble.Activation{})
		if e != nil {
			b.Fatal(e)
		}
		c := s.TimerChanges[0]
		if _, e = bb.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(c.ID, c.Generation) }}); e != nil {
			b.Fatal(e)
		}
		bb.Close()
	}
}

func TestResearchOrdinaryTickerRetention(t *testing.T) {
	for _, n := range []int{4000, 16000} {
		before := researchHeap()
		for i := 0; i < n; i++ {
			time.NewTicker(time.Hour)
		}
		after := researchHeap()
		t.Logf("ordinary n=%d retainedBytes=%d retainedObjects=%d", n, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(after.HeapObjects)-int64(before.HeapObjects))
	}
}

func TestResearchTickerWeakReachability(t *testing.T) {
	var ordinary, owned weak.Pointer[time.Ticker]
	ordinary = weak.Make(time.NewTicker(time.Hour))
	b, _ := bubble.New(bubble.Options{}, func() { owned = weak.Make(time.NewTicker(time.Hour)); select {} })
	if _, e := b.Step(bubble.Activation{}); e != nil {
		t.Fatal(e)
	}
	researchHeap()
	if ordinary.Value() != nil {
		t.Fatal("ordinary ticker unexpectedly retained")
	}
	if owned.Value() != nil {
		t.Fatal("weak candidate retained unreachable ticker")
	}
	if e := b.Discard(); e != nil {
		t.Fatal(e)
	}
	researchHeap()
	if owned.Value() != nil {
		t.Fatal("discard retained ticker")
	}
	t.Log("ordinary and candidate collected while paused; discard remains valid")
}

func TestResearchTickerEndpoint(t *testing.T) {
	start := time.Unix(0, 1<<63-1-int64(time.Second))
	var got time.Time
	b, _ := bubble.New(bubble.Options{StartTime: start}, func() { tk := time.NewTicker(time.Second); got = <-tk.C; tk.Stop() })
	defer b.Discard()
	s, e := b.Step(bubble.Activation{})
	if e != nil {
		t.Fatal(e)
	}
	c := s.TimerChanges[0]
	s, e = b.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) {
		if !d.FireTimer(c.ID, c.Generation) {
			panic("endpoint rejected")
		}
		if d.FireTimer(c.ID, c.Generation) {
			panic("endpoint duplicate")
		}
		d.RetireTimerRange(c.ID, c.ID)
	}})
	if e != nil || s.Status != bubble.Completed || !got.Equal(c.Deadline) {
		t.Fatalf("%v %v %v", s, e, got)
	}
}

func TestResearchTickerAllocations(t *testing.T) {
	var tk *time.Ticker
	b, _ := bubble.New(bubble.Options{}, func() {
		tk = time.NewTicker(time.Second)
		for {
			<-tk.C
		}
	})
	defer b.Discard()
	s, e := b.Step(bubble.Activation{})
	if e != nil {
		t.Fatal(e)
	}
	c := s.TimerChanges[0]
	allocs := testing.AllocsPerRun(100, func() {
		s, e = b.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(c.ID, c.Generation) }})
		if e != nil {
			t.Fatal(e)
		}
		c = s.TimerChanges[0]
	})
	t.Logf("allocations per timely ticker activation: %.0f", allocs)
}

func TestResearchTickInvalidDurations(t *testing.T) {
	b, _ := bubble.New(bubble.Options{}, func() {
		if time.Tick(0) != nil || time.Tick(-1) != nil {
			panic("invalid Tick")
		}
		for _, d := range []time.Duration{0, -1} {
			func() {
				defer func() {
					if recover() == nil {
						panic("NewTicker accepted invalid period")
					}
				}()
				time.NewTicker(d)
			}()
		}
	})
	defer b.Discard()
	s, e := b.Step(bubble.Activation{})
	if e != nil || s.Status != bubble.Completed || len(s.TimerChanges) != 0 {
		t.Fatalf("%v %v", s, e)
	}
}
