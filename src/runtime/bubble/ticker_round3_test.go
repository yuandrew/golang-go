package bubble

import (
	"encoding/json"
	"fmt"
	runtimebubble "internal/runtime/bubble"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"weak"
)

func round3GC() { runtime.GC(); runtime.GC() }

func TestRound3AbandonedChannelTimers(t *testing.T) {
	var timerRef weak.Pointer[time.Timer]
	var tickerRef weak.Pointer[time.Ticker]
	b, _ := New(Options{}, func() {
		timerRef = weak.Make(time.NewTimer(time.Hour))
		tickerRef = weak.Make(time.NewTicker(time.Hour))
		select {}
	})
	defer b.Discard()
	if _, e := b.Step(Activation{}); e != nil {
		t.Fatal(e)
	}
	round3GC()
	if timerRef.Value() != nil {
		t.Fatal("abandoned one-shot channel timer retained")
	}
	if tickerRef.Value() != nil {
		t.Fatal("abandoned ticker retained")
	}
}

func TestRound3UnconsumedTickerRetires(t *testing.T) {
	b, _ := New(Options{}, func() { time.NewTicker(time.Second); select {} })
	defer b.Discard()
	s, e := b.Step(Activation{})
	if e != nil {
		t.Fatal(e)
	}
	c := s.TimerChanges[0]
	s, e = b.Step(Activation{Now: c.Deadline, Deliver: func(d *Delivery) {
		if !d.FireTimer(c.ID, c.Generation) {
			panic("current rejected")
		}
		if d.FireTimer(c.ID, c.Generation) {
			panic("duplicate accepted")
		}
		d.RetireTimerRange(c.ID, c.ID)
	}})
	if e != nil || len(s.TimerChanges) != 0 {
		t.Fatalf("unconsumed rearmed: %v %v", s, e)
	}
}

func TestRound3NativeOracle(t *testing.T) {
	var native []time.Duration
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for _, delta := range []time.Duration{time.Second, 2 * time.Second, 97500 * time.Millisecond, 500 * time.Millisecond} {
			time.Sleep(delta)
			native = append(native, (<-tk.C).Sub(start))
		}
	})
	var actual []time.Duration
	var command chan bool
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	b, _ := New(Options{StartTime: start}, func() {
		command = make(chan bool, 1)
		tk := time.NewTicker(time.Second)
		for {
			<-command
			actual = append(actual, (<-tk.C).Sub(start))
		}
	})
	defer b.Discard()
	s, e := b.Step(Activation{})
	if e != nil {
		t.Fatal(e)
	}
	current := s.TimerChanges[0]
	step := func(now time.Duration, fire, read bool) {
		t.Helper()
		s, e = b.Step(Activation{Now: start.Add(now), Deliver: func(d *Delivery) {
			if fire && !d.FireTimer(current.ID, current.Generation) {
				panic("current rejected")
			}
			if read {
				command <- true
			}
		}})
		if e != nil {
			t.Fatal(e)
		}
		if len(s.TimerChanges) > 0 {
			current = s.TimerChanges[len(s.TimerChanges)-1]
		}
	}
	step(time.Second, true, true)
	step(2*time.Second, true, false)
	step(3*time.Second, false, true)
	step(100500*time.Millisecond, true, true)
	step(101*time.Second, true, true)
	if !reflect.DeepEqual(native, actual) {
		t.Fatalf("native=%v actual=%v", native, actual)
	}
	t.Logf("native and host-controlled slow-receiver timestamps: %v", actual)
}

func TestRound3CollectedCallbackStillRuns(t *testing.T) {
	var callback weak.Pointer[time.Timer]
	var channelTimer weak.Pointer[time.Timer]
	var ran bool
	b, _ := New(Options{}, func() {
		done := make(chan bool, 1)
		callback = weak.Make(time.AfterFunc(time.Second, func() { done <- true }))
		channelTimer = weak.Make(time.NewTimer(time.Second))
		ran = <-done
	})
	defer b.Discard()
	s, e := b.Step(Activation{})
	if e != nil {
		t.Fatal(e)
	}
	round3GC()
	if callback.Value() == nil || channelTimer.Value() != nil {
		t.Fatal("callback/channel lifetime distinction lost")
	}
	first, second := s.TimerChanges[0], s.TimerChanges[1]
	s, e = b.Step(Activation{Now: first.Deadline, Deliver: func(d *Delivery) {
		if !d.FireTimer(first.ID, first.Generation) || !d.FireTimer(second.ID, second.Generation) {
			panic("authorization rejected")
		}
	}})
	if e != nil || s.Status != Completed || !ran {
		t.Fatalf("%v %v ran=%v", s, e, ran)
	}
	if e = b.Close(); e != nil {
		t.Fatal(e)
	}
	round3GC()
	if callback.Value() != nil {
		t.Fatal("completed callback timer retained")
	}
}

func TestRound3Replay(t *testing.T) {
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)
	round3GC()
	mode := os.Getenv("TICKER_GC")
	var seed uint64
	fmt.Sscan(os.Getenv("TICKER_SEED"), &seed)
	changed := os.Getenv("TICKER_CHANGED") == "1"
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	var command chan string
	var values []string
	var abandoned []TimerChange
	var timerRef weak.Pointer[time.Timer]
	var tickerRef weak.Pointer[time.Ticker]
	b, _ := New(Options{StartTime: start, Seed: seed}, func() {
		command = make(chan string, 1)
		tk := time.NewTicker(time.Second)
		timerRef = weak.Make(time.NewTimer(time.Second))
		tickerRef = weak.Make(time.NewTicker(time.Second))
		for {
			switch <-command {
			case "read":
				values = append(values, (<-tk.C).Sub(start).String())
			case "stop":
				tk.Stop()
				select {
				case _, ok := <-tk.C:
					panic(fmt.Sprint("stopped channel ready, open=", ok))
				default:
				}
			case "reset":
				period := 2 * time.Second
				if changed {
					period = 3 * time.Second
				}
				tk.Reset(period)
			case "probe":
				select {
				case <-tk.C:
					panic("unauthorized tick")
				default:
				}
			}
		}
	})
	defer b.Discard()
	var trace []string
	step := func(now time.Time, token *TimerChange, cmd string, retire bool) State {
		t.Helper()
		if mode == "early" {
			round3GC()
		}
		s, e := b.Step(Activation{Now: now, Deliver: func(d *Delivery) {
			if token != nil {
				if !d.FireTimer(token.ID, token.Generation) {
					panic("current rejected")
				}
				if d.FireTimer(token.ID, token.Generation) {
					panic("duplicate accepted")
				}
				if retire {
					d.RetireTimerRange(token.ID, token.ID)
				}
			}
			if cmd == "abandoned" {
				for _, c := range abandoned {
					if !d.FireTimer(c.ID, c.Generation) {
						panic("abandoned rejected")
					}
					if d.FireTimer(c.ID, c.Generation) {
						panic("abandoned duplicate")
					}
				}
				d.RetireTimerRange(abandoned[0].ID, abandoned[len(abandoned)-1].ID)
			} else if cmd != "" {
				command <- cmd
			}
		}})
		if e != nil {
			t.Fatal(e)
		}
		trace = append(trace, fmt.Sprintf("%d/%d/%v/%v", s.Status, s.LiveGoroutines, values, s.TimerChanges))
		if mode == "late" && len(trace) >= 4 {
			round3GC()
		}
		return s
	}
	s := step(start, nil, "", false)
	c := s.TimerChanges[0]
	abandoned = s.TimerChanges[1:]
	step(c.Deadline, nil, "probe", false)
	step(c.Deadline, nil, "abandoned", false)
	if mode == "early" && (timerRef.Value() != nil || tickerRef.Value() != nil) {
		t.Fatal("GC failed to clear abandoned payloads")
	}
	s = step(c.Deadline, &c, "read", false)
	c = s.TimerChanges[0]
	s = step(c.Deadline, &c, "", true)
	if len(s.TimerChanges) != 0 {
		t.Fatal("full buffer should stop host registrations")
	}
	s = step(start.Add(100500*time.Millisecond), nil, "read", false)
	if len(s.TimerChanges) != 1 {
		t.Fatal(s)
	}
	c = s.TimerChanges[0]
	if c.ID <= 3 || c.Deadline.Sub(start) != 101*time.Second {
		t.Fatal(c)
	}
	s = step(c.Deadline, &c, "", true)
	if len(s.TimerChanges) != 0 {
		t.Fatal(s)
	}
	step(c.Deadline, nil, "stop", false)
	s = step(c.Deadline, nil, "reset", false)
	c = s.TimerChanges[0]
	step(c.Deadline, &c, "read", false)
	data, _ := json.Marshal(trace)
	fmt.Println("ROUND3", string(data))
}

func TestRound3ReclamationWithoutGCInputs(t *testing.T) {
	const count = 1000
	const rounds = 20
	var command chan bool
	var refs []weak.Pointer[time.Ticker]
	defers := 0
	b, _ := New(Options{}, func() {
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
	s, e := b.Step(Activation{})
	if e != nil {
		t.Fatal(e)
	}
	tokens := s.TimerChanges[1:]
	for round := 0; round < rounds; round++ {
		round3GC()
		for _, ref := range refs {
			if ref.Value() != nil {
				t.Fatal("abandoned payload retained")
			}
		}
		refs = nil
		s, e = b.Step(Activation{Now: tokens[0].Deadline, Deliver: func(d *Delivery) {
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
		s, e = b.Step(Activation{Deliver: func(d *Delivery) {
			d.RetireTimerRange(tokens[0].ID, tokens[len(tokens)-1].ID)
			command <- true
		}})
		if e != nil {
			t.Fatal(e)
		}
		tokens = s.TimerChanges
		stats := runtimebubble.TickerResearchStats(b.handle)
		want := count + 1
		if round == rounds-1 {
			want = 1
		}
		if stats[0] != want || stats[2] != 0 || stats[3] != 1 {
			t.Fatalf("round=%d stats=%v", round, stats)
		}
		round3GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		t.Logf("round=%d registry=%v heap=%d objects=%d gcSys=%d", round, stats, memory.HeapAlloc, memory.HeapObjects, memory.GCSys)
	}
	if e = b.Discard(); e != nil {
		t.Fatal(e)
	}
	if defers != 0 {
		t.Fatal("discard ran defer")
	}
}

func TestRound3WaitingConsumersAndGC(t *testing.T) {
	const count = 128
	const rounds = 8
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	seen := 0
	b, _ := New(Options{StartTime: start}, func() {
		results := make(chan bool, count)
		for range count {
			ch := time.Tick(time.Second)
			go func() {
				for round := 1; round <= rounds; round++ {
					if tm := <-ch; !tm.Equal(start.Add(time.Duration(round) * time.Second)) {
						panic("wrong timestamp")
					}
				}
				results <- true
			}()
		}
		for range count {
			<-results
			seen++
		}
	})
	defer b.Discard()
	s, e := b.Step(Activation{})
	if e != nil {
		t.Fatal(e)
	}
	var stop atomic.Bool
	var gcs atomic.Int32
	done := make(chan bool)
	go func() {
		defer close(done)
		for !stop.Load() {
			runtime.GC()
			gcs.Add(1)
		}
	}()
	defer func() { stop.Store(true); <-done }()
	for round := 1; round <= rounds; round++ {
		tokens := s.TimerChanges
		if len(tokens) != count {
			t.Fatalf("round %d registrations=%d", round, len(tokens))
		}
		s, e = b.Step(Activation{Now: start.Add(time.Duration(round) * time.Second), Deliver: func(d *Delivery) {
			for _, c := range tokens {
				if !d.FireTimer(c.ID, c.Generation) {
					panic("current rejected")
				}
			}
		}})
		if e != nil {
			t.Fatal(e)
		}
	}
	if s.Status != Completed || seen != count {
		t.Fatalf("%v seen=%d", s, seen)
	}
	t.Logf("retained Tick deliveries=%d GC cycles=%d", count*rounds, gcs.Load())
}

func TestRound3PollOther(t *testing.T) {
	var seed uint64
	fmt.Sscan(os.Getenv("TICKER_SEED"), &seed)
	var transcript []string
	for _, kind := range []string{"select", "reflect"} {
		start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		var command chan bool
		var values []string
		b, _ := New(Options{StartTime: start, Seed: seed}, func() {
			command = make(chan bool, 1)
			other := make(chan bool, 1)
			other <- true
			tk := time.NewTicker(time.Second)
			<-command
			if kind == "select" {
				select {
				case tm := <-tk.C:
					values = append(values, "tick:"+tm.Sub(start).String())
				case <-other:
					values = append(values, "other")
				}
			} else {
				chosen, value, _ := reflect.Select([]reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tk.C)}, {Dir: reflect.SelectRecv, Chan: reflect.ValueOf(other)}})
				if chosen == 0 {
					values = append(values, "tick:"+value.Interface().(time.Time).Sub(start).String())
				} else {
					values = append(values, "other")
				}
			}
			<-command
			values = append(values, (<-tk.C).Sub(start).String())
			select {}
		})
		s, e := b.Step(Activation{})
		if e != nil {
			t.Fatal(e)
		}
		c := s.TimerChanges[0]
		s, e = b.Step(Activation{Now: c.Deadline, Deliver: func(d *Delivery) { d.FireTimer(c.ID, c.Generation); command <- true }})
		if e != nil || len(s.TimerChanges) != 1 {
			t.Fatalf("poll %v %v", s, e)
		}
		c = s.TimerChanges[0]
		s, e = b.Step(Activation{Now: c.Deadline, Deliver: func(d *Delivery) { d.FireTimer(c.ID, c.Generation) }})
		if e != nil || len(s.TimerChanges) != 0 {
			t.Fatalf("pending %v %v", s, e)
		}
		s, e = b.Step(Activation{Now: start.Add(100 * time.Second), Deliver: func(*Delivery) { command <- true }})
		if e != nil || len(s.TimerChanges) != 1 || s.TimerChanges[0].Deadline.Sub(start) != 101*time.Second {
			t.Fatalf("phase %v %v", s, e)
		}
		want := "1s"
		if values[0] != "other" {
			want = "2s"
		}
		if values[1] != want {
			t.Fatal(values)
		}
		transcript = append(transcript, fmt.Sprint(kind, values, s.TimerChanges))
		if e = b.Discard(); e != nil {
			t.Fatal(e)
		}
	}
	data, _ := json.Marshal(transcript)
	fmt.Println("POLL", string(data))
}

func TestRound3SharedTicker(t *testing.T) {
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	var values []time.Duration
	b, _ := New(Options{StartTime: start}, func() {
		tk := time.NewTicker(time.Second)
		events := make(chan time.Time, 8)
		for range 4 {
			go func() {
				for range 2 {
					events <- <-tk.C
				}
			}()
		}
		for range 8 {
			values = append(values, (<-events).Sub(start))
		}
		tk.Stop()
	})
	defer b.Discard()
	s, e := b.Step(Activation{})
	if e != nil {
		t.Fatal(e)
	}
	for round := 1; round <= 8; round++ {
		c := s.TimerChanges[0]
		s, e = b.Step(Activation{Now: c.Deadline, Deliver: func(d *Delivery) {
			if !d.FireTimer(c.ID, c.Generation) {
				panic("current rejected")
			}
		}})
		if e != nil {
			t.Fatal(e)
		}
	}
	if s.Status != Completed || len(values) != 8 {
		t.Fatalf("%v %v", s, values)
	}
	for i, v := range values {
		if v != time.Duration(i+1)*time.Second {
			t.Fatal(values)
		}
	}
}

func TestRound3IdleAtClockLimit(t *testing.T) {
	limit := time.Unix(0, 1<<63-1)
	var gate chan bool
	var got time.Time
	b, _ := New(Options{StartTime: limit.Add(-2 * time.Second)}, func() {
		gate = make(chan bool, 1)
		tk := time.NewTicker(time.Second)
		<-gate
		got = <-tk.C
		select {
		case <-tk.C:
			panic("clock limit created an unauthorized tick")
		default:
		}
		tk.Stop()
	})
	defer b.Discard()
	s, e := b.Step(Activation{})
	if e != nil {
		t.Fatal(e)
	}
	c := s.TimerChanges[0]
	s, e = b.Step(Activation{Now: c.Deadline, Deliver: func(d *Delivery) { d.FireTimer(c.ID, c.Generation) }})
	if e != nil || len(s.TimerChanges) != 0 {
		t.Fatalf("%v %v", s, e)
	}
	s, e = b.Step(Activation{Now: limit, Deliver: func(*Delivery) { gate <- true }})
	if e != nil || s.Status != Completed || len(s.TimerChanges) != 0 || !got.Equal(c.Deadline) {
		t.Fatalf("%v %v %v", s, e, got)
	}
}

func TestRound3RetainedAfterChannel(t *testing.T) {
	var got time.Time
	b, _ := New(Options{}, func() { ch := time.After(time.Second); got = <-ch })
	defer b.Discard()
	s, e := b.Step(Activation{})
	if e != nil {
		t.Fatal(e)
	}
	c := s.TimerChanges[0]
	round3GC()
	s, e = b.Step(Activation{Now: c.Deadline, Deliver: func(d *Delivery) {
		if !d.FireTimer(c.ID, c.Generation) {
			panic("retained After rejected")
		}
	}})
	if e != nil || s.Status != Completed || !got.Equal(c.Deadline) {
		t.Fatalf("%v %v %v", s, e, got)
	}
}
