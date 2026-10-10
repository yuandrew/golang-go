// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"runtime/bubble"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newBubble(t *testing.T, f func()) *bubble.Bubble {
	t.Helper()
	b, err := bubble.New(bubble.Options{StartTime: start, Seed: 17}, f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close: %v; %s", err, b.StackTrace())
		}
	})
	return b
}

func step(t *testing.T, b *bubble.Bubble, a bubble.Activation) bubble.State {
	t.Helper()
	s, err := b.Step(a)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func requireCompleted(t *testing.T, s bubble.State) {
	t.Helper()
	if s.Status != bubble.Completed || !s.RootDone || s.LiveGoroutines != 0 {
		t.Fatalf("want completed root and zero live goroutines; got %+v", s)
	}
}

func TestFIFOAndInheritedOwnership(t *testing.T) {
	var log []int
	b := newBubble(t, func() {
		c := make(chan int)
		events := make(chan int, 4)
		go func() {
			events <- 1
			go func() { events <- 3; c <- 3 }()
			events <- 2
		}()
		events <- 0
		<-c
		for i := 0; i < 4; i++ {
			log = append(log, <-events)
		}
	})
	if len(log) != 0 {
		t.Fatal("New ran the root")
	}
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if !reflect.DeepEqual(log, []int{0, 1, 2, 3}) {
		t.Fatalf("FIFO/ownership: %v", log)
	}
}

func TestGosched(t *testing.T) {
	var log []int
	b := newBubble(t, func() {
		events := make(chan int, 3)
		go func() { events <- 1 }()
		events <- 0
		runtime.Gosched()
		events <- 2
		for i := 0; i < 3; i++ {
			log = append(log, <-events)
		}
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if !reflect.DeepEqual(log, []int{0, 1, 2}) {
		t.Fatalf("Gosched order: %v", log)
	}
}

func TestOneLogicalOwner(t *testing.T) {
	originalProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(originalProcs)
	var active atomic.Int32
	var overlap atomic.Bool
	b := newBubble(t, func() {
		done := make(chan struct{}, 8)
		for i := 0; i < 8; i++ {
			go func() {
				if active.Add(1) != 1 {
					overlap.Store(true)
				}
				// A physical preemption must preserve this goroutine's logical
				// turn, even while another processor can run its peers.
				for j := 0; j < 1_000_000; j++ {
					if active.Load() != 1 {
						overlap.Store(true)
					}
				}
				active.Add(-1)
				done <- struct{}{}
			}()
		}
		for i := 0; i < 8; i++ {
			<-done
		}
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if overlap.Load() {
		t.Fatal("bubble peers overlapped a logical turn")
	}
}

func TestSelectReproducible(t *testing.T) {
	run := func(seed uint64) []int {
		var log []int
		b, err := bubble.New(bubble.Options{Seed: seed}, func() {
			a, b := make(chan int, 1), make(chan int, 1)
			for i := 0; i < 100; i++ {
				a <- 1
				b <- 2
				select {
				case n := <-a:
					log = append(log, n)
					<-b
				case n := <-b:
					log = append(log, n)
					<-a
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		requireCompleted(t, step(t, b, bubble.Activation{}))
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		return log
	}
	want := run(0)
	originalProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(originalProcs)
	for _, procs := range []int{1, 2, 4} {
		runtime.GOMAXPROCS(procs)
		for i := 0; i < 3; i++ {
			if got := run(0); !reflect.DeepEqual(got, want) {
				t.Fatalf("seed zero changed choices with GOMAXPROCS=%d: %v != %v", procs, got, want)
			}
		}
	}
	if reflect.DeepEqual(run(1), want) {
		t.Fatal("different seeds produced identical choices")
	}
}

func TestReflectSelectReproducible(t *testing.T) {
	run := func() []int {
		var log []int
		b := newBubble(t, func() {
			channels := []chan int{make(chan int, 1), make(chan int, 1), make(chan int, 1)}
			cases := make([]reflect.SelectCase, len(channels))
			for i, c := range channels {
				cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c)}
			}
			for i := 0; i < 30; i++ {
				for j, c := range channels {
					c <- j
				}
				chosen, _, _ := reflect.Select(cases)
				log = append(log, chosen)
				for j, c := range channels {
					if j != chosen {
						<-c
					}
				}
			}
		})
		requireCompleted(t, step(t, b, bubble.Activation{}))
		return log
	}
	want := run()
	for i := 0; i < 5; i++ {
		if got := run(); !reflect.DeepEqual(got, want) {
			t.Fatalf("reflect.Select choices changed: %v != %v", got, want)
		}
	}
}

func TestChannelCloseWakeOrder(t *testing.T) {
	var log []int
	b := newBubble(t, func() {
		closed := make(chan struct{})
		registered := make(chan struct{}, 10)
		events := make(chan int, 10)
		for i := 0; i < 10; i++ {
			go func() {
				registered <- struct{}{}
				<-closed
				events <- i
			}()
		}
		for i := 0; i < 10; i++ {
			<-registered
		}
		close(closed)
		for i := 0; i < 10; i++ {
			log = append(log, <-events)
		}
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if !reflect.DeepEqual(log, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("channel close wake order: %v", log)
	}
}

func TestQuiescenceClockAndDelivery(t *testing.T) {
	var inbox chan int
	var times []time.Time
	var result int
	b := newBubble(t, func() {
		inbox = make(chan int, 1)
		times = append(times, time.Now())
		result = <-inbox
		times = append(times, time.Now())
	})
	s := step(t, b, bubble.Activation{})
	if s.Status != bubble.Quiescent || s.RootDone || s.LiveGoroutines != 1 {
		t.Fatalf("quiescent state: %+v", s)
	}
	if trace := b.StackTrace(); !strings.Contains(trace, "chan receive") {
		t.Errorf("missing channel wait stack: %s", trace)
	}
	later := start.Add(2 * time.Hour)
	var escaped *bubble.Delivery
	requireCompleted(t, step(t, b, bubble.Activation{Now: later, Deliver: func(d *bubble.Delivery) {
		escaped = d
		inbox <- 42
	}}))
	if result != 42 || len(times) != 2 || !times[0].Equal(start) || !times[1].Equal(later) {
		t.Fatalf("result=%d times=%v", result, times)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expired delivery accepted FireTimer")
			}
		}()
		escaped.FireTimer(1, 1)
	}()
}

func TestRootDoneWithLiveChild(t *testing.T) {
	var done chan struct{}
	var observed time.Time
	b := newBubble(t, func() {
		done = make(chan struct{})
		go func() { <-done; observed = time.Now() }()
	})
	s := step(t, b, bubble.Activation{})
	if !s.RootDone || s.LiveGoroutines != 1 || s.Status != bubble.Quiescent {
		t.Fatalf("root/child boundary: %+v", s)
	}
	later := start.Add(time.Hour)
	requireCompleted(t, step(t, b, bubble.Activation{Now: later, Deliver: func(*bubble.Delivery) { close(done) }}))
	if !observed.Equal(later) {
		t.Fatalf("surviving child clock: %v", observed)
	}
}

func TestIndependentClocks(t *testing.T) {
	var ca, cb chan struct{}
	var ta, tb time.Time
	a := newBubble(t, func() { ca = make(chan struct{}); <-ca; ta = time.Now() })
	b := newBubble(t, func() { cb = make(chan struct{}); <-cb; tb = time.Now() })
	step(t, a, bubble.Activation{})
	step(t, b, bubble.Activation{})
	requireCompleted(t, step(t, a, bubble.Activation{Now: start.Add(time.Hour), Deliver: func(*bubble.Delivery) { close(ca) }}))
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(*bubble.Delivery) { close(cb) }}))
	if !ta.Equal(start.Add(time.Hour)) || !tb.Equal(start) {
		t.Fatalf("clocks coupled: a=%v b=%v", ta, tb)
	}
}

func TestConcurrentIndependentBubbles(t *testing.T) {
	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var observed int
			b, err := bubble.New(bubble.Options{}, func() {
				c := make(chan int)
				go func() { c <- 42 }()
				observed = <-c
			})
			if err != nil {
				errs <- err
				return
			}
			s, err := b.Step(bubble.Activation{})
			if err != nil {
				errs <- err
			} else if s.Status != bubble.Completed || observed != 42 {
				t.Errorf("independent execution: %+v observed=%d", s, observed)
			}
			if err := b.Close(); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestDeliveryBatchBeforeUserExecution(t *testing.T) {
	var inbox, events chan int
	var done chan struct{}
	var log []int
	b := newBubble(t, func() {
		inbox = make(chan int, 2)
		events = make(chan int, 4)
		done = make(chan struct{})
		events <- <-inbox
		events <- <-inbox
		<-done
		for i := 0; i < 4; i++ {
			log = append(log, <-events)
		}
	})
	step(t, b, bubble.Activation{})
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(*bubble.Delivery) {
		inbox <- 1
		go func() { events <- 3; close(done) }()
		inbox <- 2
		events <- 0
	}}))
	if !reflect.DeepEqual(log, []int{0, 1, 2, 3}) {
		t.Fatalf("delivery batch order: %v", log)
	}
}

func TestValidation(t *testing.T) {
	for _, opts := range []bubble.Options{
		{Protocol: "unknown"},
		{StartTime: time.Unix(0, 0)},
		{StartTime: time.Unix(-1, 0)},
		{StartTime: time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		if b, err := bubble.New(opts, func() {}); err == nil {
			b.Close()
			t.Errorf("accepted options %+v", opts)
		}
	}
	if _, err := bubble.New(bubble.Options{}, nil); err == nil {
		t.Error("accepted nil root")
	}
	var gate chan struct{}
	b := newBubble(t, func() { gate = make(chan struct{}); <-gate })
	called := false
	if _, err := b.Step(bubble.Activation{Now: start.Add(time.Second), Deliver: func(*bubble.Delivery) { called = true }}); err == nil || called {
		t.Fatal("invalid first activation mutated execution")
	}
	step(t, b, bubble.Activation{Now: start.In(time.FixedZone("other", 3600))})
	if _, err := b.Step(bubble.Activation{Now: start.Add(-time.Second)}); err == nil {
		t.Fatal("accepted backward time")
	}
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(*bubble.Delivery) { close(gate) }}))
	if _, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { called = true }}); err == nil || called {
		t.Fatal("completed bubble accepted activation")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close not idempotent: %v", err)
	}
	if _, err := b.Step(bubble.Activation{}); err == nil {
		t.Error("closed bubble accepted activation")
	}
}

func TestCloseNeverStarted(t *testing.T) {
	called := false
	b := newBubble(t, func() { called = true })
	if err := b.Close(); err != nil || called {
		t.Fatalf("Close: %v called=%v", err, called)
	}
}

func TestControllerReentry(t *testing.T) {
	var b *bubble.Bubble
	var nestedErr, stepErr, closeErr error
	b = newBubble(t, func() {
		_, nestedErr = bubble.New(bubble.Options{}, func() {})
		_, stepErr = b.Step(bubble.Activation{})
		closeErr = b.Close()
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if nestedErr == nil || stepErr == nil || closeErr == nil {
		t.Fatalf("controller reentry accepted: New=%v Step=%v Close=%v", nestedErr, stepErr, closeErr)
	}
}

func TestConcurrentControllerRejected(t *testing.T) {
	// Atomic flags belong to the test harness, solely to hold a Step open while
	// probing controller validation. They are not recorded application inputs.
	var entered, release atomic.Bool
	b := newBubble(t, func() {})
	done := make(chan error, 1)
	go func() {
		_, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) {
			entered.Store(true)
			for !release.Load() {
			}
		}})
		done <- err
	}()
	for !entered.Load() {
		runtime.Gosched()
	}
	_, stepErr := b.Step(bubble.Activation{})
	closeErr := b.Close()
	trace := b.StackTrace()
	release.Store(true)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if stepErr == nil || closeErr == nil || !strings.Contains(trace, "concurrent") {
		t.Fatalf("concurrent calls accepted: Step=%v Close=%v StackTrace=%q", stepErr, closeErr, trace)
	}
}

func TestTimeBoundsAndDefault(t *testing.T) {
	for _, initial := range []time.Time{time.Unix(0, 1), time.Unix(0, 1<<63-1), {}} {
		var observed time.Time
		b, err := bubble.New(bubble.Options{StartTime: initial}, func() { observed = time.Now() })
		if err != nil {
			t.Fatal(err)
		}
		want := initial
		if initial.IsZero() {
			want = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		}
		requireCompleted(t, step(t, b, bubble.Activation{Now: want}))
		if !observed.Equal(want) {
			t.Errorf("clock = %v, want %v", observed, want)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTimerRequiresAuthorization(t *testing.T) {
	var received time.Time
	b := newBubble(t, func() { received = <-time.NewTimer(time.Hour).C })
	s := step(t, b, bubble.Activation{})
	if s.Status != bubble.Quiescent || len(s.TimerChanges) != 1 {
		t.Fatalf("timer registration: %+v", s)
	}
	arm := s.TimerChanges[0]
	if arm.Kind != bubble.TimerArmed || !arm.Deadline.Equal(start.Add(time.Hour)) {
		t.Fatalf("armed timer: %+v", arm)
	}
	later := start.Add(2 * time.Hour)
	s = step(t, b, bubble.Activation{Now: later})
	if s.Status != bubble.Quiescent || !received.IsZero() || len(s.TimerChanges) != 0 {
		t.Fatalf("clock advancement authorized timer: %+v received=%v", s, received)
	}
	var first, duplicate bool
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
		first = d.FireTimer(arm.ID, arm.Generation)
		duplicate = d.FireTimer(arm.ID, arm.Generation)
	}}))
	if !first || duplicate || !received.Equal(arm.Deadline) {
		t.Fatalf("authorization first=%v duplicate=%v received=%v", first, duplicate, received)
	}
}

func TestSleep(t *testing.T) {
	var woke time.Time
	b := newBubble(t, func() {
		time.Sleep(0)
		time.Sleep(-time.Second)
		time.Sleep(time.Minute)
		woke = time.Now()
	})
	s := step(t, b, bubble.Activation{})
	if len(s.TimerChanges) != 1 {
		t.Fatalf("Sleep timer changes: %+v", s)
	}
	arm := s.TimerChanges[0]
	requireCompleted(t, step(t, b, bubble.Activation{Now: arm.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(arm.ID, arm.Generation) }}))
	if !woke.Equal(start.Add(time.Minute)) {
		t.Fatalf("Sleep woke at %v", woke)
	}
}

func TestTimerResetStopAndStaleAuthorization(t *testing.T) {
	var timer *time.Timer
	var gate chan struct{}
	var reset, stopped bool
	b := newBubble(t, func() {
		timer = time.NewTimer(time.Minute)
		gate = make(chan struct{})
		<-gate
	})
	s := step(t, b, bubble.Activation{})
	if len(s.TimerChanges) != 1 {
		t.Fatalf("initial timer: %+v", s)
	}
	original := s.TimerChanges[0]
	s = step(t, b, bubble.Activation{Deliver: func(*bubble.Delivery) { reset = timer.Reset(2 * time.Minute) }})
	if !reset || len(s.TimerChanges) != 2 || s.TimerChanges[0].Kind != bubble.TimerDisarmed || s.TimerChanges[1].Kind != bubble.TimerArmed {
		t.Fatalf("Reset returned %v, changes %+v", reset, s.TimerChanges)
	}
	replacement := s.TimerChanges[1]
	if replacement.ID != original.ID || replacement.Generation <= original.Generation || !replacement.Deadline.Equal(start.Add(2*time.Minute)) {
		t.Fatalf("replacement timer: %+v original=%+v", replacement, original)
	}
	var stale bool
	s = step(t, b, bubble.Activation{Now: replacement.Deadline, Deliver: func(d *bubble.Delivery) {
		stale = d.FireTimer(original.ID, original.Generation)
		stopped = timer.Stop()
	}})
	if stale || !stopped || len(s.TimerChanges) != 1 || s.TimerChanges[0].Kind != bubble.TimerDisarmed {
		t.Fatalf("Stop/stale: stale=%v stopped=%v changes=%+v", stale, stopped, s.TimerChanges)
	}
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
		stale = d.FireTimer(replacement.ID, replacement.Generation)
		close(gate)
	}}))
	if stale {
		t.Fatal("stopped generation authorized")
	}
}

func TestAuthorizedTimerStopSuppressesReceive(t *testing.T) {
	var timer *time.Timer
	var gate chan struct{}
	var stopped, received bool
	b := newBubble(t, func() {
		timer = time.NewTimer(time.Minute)
		gate = make(chan struct{})
		<-gate
		select {
		case <-timer.C:
			received = true
		default:
		}
	})
	s := step(t, b, bubble.Activation{})
	if len(s.TimerChanges) != 1 {
		t.Fatalf("timer changes: %+v", s)
	}
	arm := s.TimerChanges[0]
	requireCompleted(t, step(t, b, bubble.Activation{Now: arm.Deadline, Deliver: func(d *bubble.Delivery) {
		d.FireTimer(arm.ID, arm.Generation)
		stopped = timer.Stop()
		close(gate)
	}}))
	if !stopped || received {
		t.Fatalf("authorized unreceived timer: Stop=%v received=%v", stopped, received)
	}
}

func TestOverdueTimerSelectNeedsAuthorization(t *testing.T) {
	var gate chan struct{}
	var ready bool
	b := newBubble(t, func() {
		timer := time.NewTimer(time.Minute)
		gate = make(chan struct{})
		<-gate
		select {
		case <-timer.C:
			ready = true
		default:
		}
		<-timer.C
	})
	s := step(t, b, bubble.Activation{})
	if len(s.TimerChanges) != 1 {
		t.Fatalf("timer changes: %+v", s)
	}
	arm := s.TimerChanges[0]
	s = step(t, b, bubble.Activation{Now: arm.Deadline.Add(time.Minute), Deliver: func(*bubble.Delivery) { close(gate) }})
	if ready || s.Status != bubble.Quiescent {
		t.Fatalf("overdue timer became ready without authorization: ready=%v state=%+v", ready, s)
	}
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) { d.FireTimer(arm.ID, arm.Generation) }}))
}

func TestAuthorizedTimerResetSuppressesReceive(t *testing.T) {
	var timer *time.Timer
	var gate chan struct{}
	var reset, received bool
	b := newBubble(t, func() {
		timer = time.NewTimer(time.Minute)
		gate = make(chan struct{})
		<-gate
		select {
		case <-timer.C:
			received = true
		default:
		}
		timer.Stop()
	})
	s := step(t, b, bubble.Activation{})
	if len(s.TimerChanges) != 1 {
		t.Fatalf("timer changes: %+v", s)
	}
	arm := s.TimerChanges[0]
	s = step(t, b, bubble.Activation{Now: arm.Deadline, Deliver: func(d *bubble.Delivery) {
		d.FireTimer(arm.ID, arm.Generation)
		reset = timer.Reset(time.Minute)
		close(gate)
	}})
	requireCompleted(t, s)
	if !reset || received {
		t.Fatalf("authorized unreceived timer: Reset=%v received=%v", reset, received)
	}
}

func TestNonpositiveTimers(t *testing.T) {
	var first, second time.Time
	b := newBubble(t, func() {
		timer := time.NewTimer(0)
		first = <-timer.C
		timer.Reset(-time.Minute)
		second = <-timer.C
	})
	s := step(t, b, bubble.Activation{})
	requireCompleted(t, s)
	if !first.Equal(start) || !second.Equal(start) {
		t.Fatalf("local-ready timer values: %v %v", first, second)
	}
	if len(s.TimerChanges) != 0 {
		t.Fatalf("local-ready timer exposed host registrations: %+v", s.TimerChanges)
	}
}

type deliveryPanicStringer struct{ called *bool }

func (p deliveryPanicStringer) String() string {
	*p.called = true
	return "user formatter executed outside bubble"
}

// A subprocess also exercises fatal unsupported delivery operations.
func TestDeliveryFault(t *testing.T) {
	if scenario := os.Getenv("GO_BUBBLE_DELIVERY_FAULT"); scenario != "" {
		var timer *time.Timer
		var resumed bool
		var formatted bool
		b, err := bubble.New(bubble.Options{StartTime: start}, func() {
			timer = time.NewTimer(time.Hour)
			<-timer.C
			resumed = true
		})
		if err != nil {
			t.Fatal(err)
		}
		s := step(t, b, bubble.Activation{})
		if len(s.TimerChanges) != 1 {
			t.Fatalf("timer changes: %+v", s)
		}
		arm := s.TimerChanges[0]
		s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) {
			switch scenario {
			case "panic":
				go func() { resumed = true }()
				panic("host callback")
			case "early":
				d.FireTimer(arm.ID, arm.Generation)
			case "unknown":
				d.FireTimer(arm.ID+100, arm.Generation)
			case "future-generation":
				d.FireTimer(arm.ID, arm.Generation+1)
			case "panic-stringer":
				panic(deliveryPanicStringer{&formatted})
			case "panic-long-message":
				panic(strings.Repeat("x", 1<<20))
			case "goexit":
				runtime.Goexit()
			default:
				t.Fatal("unknown subprocess scenario")
			}
		}})
		if err == nil || s.Status != bubble.Faulted || s.LiveGoroutines < 1 || resumed {
			t.Fatalf("fault boundary: err=%v state=%+v resumed=%v", err, s, resumed)
		}
		if formatted || len(err.Error()) > 512 {
			t.Fatalf("unsafe panic formatting: String called=%v error bytes=%d", formatted, len(err.Error()))
		}
		called := false
		if _, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { called = true }}); err == nil || called {
			t.Fatal("faulted bubble resumed")
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if resumed {
			t.Fatal("fault disposal resumed user code")
		}
		return
	}
	for _, scenario := range []string{"panic", "early", "unknown", "future-generation", "panic-stringer", "panic-long-message", "goexit"} {
		t.Run(scenario, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestDeliveryFault$", "-test.timeout=10s")
			cmd.Env = append(os.Environ(), "GO_BUBBLE_DELIVERY_FAULT="+scenario)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

func TestUnsupportedOperations(t *testing.T) {
	if scenario := os.Getenv("GO_BUBBLE_UNSUPPORTED"); scenario != "" {
		external := make(chan int, 1)
		var owned chan int
		b, err := bubble.New(bubble.Options{}, func() {
			switch scenario {
			case "external-send":
				external <- 1
			case "external-select":
				select {
				case <-external:
				default:
				}
			case "external-reflect-select":
				reflect.Select([]reflect.SelectCase{
					{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(external)},
					{Dir: reflect.SelectDefault},
				})
			case "external-close":
				close(external)
			case "external-closed-receive":
				<-external
			case "host-receive":
				owned = make(chan int, 1)
				owned <- 42
			case "ticker":
				time.NewTicker(time.Hour)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if scenario == "external-closed-receive" {
			close(external)
		}
		var activation bubble.Activation
		if scenario == "delivery-yield" {
			activation.Deliver = func(*bubble.Delivery) { runtime.Gosched() }
		}
		if scenario == "delivery-block" {
			activation.Deliver = func(*bubble.Delivery) { <-make(chan int) }
		}
		if _, err := b.Step(activation); err != nil {
			t.Fatal(err)
		}
		if scenario == "host-receive" {
			<-owned
		}
		return
	}
	for _, scenario := range []string{"external-send", "external-select", "external-reflect-select", "external-close", "external-closed-receive", "host-receive", "ticker", "delivery-yield", "delivery-block"} {
		t.Run(scenario, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestUnsupportedOperations$", "-test.timeout=10s")
			cmd.Env = append(os.Environ(), "GO_BUBBLE_UNSUPPORTED="+scenario)
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("unsupported operation succeeded: %s", output)
			}
			if strings.Contains(string(output), "test timed out") {
				t.Fatalf("unsupported operation hung instead of being rejected: %s", output)
			}
			if !strings.Contains(string(output), "bubble") {
				t.Fatalf("missing bubble diagnostic: %s", output)
			}
		})
	}
}
