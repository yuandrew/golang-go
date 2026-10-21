// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"os"
	"os/exec"
	"runtime"
	"runtime/bubble"
	"strings"
	"testing"
	"time"
	"weak"
)

const lifecycleEnv = "GO_BUBBLE_LIFECYCLE"

// A process harness contains baseline fatal behavior; disposal itself must keep
// this worker alive through repeated failures and evictions.
func TestLifecycle(t *testing.T) {
	if mode := os.Getenv(lifecycleEnv); mode != "" {
		switch mode {
		case "panic":
			testChildPanic(t)
		case "dispose":
			testDispose(t)
		case "timer-payload":
			testTimerPayload(t)
		case "recovered":
			testRecoveredPanic(t)
		case "fired-payload":
			testFiredPayload(t)
		case "panic-values":
			testPanicValues(t)
		case "registry-memory":
			testRetiredMemory(t)
		}
		return
	}
	for _, mode := range []string{"panic", "dispose", "timer-payload", "recovered", "fired-payload", "panic-values", "registry-memory"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestLifecycle$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), lifecycleEnv+"="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
		})
	}
}

func testRetiredMemory(t *testing.T) {
	const retiredTimers = 10000
	const batches = 10
	// Allow bookkeeping noise once, not separately for each batch.
	const maxRetiredBytes = 256 << 10
	var commands chan int
	b, _ := bubble.New(bubble.Options{}, func() {
		commands = make(chan int, 1)
		for count := range commands {
			timers := make([]*time.Timer, count)
			for i := range timers {
				timers[i] = time.AfterFunc(time.Hour, func() {})
			}
			for _, timer := range timers {
				if !timer.Stop() {
					panic("Stop failed")
				}
			}
			timers = nil
		}
	})
	if _, err := b.Step(bubble.Activation{}); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for batch := 1; batch <= batches; batch++ {
		if _, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { commands <- retiredTimers }}); err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		t.Logf("batch %d: retained %d bytes from baseline", batch, retained)
		if retained > maxRetiredBytes {
			t.Fatalf("retired one-shot registry retains %d bytes after %d batches", retained, batch)
		}
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
}

func testFiredPayload(t *testing.T) {
	const payloadSize = 1 << 20
	var payload weak.Pointer[[payloadSize]byte]
	var gate chan struct{}
	var fired bool
	b, _ := bubble.New(bubble.Options{}, func() {
		gate = make(chan struct{})
		func() {
			data := new([payloadSize]byte)
			payload = weak.Make(data)
			time.AfterFunc(time.Hour, func() { fired = true; runtime.KeepAlive(data) })
		}()
		<-gate
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil {
		t.Fatal(err)
	}
	c := s.TimerChanges[0]
	s, err = b.Step(bubble.Activation{Now: c.Deadline, Deliver: func(d *bubble.Delivery) { d.FireTimer(c.ID, c.Generation) }})
	if err != nil || !fired {
		t.Fatalf("fire: %+v %v", s, err)
	}
	runtime.GC()
	if payload.Value() != nil {
		t.Fatal("fired timer retains captured payload")
	}
	s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) {
		if d.FireTimer(c.ID, c.Generation) {
			t.Error("reclaimed timer refired")
		}
		close(gate)
	}})
	if err != nil || s.Status != bubble.Completed {
		t.Fatalf("finish: %+v %v", s, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

type lifecyclePanic struct{ formatted *bool }

func (p lifecyclePanic) Error() string { *p.formatted = true; return "unsafe formatting" }

func testPanicValues(t *testing.T) {
	for _, mode := range []string{"nested", "object", "blocked-defer"} {
		var formatted bool
		b, _ := bubble.New(bubble.Options{}, func() {
			go func() {
				switch mode {
				case "nested":
					defer func() { panic("defer failure") }()
				case "blocked-defer":
					defer func() { <-make(chan struct{}) }()
				case "object":
					panic(lifecyclePanic{formatted: &formatted})
				}
				panic("original failure")
			}()
			select {}
		})
		s, err := b.Step(bubble.Activation{})
		if mode == "blocked-defer" {
			if err != nil || s.Status != bubble.Quiescent {
				t.Fatalf("blocked defer: %+v %v", s, err)
			}
		} else if err == nil || s.Status != bubble.Faulted {
			t.Fatalf("panic %s: %+v %v", mode, s, err)
		}
		if formatted {
			t.Fatal("panic object formatted")
		}
		if err := b.Discard(); err != nil {
			t.Fatal(err)
		}
	}
}

func testChildPanic(t *testing.T) {
	for range 100 {
		var unwound, peerRan bool
		b, _ := bubble.New(bubble.Options{}, func() {
			go func() { defer func() { unwound = true }(); panic("child failure") }()
			go func() { peerRan = true }()
			select {}
		})
		s, err := b.Step(bubble.Activation{})
		if err == nil || s.Status != bubble.Faulted || !strings.Contains(err.Error(), "child failure") || !unwound || peerRan {
			t.Fatalf("panic: %+v %v unwound=%v peer=%v", s, err, unwound, peerRan)
		}
		if s.LiveGoroutines != 2 || s.RootDone {
			t.Fatalf("fault returned before child teardown: %+v", s)
		}
		if err := b.Discard(); err != nil {
			t.Fatal(err)
		}
		if peerRan {
			t.Fatal("disposal executed queued peer")
		}
	}
}

func testDispose(t *testing.T) {
	before := runtime.NumGoroutine()
	for range 100 {
		var resumed, deferred bool
		b, _ := bubble.New(bubble.Options{}, func() {
			c := make(chan int)
			other := make(chan int)
			waits := []func(){
				func() { make(chan int) <- 1 },
				func() { <-other },
				func() {
					select {
					case <-c:
					case <-other:
					}
				},
				func() { var nilc chan int; <-nilc },
				func() { var nilc chan int; nilc <- 1 },
				func() { select {} },
				func() { time.Sleep(time.Hour) },
				func() { <-time.NewTimer(time.Hour).C },
			}
			// Separate channel directions ensure every child remains parked.
			for _, wait := range waits {
				go func() { defer func() { deferred = true }(); wait(); resumed = true }()
			}
		})
		s, err := b.Step(bubble.Activation{})
		if err != nil || s.Status != bubble.Quiescent || s.LiveGoroutines != 8 {
			t.Fatalf("park: %+v %v", s, err)
		}
		if err := b.Discard(); err != nil {
			t.Fatal(err)
		}
		if resumed || deferred {
			t.Fatalf("disposal ran user code: resume=%v defer=%v", resumed, deferred)
		}
		if err := b.Discard(); err != nil {
			t.Fatal(err)
		}
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Fatalf("goroutines: before=%d after=%d", before, got)
	}
}

func testTimerPayload(t *testing.T) {
	const payloadSize = 1 << 20
	var payload weak.Pointer[[payloadSize]byte]
	var gate chan struct{}
	b, _ := bubble.New(bubble.Options{}, func() {
		gate = make(chan struct{})
		func() {
			data := new([payloadSize]byte)
			payload = weak.Make(data)
			timer := time.AfterFunc(time.Hour, func() { runtime.KeepAlive(data) })
			if !timer.Stop() {
				panic("Stop failed")
			}
		}()
		<-gate
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	if payload.Value() != nil {
		t.Fatal("stopped timer retains captured payload")
	}
	stale := s.TimerChanges[0]
	s, err = b.Step(bubble.Activation{Deliver: func(d *bubble.Delivery) {
		if d.FireTimer(stale.ID, stale.Generation) {
			t.Error("reclaimed timer fired")
		}
		close(gate)
	}})
	if err != nil || s.Status != bubble.Completed {
		t.Fatalf("finish: %+v %v", s, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func testRecoveredPanic(t *testing.T) {
	var recovered bool
	b, _ := bubble.New(bubble.Options{}, func() {
		go func() { defer func() { recovered = recover() != nil }(); panic("recoverable") }()
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || s.Status != bubble.Completed || !recovered {
		t.Fatalf("recover: %+v %v recovered=%v", s, err, recovered)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseRejectsPausedExecution(t *testing.T) {
	var gate chan struct{}
	var deferred bool
	b, err := bubble.New(bubble.Options{}, func() {
		defer func() { deferred = true }()
		gate = make(chan struct{})
		<-gate
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.Step(bubble.Activation{})
	if err != nil || state.Status != bubble.Quiescent {
		t.Fatalf("pause: %+v, %v", state, err)
	}
	if err := b.Close(); err == nil {
		t.Fatal("Close discarded paused execution")
	}
	if deferred {
		t.Fatal("rejected Close ran a defer")
	}
	state, err = b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { close(gate) }})
	if err != nil || state.Status != bubble.Completed || !deferred {
		t.Fatalf("resume after rejected Close: %+v, %v, defer=%v", state, err, deferred)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiscardLifetime(t *testing.T) {
	for _, mode := range []string{"never-started", "completed", "paused", "faulted"} {
		t.Run(mode, func(t *testing.T) {
			var called, deferred bool
			b, err := bubble.New(bubble.Options{}, func() {
				called = true
				defer func() { deferred = true }()
				switch mode {
				case "paused":
					select {}
				case "faulted":
					go func() { panic("fault") }()
					select {}
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			if mode != "never-started" {
				state, err := b.Step(bubble.Activation{})
				want := bubble.Quiescent
				if mode == "completed" {
					want = bubble.Completed
				}
				if mode == "faulted" {
					want = bubble.Faulted
				}
				if state.Status != want || (err != nil) != (mode == "faulted") {
					t.Fatalf("start: %+v, %v", state, err)
				}
			}
			if mode == "paused" || mode == "faulted" {
				if err := b.Close(); err == nil {
					t.Fatal("Close accepted live goroutines")
				}
			}
			if err := b.Discard(); err != nil {
				t.Fatal(err)
			}
			if called != (mode != "never-started") || deferred != (mode == "completed") {
				t.Fatalf("user code after Discard: called=%v, defer=%v", called, deferred)
			}
			if err := b.Discard(); err != nil {
				t.Fatalf("repeated Discard: %v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("Close after Discard: %v", err)
			}
			delivered := false
			if _, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { delivered = true }}); err == nil || delivered {
				t.Fatal("discarded execution accepted activation")
			}
		})
	}
}

func TestCloseAfterFaultWithoutPeers(t *testing.T) {
	b, err := bubble.New(bubble.Options{}, func() { panic("fault") })
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.Step(bubble.Activation{})
	if err == nil || state.Status != bubble.Faulted || state.LiveGoroutines != 0 {
		t.Fatalf("fault: %+v, %v", state, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Discard(); err != nil {
		t.Fatalf("Discard after Close: %v", err)
	}
}
