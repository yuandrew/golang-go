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
	"sync"
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
		case "preflight":
			testDisposePreflight(t)
		case "fired-payload":
			testFiredPayload(t)
		case "panic-values":
			testPanicValues(t)
		case "registry-memory":
			testRetiredMemory(t)
		}
		return
	}
	for _, mode := range []string{"panic", "dispose", "timer-payload", "recovered", "preflight", "fired-payload", "panic-values", "registry-memory"} {
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
	// Allow runtime bookkeeping noise, but reject a retained record per timer.
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
	if _, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { commands <- retiredTimers }}); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if retained > maxRetiredBytes {
		t.Fatalf("retired one-shot registry retains %d bytes", retained)
	}
	if err := b.Close(); err != nil {
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
		if err := b.Close(); err != nil {
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
		if err := b.Close(); err != nil {
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
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if resumed || deferred {
			t.Fatalf("disposal ran user code: resume=%v defer=%v", resumed, deferred)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Fatalf("goroutines: before=%d after=%d", before, got)
	}
}

func testDisposePreflight(t *testing.T) {
	var wg *sync.WaitGroup
	var gate chan struct{}
	var completed bool
	b, _ := bubble.New(bubble.Options{}, func() {
		wg = new(sync.WaitGroup)
		wg.Add(1)
		gate = make(chan struct{})
		go func() { wg.Wait(); completed = true }()
		<-gate
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || s.LiveGoroutines != 2 {
		t.Fatalf("start: %+v %v", s, err)
	}
	if err := b.Close(); err == nil {
		t.Fatal("unsupported WaitGroup disposal accepted")
	}
	s, err = b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { wg.Done(); close(gate) }})
	if err != nil || s.Status != bubble.Completed || !completed {
		t.Fatalf("preflight mutated live state: %+v %v", s, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
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
