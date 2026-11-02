// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime_test

import (
	"reflect"
	"runtime"
	"runtime/bubble"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

func handleOf(b *bubble.Bubble) any {
	field := reflect.ValueOf(b).Elem().FieldByName("handle")
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}

//go:linkname bareAcquire sync.runtime_Semacquire
func bareAcquire(*uint32)

//go:linkname bareRelease sync.runtime_Semrelease
func bareRelease(*uint32, bool, int)

func fields(wg *sync.WaitGroup) (*uint64, *uint32) {
	// Assert the current private layout rather than silently relying on offsets.
	if unsafe.Sizeof(*wg) != 16 {
		panic("unexpected WaitGroup layout")
	}
	return (*uint64)(unsafe.Pointer(wg)), (*uint32)(unsafe.Add(unsafe.Pointer(wg), 8))
}

type observed struct {
	state       uint64
	tokens      uint32
	queued      int
	root        uint32
	association int
}

func observe(wg *sync.WaitGroup) observed {
	state, sema := fields(wg)
	a, b, c, d, e := runtime.BubbleWaitGroupSnapshot(state, sema, unsafe.Pointer(wg))
	return observed{a, b, c, d, e}
}

func foreignWait(sema *uint32, done chan struct{}) { bareAcquire(sema); close(done) }

func waitForeign(t *testing.T) {
	t.Helper()
	stack := make([]byte, 1<<20)
	for range 10000 {
		text := string(stack[:runtime.Stack(stack, true)])
		for _, block := range strings.Split(text, "\n\n") {
			if strings.Contains(block, "[semacquire]") && strings.Contains(block, "foreignWait(") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("foreign semaphore waiter did not park")
}

func TestBubbleWaitGroupRejectSharedQueue(t *testing.T) {
	var wg *sync.WaitGroup
	var valid *sync.WaitGroup
	var gate chan struct{}
	var completed bool
	b, _ := bubble.New(bubble.Options{}, func() {
		valid = new(sync.WaitGroup)
		valid.Add(1)
		go valid.Wait()
		wg = new(sync.WaitGroup)
		wg.Add(1)
		gate = make(chan struct{})
		<-gate
		wg.Wait()
		completed = true
	})
	if _, err := b.Step(bubble.Activation{}); err != nil {
		t.Fatal(err)
	}
	_, sema := fields(wg)
	done := make(chan struct{})
	go foreignWait(sema, done)
	waitForeign(t)
	s, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { close(gate) }})
	if err != nil || s.Status != bubble.Quiescent {
		t.Fatal(s, err)
	}
	before := observe(wg)
	validBefore := observe(valid)
	if before.queued != 2 || before.state&0x7fffffff != 1 {
		t.Fatalf("bad fixture: %+v", before)
	}
	if err = b.Discard(); err == nil {
		t.Fatal("discard accepted foreign semaphore waiter")
	}
	after := observe(wg)
	if after != before {
		t.Fatalf("rejection mutated queue: %+v -> %+v", before, after)
	}
	if observe(valid) != validBefore {
		t.Fatal("rejection mutated earlier valid group")
	}
	if completed {
		t.Fatal("rejection resumed user code")
	}
	bareRelease(sema, false, 0)
	<-done
	s, err = b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { wg.Done(); valid.Done() }})
	if err != nil || s.Status != bubble.Completed || !completed {
		t.Fatal(s, err, completed)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertDiscarded(t *testing.T, wg *sync.WaitGroup, queued bool) {
	t.Helper()
	got := observe(wg)
	if got.state != 1<<63|0x80000000 || got.tokens != 0 || got.queued != 0 {
		t.Fatalf("discard left semaphore state: %+v", got)
	}
	if queued && got.association == 0 || !queued && got.association != 0 {
		t.Fatalf("unexpected association: %+v", got)
	}
}

func TestBubbleWaitGroupMixedQueueAccounting(t *testing.T) {
	var pending, awakened *sync.WaitGroup
	var fault chan struct{}
	var user bool
	b, _ := bubble.New(bubble.Options{}, func() {
		pending = new(sync.WaitGroup)
		pending.Add(1)
		awakened = new(sync.WaitGroup)
		awakened.Add(1)
		fault = make(chan struct{})
		go func() { <-fault; awakened.Done(); panic("sibling fault") }()
		for range 8 {
			go func() { defer func() { user = true }(); awakened.Wait(); user = true }()
		}
		pending.Wait()
		user = true
	})
	if _, err := b.Step(bubble.Activation{}); err != nil {
		t.Fatal(err)
	}
	a, p := observe(awakened), observe(pending)
	if a.queued != 8 || p.queued != 1 {
		t.Fatal(a, p)
	}
	s, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { close(fault) }})
	if err == nil || s.Status != bubble.Faulted {
		t.Fatal(s, err)
	}
	a = observe(awakened)
	if a.state != 0 || a.tokens != 8 || a.queued != 0 {
		t.Fatalf("no dequeued token fixture: %+v", a)
	}
	if err = b.Discard(); err != nil {
		t.Fatal(err)
	}
	assertDiscarded(t, pending, true)
	assertDiscarded(t, awakened, false)
	for _, wg := range []*sync.WaitGroup{pending, awakened} {
		before := observe(wg)
		for _, f := range []func(){func() { wg.Add(0) }, func() { wg.Add(1) }, wg.Done, wg.Wait, func() { wg.Go(func() { user = true }) }} {
			func() {
				defer func() {
					if recover() == nil {
						t.Error("abandoned group accepted operation")
					}
				}()
				f()
			}()
			if observe(wg) != before {
				t.Fatal("misuse changed tombstone")
			}
		}
	}
	if user {
		t.Fatal("discard ran user code or defers")
	}
}

func TestBubbleWaitGroupGoFaultAndBlockedDefer(t *testing.T) {
	for _, mode := range []string{"go-panic", "blocked-defer"} {
		t.Run(mode, func(t *testing.T) {
			var wg *sync.WaitGroup
			var user bool
			b, _ := bubble.New(bubble.Options{}, func() {
				wg = new(sync.WaitGroup)
				if mode == "go-panic" {
					wg.Go(func() { panic("Go fault") })
				} else {
					wg.Add(1)
					go func() { defer func() { <-make(chan struct{}); wg.Done(); user = true }() }()
				}
				wg.Wait()
				user = true
			})
			s, err := b.Step(bubble.Activation{})
			if mode == "go-panic" && (err == nil || s.Status != bubble.Faulted) {
				t.Fatal(s, err)
			}
			if mode == "blocked-defer" && (err != nil || s.Status != bubble.Quiescent) {
				t.Fatal(s, err)
			}
			if err = b.Discard(); err != nil {
				t.Fatal(err)
			}
			assertDiscarded(t, wg, true)
			if user {
				t.Fatal("discard ran defer or user code")
			}
		})
	}
}

func TestBubbleWaitGroupCompletedGroupSurvivesDiscard(t *testing.T) {
	var completed, pending *sync.WaitGroup
	b, _ := bubble.New(bubble.Options{}, func() {
		completed = new(sync.WaitGroup)
		completed.Go(func() {})
		completed.Wait()
		pending = new(sync.WaitGroup)
		pending.Add(1)
		pending.Wait()
	})
	if _, err := b.Step(bubble.Activation{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
	b, _ = bubble.New(bubble.Options{}, func() { completed.Go(func() {}); completed.Wait() })
	if s, err := b.Step(bubble.Activation{}); err != nil || s.Status != bubble.Completed {
		t.Fatal(s, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	assertDiscarded(t, pending, true)
}

func TestBubbleWaitGroupRegistryNormalReturns(t *testing.T) {
	b, _ := bubble.New(bubble.Options{}, func() { var wg sync.WaitGroup; wg.Go(func() {}); wg.Wait(); <-make(chan struct{}) })
	if _, err := b.Step(bubble.Activation{}); err != nil {
		t.Fatal(err)
	}
	n, capacity, retained := runtime.BubbleWaitGroupRegistry(handleOf(b))
	if n != 0 || capacity < 1 || retained != 0 {
		t.Fatal(n, capacity, retained)
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
}
