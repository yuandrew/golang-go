// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime_test

import (
	"runtime"
	"runtime/bubble"
	"runtime/debug"
	"sync"
	"testing"
	"unsafe"
)

func TestBubbleWaitGroupUnrelatedSharedRoot(t *testing.T) {
	var wg *sync.WaitGroup
	b, _ := bubble.New(bubble.Options{}, func() { wg = new(sync.WaitGroup); wg.Add(1); wg.Wait() })
	if _, err := b.Step(bubble.Activation{}); err != nil {
		t.Fatal(err)
	}
	state, sema := fields(wg)
	pool := new([4096]uint32)
	var unrelated *uint32
	for i := range pool {
		if runtime.BubbleWaitGroupSameRoot(sema, &pool[i]) {
			unrelated = &pool[i]
			break
		}
	}
	if unrelated == nil {
		t.Fatal("no shared hash root found")
	}
	done := make(chan struct{})
	go foreignWait(unrelated, done)
	waitForeign(t)
	before := observe(wg)
	if before.queued != 1 || before.root != 2 {
		t.Fatal("bad shared root fixture", before)
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
	after := observe(wg)
	if after.queued != 0 || after.root != 1 {
		t.Fatal("discard damaged unrelated address", after)
	}
	_, tokens, queued, nwait, _ := runtime.BubbleWaitGroupSnapshot(state, unrelated, unsafe.Pointer(wg))
	if tokens != 0 || queued != 1 || nwait != 1 {
		t.Fatal("unrelated waiter changed", tokens, queued, nwait)
	}
	bareRelease(unrelated, false, 0)
	<-done
	if observe(wg).root != 0 {
		t.Fatal("root count did not clear")
	}
	runtime.KeepAlive(pool)
}

//go:noinline
func growBubbleWaitGroupStack(n int) int {
	var frame [1024]byte
	frame[n] = byte(n)
	result := 0
	if n > 0 {
		result = growBubbleWaitGroupStack(n - 1)
	}
	return result + int(frame[n])
}

func TestBubbleWaitGroupActualStackShrink(t *testing.T) {
	gc := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(gc)
	runtime.SetBlockProfileRate(1)
	defer runtime.SetBlockProfileRate(0)
	for _, awakened := range []bool{false, true} {
		var wg *sync.WaitGroup
		var ran bool
		b, _ := bubble.New(bubble.Options{}, func() {
			wg = new(sync.WaitGroup)
			wg.Add(1)
			for range 16 {
				go func() { growBubbleWaitGroupStack(64); defer func() { ran = true }(); wg.Wait(); ran = true }()
			}
			if awakened {
				go func() { wg.Done(); panic("fault after wake") }()
			}
			<-make(chan struct{})
		})
		s, err := b.Step(bubble.Activation{})
		if awakened && (err == nil || s.Status != bubble.Faulted) {
			t.Fatal(s, err)
		}
		if !awakened && (err != nil || s.Status != bubble.Quiescent) {
			t.Fatal(s, err)
		}
		handle := handleOf(b)
		before, n := runtime.BubbleWaitGroupStacks(handle)
		for range 3 {
			runtime.GC()
		}
		after, m := runtime.BubbleWaitGroupStacks(handle)
		if n != 16 || m != 16 || after >= before {
			t.Fatal("did not exercise stack shrink", before, after, n, m)
		}
		t.Logf("awakened=%v: registered stacks %d -> %d bytes", awakened, before, after)
		if err = b.Discard(); err != nil {
			t.Fatal(err)
		}
		assertDiscarded(t, wg, !awakened)
		if ran {
			t.Fatal("discard ran user code")
		}
		runtime.KeepAlive(handle)
		// Churn ordinary native waits to reuse released descriptors.
		for range 64 {
			var ordinary sync.WaitGroup
			ordinary.Go(func() { runtime.Gosched() })
			ordinary.Wait()
		}
	}
}
