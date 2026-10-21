// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime_test

import (
	"bytes"
	"runtime"
	"runtime/bubble"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"testing"
	"weak"
)

func TestBubbleWaitGroupStressDiscard(t *testing.T) {
	const cycles = 128
	var defers atomic.Int64
	var stop atomic.Bool
	var collectors sync.WaitGroup
	collectors.Go(func() {
		for !stop.Load() {
			runtime.GC()
			runtime.Gosched()
		}
	})
	collectors.Go(func() {
		for !stop.Load() {
			var buf bytes.Buffer
			if err := pprof.Lookup("goroutine").WriteTo(&buf, 0); err != nil {
				panic(err)
			}
			runtime.Gosched()
		}
	})
	defer func() { stop.Store(true); collectors.Wait() }()
	var traced bytes.Buffer
	if err := trace.Start(&traced); err != nil {
		t.Fatal(err)
	}
	defer trace.Stop()
	refs := make([]weak.Pointer[[4096]byte], 0, cycles)
	var groupRefs []weak.Pointer[sync.WaitGroup]
	var handles []any
	for i := range cycles {
		var wg *sync.WaitGroup
		b, _ := bubble.New(bubble.Options{Seed: uint64(i + 1)}, func() {
			wg = new(sync.WaitGroup)
			wg.Add(1)
			data := new([4096]byte)
			data[0] = byte(i)
			refs = append(refs, weak.Make(data))
			gate := make(chan struct{})
			for range 8 {
				go func() { defer defers.Add(1); wg.Wait(); runtime.KeepAlive(data) }()
			}
			if i%2 == 0 {
				go func() { wg.Done(); panic("fault after wake") }()
			}
			<-gate
		})
		s, err := b.Step(bubble.Activation{})
		if i%2 == 0 && (err == nil || s.Status != bubble.Faulted) {
			t.Fatal(s, err)
		}
		if i%2 != 0 && (err != nil || s.Status != bubble.Quiescent) {
			t.Fatal(s, err)
		}
		handle := handleOf(b)
		groupRefs = append(groupRefs, weak.Make(wg))
		handles = append(handles, handle)
		n, _, retained := runtime.BubbleWaitGroupRegistry(handle)
		if n != 8 || retained != 8 {
			t.Fatal("missing live waiter records", n, retained)
		}
		if err = b.Discard(); err != nil {
			t.Fatal(err)
		}
		if n, capacity, retained := runtime.BubbleWaitGroupRegistry(handle); n != 0 || capacity != 0 || retained != 0 {
			t.Fatal("discard retained registry", n, capacity, retained)
		}
		assertDiscarded(t, wg, i%2 != 0)
	}
	if defers.Load() != 0 {
		t.Fatal("discard ran user defers", defers.Load())
	}
	for range 20 {
		runtime.GC()
		alive := 0
		for _, ref := range refs {
			if ref.Value() != nil {
				alive++
			}
		}
		for _, ref := range groupRefs {
			if ref.Value() != nil {
				alive++
			}
		}
		if alive == 0 {
			runtime.KeepAlive(handles)
			return
		}
		runtime.Gosched()
	}
	t.Fatal("discard retains waiter payloads")
}

func TestBubbleWaitGroupQueueRootAccounting(t *testing.T) {
	// Include multiple addresses in the same semaphore hash root.
	const groups = 512
	var list []*sync.WaitGroup
	b, _ := bubble.New(bubble.Options{}, func() {
		for range groups {
			wg := new(sync.WaitGroup)
			list = append(list, wg)
			wg.Add(1)
			go wg.Wait()
		}
		<-make(chan struct{})
	})
	if _, err := b.Step(bubble.Activation{}); err != nil {
		t.Fatal(err)
	}
	before := make([]observed, len(list))
	collision := false
	for i, wg := range list {
		before[i] = observe(wg)
		if before[i].queued != 1 {
			t.Fatal(before[i])
		}
		if before[i].root > 1 {
			collision = true
		}
	}
	if !collision {
		t.Fatal("fixture has no shared semaphore hash root")
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
	for _, wg := range list {
		assertDiscarded(t, wg, true)
		if observe(wg).root != 0 {
			t.Fatal("stranded root waiter accounting", observe(wg))
		}
	}
}
