// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"runtime/bubble"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestWaitGroupPendingDiscard(t *testing.T) {
	for _, groups := range []int{1, 3} {
		for _, waiters := range []int{1, 4, 16} {
			t.Run(fmt.Sprintf("groups%d/waiters%d", groups, waiters), func(t *testing.T) {
				var ran atomic.Int64
				var list []*sync.WaitGroup
				b, err := bubble.New(bubble.Options{}, func() {
					gate := make(chan struct{})
					for range groups {
						wg := new(sync.WaitGroup)
						list = append(list, wg)
						wg.Add(1)
						for range waiters {
							go func() { defer ran.Add(1); wg.Wait(); ran.Add(100) }()
						}
					}
					<-gate
				})
				if err != nil {
					t.Fatal(err)
				}
				s, err := b.Step(bubble.Activation{})
				if err != nil || s.Status != bubble.Quiescent || s.LiveGoroutines != groups*waiters+1 {
					t.Fatal(s, err)
				}
				if err = b.Discard(); err != nil {
					t.Fatalf("pending WaitGroup discard: %v", err)
				}
				if ran.Load() != 0 {
					t.Fatal("discard ran user code or defers")
				}
				for _, wg := range list {
					for _, operation := range []func(){func() { wg.Add(1) }, wg.Done, wg.Wait, func() { wg.Go(func() {}) }} {
						func() {
							defer func() {
								if recover() == nil {
									t.Error("abandoned group accepted operation")
								}
							}()
							operation()
						}()
					}
				}
			})
		}
	}
}

func TestWaitGroupDequeuedDiscard(t *testing.T) {
	var wg *sync.WaitGroup
	var ran atomic.Bool
	b, _ := bubble.New(bubble.Options{}, func() {
		defer ran.Store(true)
		wg = new(sync.WaitGroup)
		wg.Add(1)
		go func() { defer wg.Done(); panic("fault") }()
		wg.Wait()
		ran.Store(true)
	})
	s, err := b.Step(bubble.Activation{})
	if err == nil || s.Status != bubble.Faulted {
		t.Fatal(s, err)
	}
	if err = b.Discard(); err != nil {
		t.Fatal(err)
	}
	if ran.Load() {
		t.Fatal("resumed dequeued waiter")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("discarded dequeued group was reusable")
		}
	}()
	wg.Add(1)
}

func TestWaitGroupOrdinaryMemoryModel(t *testing.T) {
	for range 100 {
		var wg sync.WaitGroup
		var data [32]int
		for i := range data {
			wg.Go(func() { data[i] = i + 1 })
		}
		wg.Wait()
		for i, n := range data {
			if n != i+1 {
				t.Fatal("lost synchronization")
			}
		}
	}
}

func TestWaitGroupNormalOwned(t *testing.T) {
	var trace []int
	var mu sync.Mutex
	b, _ := bubble.New(bubble.Options{Seed: 42}, func() {
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() { runtime.Gosched(); mu.Lock(); trace = append(trace, i); mu.Unlock() })
		}
		wg.Wait()
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil || s.Status != bubble.Completed || !reflect.DeepEqual(trace, []int{0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Fatal(s, err, trace)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
}

var packageWaitGroup sync.WaitGroup

func TestWaitGroupOwnership(t *testing.T) {
	if mode := os.Getenv("GO_BUBBLE_WAITGROUP_OWNER"); mode != "" {
		wg := new(sync.WaitGroup)
		if mode == "external-active" {
			wg.Add(1)
		}
		b, err := bubble.New(bubble.Options{}, func() {
			switch mode {
			case "global-value":
				packageWaitGroup.Add(1)
			case "external-active":
				wg.Wait()
			default:
				wg.Add(1)
				if mode == "foreign-zero" {
					wg.Done()
				}
				<-make(chan struct{})
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Step(bubble.Activation{}); err != nil {
			t.Fatal(err)
		}
		switch mode {
		case "foreign-wait", "foreign-zero":
			wg.Wait()
		case "foreign-done":
			wg.Done()
		case "other-bubble":
			other, err := bubble.New(bubble.Options{}, wg.Wait)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := other.Step(bubble.Activation{}); err != nil {
				t.Fatal(err)
			}
		}
		t.Fatal("foreign WaitGroup access survived")
	}
	for _, mode := range []string{"global-value", "external-active", "foreign-wait", "foreign-zero", "foreign-done", "other-bubble"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestWaitGroupOwnership$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "GO_BUBBLE_WAITGROUP_OWNER="+mode)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "fatal error: sync: WaitGroup") || strings.Contains(string(output), "test timed out") {
				t.Fatalf("expected domain rejection: %v\n%s", err, output)
			}
		})
	}
}
