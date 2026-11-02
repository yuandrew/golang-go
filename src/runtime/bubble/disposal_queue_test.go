// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"fmt"
	"runtime"
	"runtime/bubble"
	"sync/atomic"
	"testing"
)

func TestDiscardChannelQueues(t *testing.T) {
	for _, children := range []int{1, 16, 128} {
		for _, direction := range []string{"send", "receive", "select-send", "select-receive", "select-both"} {
			t.Run(fmt.Sprintf("%s/%d", direction, children), func(t *testing.T) {
				var ran atomic.Bool
				b, err := bubble.New(bubble.Options{}, func() {
					shared := make(chan int)
					for range children {
						ch := shared
						// Separate channels prevent peers from satisfying mixed selects.
						if direction == "select-both" {
							ch = make(chan int)
						}
						go func() {
							defer ran.Store(true)
							switch direction {
							case "send":
								ch <- 1
							case "receive":
								<-ch
							case "select-send":
								select {
								case ch <- 1:
								case ch <- 2:
								}
							case "select-receive":
								select {
								case <-ch:
								case <-ch:
								}
							case "select-both":
								select {
								case ch <- 1:
								case <-ch:
								}
							}
							ran.Store(true)
						}()
					}
				})
				if err != nil {
					t.Fatal(err)
				}
				state, err := b.Step(bubble.Activation{})
				if err != nil || state.Status != bubble.Quiescent || !state.RootDone || state.LiveGoroutines != children {
					t.Fatalf("paused state: %+v, %v", state, err)
				}
				runtime.GC()
				if err := b.Discard(); err != nil {
					t.Fatal(err)
				}
				if ran.Load() {
					t.Fatal("discard ran user code or defers")
				}
			})
		}
	}
}
