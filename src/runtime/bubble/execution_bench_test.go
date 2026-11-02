// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"runtime/bubble"
	"testing"
)

// Each operation includes creation, empty execution to completion, and cleanup.
func BenchmarkBubbleCreate(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		workflow, err := bubble.New(bubble.Options{}, func() {})
		if err != nil {
			b.Fatal(err)
		}
		state, err := workflow.Step(bubble.Activation{})
		if err != nil {
			b.Fatal(err)
		}
		if state.Status != bubble.Completed || state.LiveGoroutines != 0 {
			b.Fatalf("empty bubble did not complete: %+v", state)
		}
		if err := workflow.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBubbleStep(b *testing.B) {
	var inbox chan struct{}
	processed := 0
	workflow, err := bubble.New(bubble.Options{}, func() {
		inbox = make(chan struct{}, 1)
		for range inbox {
			processed++
		}
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := workflow.Close(); err != nil {
			b.Fatal(err)
		}
	})
	state, err := workflow.Step(bubble.Activation{})
	if err != nil {
		b.Fatal(err)
	}
	if state.Status != bubble.Quiescent {
		b.Fatalf("bubble did not pause for input: %+v", state)
	}
	activation := bubble.Activation{Deliver: func(*bubble.Delivery) { inbox <- struct{}{} }}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		state, err = workflow.Step(activation)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if processed != b.N || state.Status != bubble.Quiescent {
		b.Fatalf("processed %d inputs, want %d; state: %+v", processed, b.N, state)
	}
	state, err = workflow.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { close(inbox) }})
	if err != nil {
		b.Fatal(err)
	}
	if state.Status != bubble.Completed || state.LiveGoroutines != 0 {
		b.Fatalf("bubble did not complete: %+v", state)
	}
}

// Each operation is one request/reply round trip, or two unbuffered handoffs.
func BenchmarkBubbleChannelHandoff(b *testing.B) {
	var requests, replies chan int
	completed := 0
	workflow, err := bubble.New(bubble.Options{}, func() {
		for i := range b.N {
			requests <- i
			if got := <-replies; got != i {
				panic("channel reply does not match request")
			}
			completed++
		}
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := workflow.Close(); err != nil {
			b.Fatal(err)
		}
	})
	setup := bubble.Activation{Deliver: func(*bubble.Delivery) {
		requests = make(chan int)
		replies = make(chan int)
		go func() {
			for range b.N {
				replies <- <-requests
			}
		}()
	}}
	b.ReportAllocs()
	b.ResetTimer()
	state, err := workflow.Step(setup)
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	if completed != b.N || state.Status != bubble.Completed || state.LiveGoroutines != 0 {
		b.Fatalf("completed %d round trips, want %d; state: %+v", completed, b.N, state)
	}
}
