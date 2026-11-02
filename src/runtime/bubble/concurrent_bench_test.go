// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"fmt"
	"runtime/bubble"
	"sync"
	"testing"
)

// Divide b.N input activations across independent host controllers. Each input
// performs one internal request/reply round trip before returning to quiescence.
func BenchmarkBubbleConcurrent(b *testing.B) {
	for _, count := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("bubbles=%d", count), func(b *testing.B) {
			workflows := make([]*bubble.Bubble, count)
			inboxes := make([]chan int, count)
			processed := make([]int, count)
			for i := range workflows {
				workflow, err := bubble.New(bubble.Options{}, func() {
					inboxes[i] = make(chan int, 1)
					requests, replies := make(chan int), make(chan int)
					go func() {
						for value := range requests {
							replies <- value
						}
					}()
					for value := range inboxes[i] {
						requests <- value
						if got := <-replies; got != value {
							panic("channel reply does not match input")
						}
						processed[i]++
					}
					close(requests)
				})
				if err != nil {
					b.Fatal(err)
				}
				workflows[i] = workflow
				b.Cleanup(func() {
					if err := workflow.Close(); err != nil {
						b.Error(err)
					}
				})
				state, err := workflow.Step(bubble.Activation{})
				if err != nil || state.Status != bubble.Quiescent {
					b.Fatalf("initial Step: %+v, %v", state, err)
				}
			}
			start := make(chan struct{})
			errors := make(chan error, count)
			var ready, finished sync.WaitGroup
			ready.Add(count)
			finished.Add(count)
			for i, workflow := range workflows {
				go func() {
					defer finished.Done()
					ready.Done()
					<-start
					for value := i; value < b.N; value += count {
						state, err := workflow.Step(bubble.Activation{
							Deliver: func(*bubble.Delivery) { inboxes[i] <- value },
						})
						if err != nil || state.Status != bubble.Quiescent {
							errors <- fmt.Errorf("bubble %d Step: %+v, %v", i, state, err)
							return
						}
					}
				}()
			}
			ready.Wait()
			b.ReportAllocs()
			b.ResetTimer()
			close(start)
			finished.Wait()
			b.StopTimer()
			close(errors)
			for err := range errors {
				b.Error(err)
			}
			total := 0
			for i, workflow := range workflows {
				total += processed[i]
				state, err := workflow.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { close(inboxes[i]) }})
				if err != nil || state.Status != bubble.Completed || state.LiveGoroutines != 0 {
					b.Fatalf("final Step: %+v, %v", state, err)
				}
			}
			if total != b.N {
				b.Fatalf("processed %d inputs, want %d", total, b.N)
			}
		})
	}
}
