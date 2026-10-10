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

func BenchmarkClosePaused(b *testing.B) {
	const ownedWaits = 8
	for _, neighbors := range []int{0, 1000} {
		b.Run(fmt.Sprintf("neighbors=%d", neighbors), func(b *testing.B) {
			gate := make(chan struct{})
			var ready, finished sync.WaitGroup
			ready.Add(neighbors)
			finished.Add(neighbors)
			for range neighbors {
				go func() { ready.Done(); <-gate; finished.Done() }()
			}
			ready.Wait()
			defer func() { close(gate); finished.Wait() }()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				workflow, err := bubble.New(bubble.Options{}, func() {
					for range ownedWaits {
						go func() { select {} }()
					}
				})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := workflow.Step(bubble.Activation{}); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				if err := workflow.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
