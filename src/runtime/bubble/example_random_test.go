// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"fmt"
	rand "math/rand/v2"
	"runtime/bubble"
)

func Example_randomSource() {
	run := func() [3]uint64 {
		var values [3]uint64
		b, err := bubble.New(bubble.Options{
			Seed:         7, // scheduler choices
			RandomSource: rand.NewPCG(42, 99),
		}, func() {
			for i := range values {
				values[i] = rand.Uint64() // ordinary package-level API
			}
		})
		if err != nil {
			panic(err)
		}
		if _, err := b.Step(bubble.Activation{}); err != nil {
			panic(err)
		}
		if err := b.Close(); err != nil {
			panic(err)
		}
		return values
	}
	// Replaying requires a fresh source with the same initial state.
	fmt.Println("same stream:", run() == run())
	// Output: same stream: true
}
