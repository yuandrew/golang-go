// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build !race

package bubble_test

import (
	"runtime/bubble"
	"testing"
)

func TestMembershipOverflow(t *testing.T) {
	const children = 1 << 16
	var finished bool
	var cleanup bool
	b, err := bubble.New(bubble.Options{}, func() {
		for index := range children {
			go func() {
				if index == children-1 {
					finished = true
					return
				}
				defer func() { cleanup = true }()
				select {}
			}()
		}
		select {}
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.Step(bubble.Activation{})
	if err != nil || state.Status != bubble.Quiescent || state.LiveGoroutines != children || !finished {
		t.Fatalf("overflow execution: %+v, %v, finished=%v", state, err, finished)
	}
	if err := b.Discard(); err != nil || cleanup {
		t.Fatalf("overflow discard: %v, cleanup=%v", err, cleanup)
	}
}
