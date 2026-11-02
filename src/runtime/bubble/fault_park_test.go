// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"runtime/bubble"
	"testing"
)

func TestFaultDuringPark(t *testing.T) {
	var peerRan bool
	b, err := bubble.New(bubble.Options{}, func() {
		go func() { panic("sibling fault during parking cleanup") }()
		go func() { peerRan = true }()
		select {}
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := b.Step(bubble.Activation{})
	if err == nil || state.Status != bubble.Faulted || peerRan || state.LiveGoroutines != 2 {
		t.Fatalf("fault state: %+v, %v, peer ran=%v", state, err, peerRan)
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
}
