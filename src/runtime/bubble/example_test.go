// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"fmt"
	"runtime/bubble"
	"time"
)

func Example() {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var aTime, bTime time.Time
	a, err := bubble.New(bubble.Options{StartTime: start}, func() {
		time.Sleep(time.Minute)
		aTime = time.Now()
	})
	if err != nil {
		panic(err)
	}
	defer a.Close()
	var inbox chan struct{}
	b, err := bubble.New(bubble.Options{StartTime: start}, func() {
		inbox = make(chan struct{})
		<-inbox
		bTime = time.Now()
	})
	if err != nil {
		panic(err)
	}
	defer b.Close()

	as, err := a.Step(bubble.Activation{})
	if err != nil {
		panic(err)
	}
	bs, err := b.Step(bubble.Activation{})
	if err != nil {
		panic(err)
	}
	fmt.Println("a quiescent:", as.Status == bubble.Quiescent)
	fmt.Println("b quiescent:", bs.Status == bubble.Quiescent)

	// Advancing a's clock and authorizing its timer leaves b's clock unchanged.
	armed := as.TimerChanges[0]
	_, err = a.Step(bubble.Activation{
		Now: armed.Deadline,
		Deliver: func(d *bubble.Delivery) {
			d.FireTimer(armed.ID, armed.Generation)
		},
	})
	if err != nil {
		panic(err)
	}
	_, err = b.Step(bubble.Activation{
		Deliver: func(*bubble.Delivery) { close(inbox) },
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("a time:", aTime.UTC().Format(time.RFC3339))
	fmt.Println("b time:", bTime.UTC().Format(time.RFC3339))
	// Output:
	// a quiescent: true
	// b quiescent: true
	// a time: 2026-01-01T00:01:00Z
	// b time: 2026-01-01T00:00:00Z
}
