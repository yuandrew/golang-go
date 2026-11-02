// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"reflect"
	"runtime/bubble"
	"strings"
	"testing"
)

func TestChannelOwnershipRejection(t *testing.T) {
	operations := []struct {
		name string
		call func(chan int)
	}{
		{"send", func(c chan int) { c <- 9 }},
		{"receive", func(c chan int) { <-c }},
		{"close", func(c chan int) { close(c) }},
		{"select-send", func(c chan int) {
			var other chan int
			select {
			case c <- 9:
			case other <- 1:
			default:
			}
		}},
		{"select-receive", func(c chan int) {
			var other chan int
			select {
			case <-c:
			case <-other:
			default:
			}
		}},
		{"reflect-send", func(c chan int) { reflect.ValueOf(c).Send(reflect.ValueOf(9)) }},
		{"reflect-receive", func(c chan int) { reflect.ValueOf(c).Recv() }},
		{"reflect-select", func(c chan int) {
			reflect.Select([]reflect.SelectCase{
				{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c)},
				{Dir: reflect.SelectDefault},
			})
		}},
		{"len", func(c chan int) {
			if len(c) != 1 {
				panic("unexpected length")
			}
		}},
		{"cap", func(c chan int) {
			if cap(c) != 2 {
				panic("unexpected capacity")
			}
		}},
	}
	for _, domain := range []string{"ordinary", "other-bubble"} {
		for _, context := range []string{"root", "child", "delivery"} {
			for _, operation := range operations {
				for _, policy := range []string{"recovered", "faulted"} {
					t.Run(domain+"/"+context+"/"+operation.name+"/"+policy, func(t *testing.T) {
						checkChannelOwnershipRejection(t, domain, context, policy, operation.call)
					})
				}
			}
		}
	}
}

func checkChannelOwnershipRejection(t *testing.T, domain, context, policy string, operation func(chan int)) {
	t.Helper()
	var external chan int
	var owner *bubble.Bubble
	var ownerGate chan struct{}
	initialize := func() { external = make(chan int, 2); external <- 7 }
	if domain == "ordinary" {
		initialize()
	} else {
		owner = newBubble(t, func() {
			initialize()
			ownerGate = make(chan struct{})
			<-ownerGate
		})
		if s := step(t, owner, bubble.Activation{}); s.Status != bubble.Quiescent {
			t.Fatalf("owner setup: %+v", s)
		}
	}
	const diagnostic = "runtime/bubble: channel belongs to another execution domain"
	var recovered any
	var deferred, after, valid, peer bool
	invoke := func() {
		call := func() {
			defer func() { deferred = true }()
			operation(external)
			after = true
		}
		if policy == "faulted" {
			call()
			return
		}
		func() {
			defer func() { recovered = recover() }()
			call()
		}()
		c := make(chan int, 1)
		c <- 11
		valid = <-c == 11
		close(c)
	}
	var gate chan struct{}
	b := newBubble(t, func() {
		gate = make(chan struct{})
		if context == "delivery" {
			<-gate
			return
		}
		if context == "child" {
			go invoke()
			go func() { peer = true }()
			<-gate
			return
		}
		go func() { peer = true }()
		invoke()
	})
	var activation bubble.Activation
	if context == "delivery" {
		step(t, b, bubble.Activation{})
		activation.Deliver = func(*bubble.Delivery) {
			go func() { peer = true }()
			invoke()
			close(gate)
		}
	}
	s, err := b.Step(activation)
	if !deferred || after {
		t.Fatalf("defer=%v operation returned=%v", deferred, after)
	}
	if policy == "recovered" {
		if err != nil || recovered != diagnostic || !valid || !peer || s.Status == bubble.Faulted {
			t.Fatalf("recovery: state=%+v err=%v recovered=%v valid=%v peer=%v", s, err, recovered, valid, peer)
		}
		if context == "child" {
			s = step(t, b, bubble.Activation{Deliver: func(*bubble.Delivery) { close(gate) }})
		}
		requireCompleted(t, s)
	} else {
		if err == nil || !strings.Contains(err.Error(), diagnostic) || s.Status != bubble.Faulted || peer {
			t.Fatalf("fault: state=%+v err=%v peer=%v", s, err, peer)
		}
		if _, err := b.Step(bubble.Activation{}); err == nil {
			t.Fatal("faulted execution resumed")
		}
	}
	if err := b.Discard(); err != nil {
		t.Fatal(err)
	}
	check := func() {
		if len(external) != 1 || cap(external) != 2 {
			t.Fatalf("foreign channel changed: len=%d cap=%d", len(external), cap(external))
		}
		if value, ok := <-external; !ok || value != 7 {
			t.Fatalf("foreign message changed: value=%d open=%v", value, ok)
		}
		select {
		case _, ok := <-external:
			t.Fatalf("foreign channel modified: open=%v", ok)
		default:
		}
	}
	if owner == nil {
		check()
	} else {
		requireCompleted(t, step(t, owner, bubble.Activation{Deliver: func(*bubble.Delivery) {
			check()
			close(ownerGate)
		}}))
	}
}

func TestChannelRejectionPreservesSelectEvaluation(t *testing.T) {
	external := make(chan int, 1)
	external <- 7
	var evaluated []string
	var recovered any
	b := newBubble(t, func() {
		owned := func() chan int { evaluated = append(evaluated, "owned"); return make(chan int) }
		value := func() int { evaluated = append(evaluated, "value"); return 9 }
		foreign := func() chan int { evaluated = append(evaluated, "foreign"); return external }
		defer func() { recovered = recover() }()
		select {
		case owned() <- value():
		case <-foreign():
		default:
		}
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if recovered == nil || strings.Join(evaluated, ",") != "owned,value,foreign" || len(external) != 1 || <-external != 7 {
		t.Fatalf("recovered=%v evaluated=%v", recovered, evaluated)
	}
}

func TestChannelOwnershipClosedReceive(t *testing.T) {
	external := make(chan int, 1)
	external <- 7
	close(external)
	var recovered any
	b := newBubble(t, func() {
		defer func() { recovered = recover() }()
		<-external
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if recovered == nil || len(external) != 1 || <-external != 7 {
		t.Fatalf("recovered=%v remaining=%d", recovered, len(external))
	}
}

func TestChannelOwnershipNilAndOwned(t *testing.T) {
	var nilChannel chan int
	var received int
	b := newBubble(t, func() {
		if len(nilChannel) != 0 || cap(nilChannel) != 0 {
			panic("nil channel has nonzero size")
		}
		select {
		case <-nilChannel:
			panic("nil channel received")
		default:
		}
		owned := make(chan int)
		go func() { owned <- 7 }()
		received = <-owned
		close(owned)
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if received != 7 {
		t.Fatalf("owned channel received %d", received)
	}
}
