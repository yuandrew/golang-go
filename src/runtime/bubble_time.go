// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import bubbleapi "internal/runtime/bubble"

// deterministicTimer holds registration metadata only for host-controlled
// timers. Its owner is immutable; the remaining fields are accessed only by
// that bubble's single executing goroutine or exclusive delivery callback.
type deterministicTimer struct {
	owner      *synctestBubble
	id         uint64
	generation uint64
	armed      bool
}

// deterministicTimers is accessed only during exclusive bubble execution or
// while the bubble is paused. Registrations are retained until Close so that
// stale host events can be distinguished from unknown timer IDs.
type deterministicTimers struct {
	registered []*timer
	changes    []bubbleapi.TimerChange
}

func (t *timer) deterministicRegister(b *synctestBubble) {
	ts := &b.deterministic.timers
	t.deterministic = &deterministicTimer{owner: b, id: uint64(len(ts.registered)) + 1}
	ts.registered = append(ts.registered, t)
}

func (t *timer) deterministicCheckOwner() {
	b := getg().bubble
	var owner *synctestBubble
	if d := t.deterministic; d != nil {
		owner = d.owner
	}
	if owner != nil && owner != b ||
		b != nil && b.deterministic != nil && owner != b {
		panic(errorString("runtime/bubble: timer belongs to another execution domain"))
	}
}

// deterministicDisarm records cancellation after the native timer operation
// has released its locks. Appending may allocate and must run on a user stack.
func (t *timer) deterministicDisarm() {
	d := t.deterministic
	if !d.armed {
		return
	}
	ts := &d.owner.deterministic.timers
	ts.changes = append(ts.changes, bubbleapi.TimerChange{
		Kind:       1,
		ID:         d.id,
		Generation: d.generation,
	})
	d.armed = false
}

// deterministicModified follows the ordinary modify path, including draining
// any previously authorized but unreceived channel value.
func (t *timer) deterministicModified(when int64) {
	t.deterministicDisarm()
	d := t.deterministic
	d.generation++
	if d.generation == 0 {
		throw("runtime/bubble: timer generation overflow")
	}
	b := d.owner
	if t.isChan && when <= b.now {
		// Nonpositive channel timers are ready locally. Their send uses the
		// native timer path, so blocked receivers enter the bubble FIFO and
		// buffered values remain subject to Stop/Reset draining.
		systemstack(func() {
			t.lock()
			t.unlockAndRun(b.now, b)
		})
		return
	}
	ts := &b.deterministic.timers
	ts.changes = append(ts.changes, bubbleapi.TimerChange{
		Kind:       0,
		ID:         d.id,
		Generation: d.generation,
		Deadline:   when,
	})
	d.armed = true
}

func (b *synctestBubble) deterministicTimerChanges() []bubbleapi.TimerChange {
	ts := &b.deterministic.timers
	changes := ts.changes
	// The host owns this snapshot; never reuse its backing array.
	ts.changes = nil
	return changes
}

func (b *synctestBubble) deterministicFireTimer(id, generation uint64) bool {
	ts := &b.deterministic.timers
	if id == 0 || id > uint64(len(ts.registered)) {
		panic(errorString("runtime/bubble: unknown timer ID"))
	}
	t := ts.registered[id-1]
	d := t.deterministic
	if generation == 0 || generation > d.generation {
		panic(errorString("runtime/bubble: unknown timer generation"))
	}
	if generation != d.generation || !d.armed {
		return false
	}
	if b.now < t.when {
		panic(errorString("runtime/bubble: timer fired before its deadline"))
	}
	d.armed = false
	// unlockAndRun computes delay from the scheduled deadline, preserving
	// time.Timer.C's original timestamp even when the host fires it late.
	systemstack(func() {
		t.lock()
		t.unlockAndRun(b.now, b)
	})
	return true
}
