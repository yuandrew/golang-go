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
	timer      *timer // strong only while awaiting host authorization
}

// deterministicTimers is accessed only during exclusive bubble execution or
// while paused. Retired one-shot IDs need no record: the high-water ID proves
// membership and their final generation is one. Resets retain sparse exceptions.
type deterministicTimers struct {
	ranges       []timerRange
	nextID       uint64
	registered   []activeTimer  // sorted IDs; retired entries release their payload
	holes        int            // empty records, compacted after at least half the table retires
	retired      []retiredTimer // nondefault generations, sorted by ID
	retiredAlloc int            // capacity of the backing allocation, including dropped prefixes
	changes      []bubbleapi.TimerChange
}

type timerRange struct {
	first, last uint64
}

type activeTimer struct {
	id    uint64
	timer *deterministicTimer
}

const timerCompactRatio = 2

type retiredTimer struct {
	id         uint64
	generation uint64
}

const (
	unarmedGeneration      uint64 = 0
	initialTimerGeneration uint64 = 1
)

const (
	deterministicTimerArmed uint8 = iota
	deterministicTimerDisarmed
)

// newID preserves generation-zero validation if initial arming fails.
func (ts *deterministicTimers) newID() uint64 {
	ts.nextID++
	if ts.nextID == 0 {
		throw("runtime/bubble: timer ID overflow")
	}
	ts.setRetired(ts.nextID, unarmedGeneration)
	return ts.nextID
}

// Keep bubble registration out of the ordinary timer allocation hot path.
//
//go:noinline
func (t *timer) deterministicRegister(b *synctestBubble) {
	ts := &b.deterministic.timers
	t.deterministic = &deterministicTimer{owner: b, id: ts.newID()}
}

// Range acknowledgments preserve active gaps and merge adjacent history.
// Storage follows the number of gaps, not the number of acknowledged IDs.
func (ts *deterministicTimers) retireRange(first, last uint64) {
	if first == 0 || first > last || last > ts.nextID {
		panic(errorString("runtime/bubble: invalid retirement range"))
	}
	for i := ts.activeIndex(first); i < len(ts.registered); i++ {
		entry := ts.registered[i]
		if entry.id > last {
			break
		}
		if entry.timer != nil {
			panic(errorString("runtime/bubble: retirement includes active timer"))
		}
	}

	start, end := ts.retiredIndex(first), ts.retiredIndex(last)
	if end < len(ts.retired) && ts.retired[end].id == last {
		end++
	}
	ts.dropRetired(start, end)
	ts.mergeRange(first, last)
}

// Drop leading records without shifting. Release excess backing storage only
// after half is unused; total copying stays amortized for ordered delivery.
func (ts *deterministicTimers) dropRetired(start, end int) {
	if start == end {
		return
	}
	if start == 0 {
		ts.retired = ts.retired[end:]
	} else {
		copy(ts.retired[start:], ts.retired[end:])
		ts.retired = ts.retired[:len(ts.retired)-(end-start)]
	}
	if len(ts.retired) == 0 {
		ts.retired = nil
		ts.retiredAlloc = 0
		return
	}
	if len(ts.retired)*timerCompactRatio > ts.retiredAlloc {
		return
	}
	retired := make([]retiredTimer, len(ts.retired))
	copy(retired, ts.retired)
	ts.retired = retired
	ts.retiredAlloc = len(retired)
}

// rangeIndex finds the first range whose end reaches id.
func (ts *deterministicTimers) rangeIndex(id uint64) int {
	lo, hi := 0, len(ts.ranges)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if ts.ranges[mid].last < id {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Merge only intersecting/adjacent records. Ordered additions append or extend;
// out-of-order insertions can still shift the sorted slice.
func (ts *deterministicTimers) mergeRange(first, last uint64) {
	start := ts.rangeIndex(first - 1)
	end := start
	for end < len(ts.ranges) {
		span := ts.ranges[end]
		if span.first > last && span.first-last > 1 {
			break
		}
		if span.first < first {
			first = span.first
		}
		if span.last > last {
			last = span.last
		}
		end++
	}
	if start == end {
		ts.ranges = append(ts.ranges, timerRange{})
		copy(ts.ranges[start+1:], ts.ranges[start:])
	} else {
		copy(ts.ranges[start+1:], ts.ranges[end:])
		ts.ranges = ts.ranges[:len(ts.ranges)-(end-start)+1]
	}
	ts.ranges[start] = timerRange{first, last}
	ts.compactRanges()
}

func (ts *deterministicTimers) compactRanges() {
	if len(ts.ranges) == 0 {
		ts.ranges = nil
		return
	}
	if len(ts.ranges)*timerCompactRatio > cap(ts.ranges) {
		return
	}
	ranges := make([]timerRange, len(ts.ranges))
	copy(ranges, ts.ranges)
	ts.ranges = ranges
}

func (ts *deterministicTimers) acknowledged(id uint64) bool {
	i := ts.rangeIndex(id)
	return i < len(ts.ranges) && ts.ranges[i].first <= id
}

func (ts *deterministicTimers) activeIndex(id uint64) int {
	lo, hi := 0, len(ts.registered)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if ts.registered[mid].id < id {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (ts *deterministicTimers) retiredIndex(id uint64) int {
	lo, hi := 0, len(ts.retired)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if ts.retired[mid].id < id {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (ts *deterministicTimers) forgetRetired(id uint64) {
	i := ts.retiredIndex(id)
	if i == len(ts.retired) || ts.retired[i].id != id {
		return
	}
	for j := i; j+1 < len(ts.retired); j++ {
		ts.retired[j] = ts.retired[j+1]
	}
	ts.retired = ts.retired[:len(ts.retired)-1]
}

func (ts *deterministicTimers) setRetired(id, generation uint64) {
	if generation == initialTimerGeneration {
		ts.forgetRetired(id)
		return
	}
	i := ts.retiredIndex(id)
	if i < len(ts.retired) && ts.retired[i].id == id {
		ts.retired[i].generation = generation
		return
	}
	capacity := cap(ts.retired)
	ts.retired = append(ts.retired, retiredTimer{})
	if cap(ts.retired) != capacity {
		ts.retiredAlloc = cap(ts.retired)
	}
	for j := len(ts.retired) - 1; j > i; j-- {
		ts.retired[j] = ts.retired[j-1]
	}
	ts.retired[i] = retiredTimer{id: id, generation: generation}
}

func (ts *deterministicTimers) arm(d *deterministicTimer) {
	i := ts.activeIndex(d.id)
	if i < len(ts.registered) && ts.registered[i].id == d.id {
		ts.registered[i].timer = d
		ts.holes--
		ts.forgetRetired(d.id)
		return
	}
	ts.registered = append(ts.registered, activeTimer{})
	for j := len(ts.registered) - 1; j > i; j-- {
		ts.registered[j] = ts.registered[j-1]
	}
	ts.registered[i] = activeTimer{id: d.id, timer: d}
	ts.forgetRetired(d.id)
}

// Shrink after substantial sparsity to avoid retaining a burst behind one timer.
const timerShrinkRatio = 4

// Compact in ID order so lookup and reset keep the same event protocol.
func (ts *deterministicTimers) compact() {
	n := 0
	for _, entry := range ts.registered {
		if entry.timer == nil {
			continue
		}
		ts.registered[n] = entry
		n++
	}
	for i := n; i < len(ts.registered); i++ {
		ts.registered[i] = activeTimer{}
	}
	ts.registered = ts.registered[:n]
	ts.holes = 0
}

func (d *deterministicTimer) deterministicRetire() {
	ts := &d.owner.deterministic.timers
	i := ts.activeIndex(d.id)
	if i < len(ts.registered) && ts.registered[i].timer == d {
		// Clear payload immediately; amortize shifts across many retirements.
		ts.registered[i].timer = nil
		ts.holes++
		if ts.holes*timerCompactRatio >= len(ts.registered) {
			ts.compact()
		}
	}
	ts.setRetired(d.id, d.generation)
	d.armed = false
	d.timer = nil
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
		Kind:       deterministicTimerDisarmed,
		ID:         d.id,
		Generation: d.generation,
	})
	d.deterministicRetire()
}

// deterministicModified follows the ordinary modify path, including draining
// any previously authorized but unreceived channel value.
func (t *timer) deterministicModified(when int64) {
	t.deterministicDisarm()
	d := t.deterministic
	ts := &d.owner.deterministic.timers
	if ts.acknowledged(d.id) {
		// Acknowledged IDs never become live again, even if Go code retains a timer.
		d.id = ts.newID()
		d.generation = unarmedGeneration
	}
	d.generation++
	if d.generation == 0 {
		throw("runtime/bubble: timer generation overflow")
	}
	b := d.owner
	if when <= b.now {
		d.deterministicRetire()
		// Nonpositive timers are ready locally. Native delivery queues owned
		// callback goroutines or channel receivers without host authorization.
		systemstack(func() {
			t.lock()
			t.unlockAndRun(b.now, b)
		})
		return
	}
	ts.changes = append(ts.changes, bubbleapi.TimerChange{
		Kind:       deterministicTimerArmed,
		ID:         d.id,
		Generation: d.generation,
		Deadline:   when,
	})
	d.armed = true
	d.timer = t
	ts.arm(d)
}

func (b *synctestBubble) deterministicTimerChanges() []bubbleapi.TimerChange {
	ts := &b.deterministic.timers
	if len(ts.registered) == 0 {
		// Release backing capacity after a large batch is entirely retired.
		ts.registered = nil
	} else if len(ts.registered) <= cap(ts.registered)/timerShrinkRatio {
		// Resize once per boundary, after clearing retired pointers immediately.
		n := len(ts.registered)
		entries := make([]activeTimer, n, n*timerCompactRatio)
		copy(entries, ts.registered)
		ts.registered = entries
	}
	changes := ts.changes
	// The host owns this snapshot; never reuse its backing array.
	ts.changes = nil
	return changes
}

func (b *synctestBubble) deterministicFireTimer(id, generation uint64) bool {
	ts := &b.deterministic.timers
	if id == 0 || id > ts.nextID {
		panic(errorString("runtime/bubble: unknown timer ID"))
	}
	if generation == unarmedGeneration {
		panic(errorString("runtime/bubble: unknown timer generation"))
	}
	if ts.acknowledged(id) {
		return false
	}
	var d *deterministicTimer
	i := ts.activeIndex(id)
	if i < len(ts.registered) && ts.registered[i].id == id {
		d = ts.registered[i].timer
	}
	maxGeneration := initialTimerGeneration
	if d != nil {
		maxGeneration = d.generation
	} else {
		i := ts.retiredIndex(id)
		if i < len(ts.retired) && ts.retired[i].id == id {
			maxGeneration = ts.retired[i].generation
		}
	}
	if generation == 0 || generation > maxGeneration {
		panic(errorString("runtime/bubble: unknown timer generation"))
	}
	if d == nil || generation != d.generation || !d.armed {
		return false
	}
	t := d.timer
	if b.now < t.when {
		panic(errorString("runtime/bubble: timer fired before its deadline"))
	}
	d.deterministicRetire()
	// unlockAndRun computes delay from the scheduled deadline, preserving
	// time.Timer.C's original timestamp even when the host fires it late.
	systemstack(func() {
		t.lock()
		t.unlockAndRun(b.now, b)
	})
	return true
}
