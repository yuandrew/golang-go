// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"internal/runtime/atomic"
	bubbleapi "internal/runtime/bubble"
	"unsafe"
)

//go:linkname waitGroupUnassociatedAdd sync.waitGroupUnassociatedAdd
func waitGroupUnassociatedAdd() {
	b := getg().bubble
	if b != nil && b.deterministic != nil {
		fatal("sync: WaitGroup execution domain mismatch")
	}
}

//go:linkname waitGroupWaitCheck sync.waitGroupWaitCheck
func waitGroupWaitCheck(p unsafe.Pointer, state uint64) {
	const associatedFlag = uint64(0x80000000)
	b := getg().bubble
	replay := b != nil && b.deterministic != nil
	if state&associatedFlag == 0 {
		if replay && int32(state>>32) != 0 {
			fatal("sync: WaitGroup execution domain mismatch")
		}
		return
	}
	span := spanOfHeap(uintptr(p))
	if span == nil {
		if replay {
			fatal("sync: WaitGroup execution domain mismatch")
		}
		return
	}
	mp := acquirem()
	span.ensureSwept()
	offset := uintptr(p) - span.base()
	lock(&span.speciallock)
	iter, exists := span.specialFindSplicePoint(offset, _KindSpecialBubble)
	var owner uint64
	var ownerReplay bool
	if exists {
		s := (*specialBubble)(unsafe.Pointer(*iter))
		owner = s.bubbleid
		ownerReplay = s.replay
	}
	unlock(&span.speciallock)
	releasem(mp)
	if replay || ownerReplay {
		if b == nil || b.id != owner {
			fatal("sync: WaitGroup execution domain mismatch")
		}
	}
}

type bubbleWaitGroup struct {
	gp     *g
	group  unsafe.Pointer
	state  *uint64
	sema   *uint32
	waiter *sudog
}

// shortcut: linear waiter lookup makes disposal quadratic; index for larger waiter sets.
func (d *deterministicBubble) waitGroupFor(gp *g) *bubbleWaitGroup {
	for i := range d.waitGroups {
		if d.waitGroups[i].gp == gp {
			return &d.waitGroups[i]
		}
	}
	return nil
}

func (d *deterministicBubble) removeWaitGroup(gp *g) {
	for i := range d.waitGroups {
		if d.waitGroups[i].gp == gp {
			copy(d.waitGroups[i:], d.waitGroups[i+1:])
			d.waitGroups[len(d.waitGroups)-1] = bubbleWaitGroup{}
			d.waitGroups = d.waitGroups[:len(d.waitGroups)-1]
			return
		}
	}
	throw("runtime/bubble: missing WaitGroup return")
}

func (root *semaRoot) bubbleWaitHead(addr *uint32) *sudog {
	for s := root.treap; s != nil; {
		if uintptr(unsafe.Pointer(addr)) == s.elem.uintptr() {
			return s
		}
		if uintptr(unsafe.Pointer(addr)) < s.elem.uintptr() {
			s = s.prev
		} else {
			s = s.next
		}
	}
	return nil
}

func (b *goroutineBubble) preflightWaitGroups() bool {
	d := b.deterministic
	// Build the index before taking shared semaphore locks.
	records := make(map[*g]*bubbleWaitGroup, len(d.waitGroups))
	for i := range d.waitGroups {
		entry := &d.waitGroups[i]
		records[entry.gp] = entry
	}

	for i, entry := range d.waitGroups {
		first := true
		for j := 0; j < i; j++ {
			if d.waitGroups[j].sema == entry.sema {
				first = false
				break
			}
		}
		if !first {
			continue
		}
		state := atomic.Load64(entry.state)
		count := 0
		queued := 0
		for _, other := range d.waitGroups {
			if other.sema == entry.sema {
				if other.group != entry.group || other.state != entry.state || other.gp.bubble != b || other.waiter == nil {
					return false
				}
				count++
				if other.waiter.elem.get() != nil {
					if other.waiter.elem.get() != unsafe.Pointer(entry.sema) {
						return false
					}
					queued++
				}
			}
		}
		association := getOrSetBubbleSpecial(entry.group, b.id, false)
		if queued != 0 {
			if queued != count || int32(state>>32) <= 0 || state&0x80000000 == 0 || int(state&0x7fffffff) != queued || association != bubbleAssocCurrentBubble {
				return false
			}
		} else if state != 0 || association == bubbleAssocOtherBubble {
			return false
		}
		root := semtable.rootFor(entry.sema)
		lockWithRank(&root.lock, lockRankRoot)
		seen := 0
		valid := true
		for s := root.bubbleWaitHead(entry.sema); s != nil; s = s.waitlink {
			registered := records[s.g]
			if s.g.bubble != b || registered == nil || registered.sema != entry.sema || registered.waiter != s {
				valid = false
				break
			}
			seen++
		}
		valid = valid && seen == queued && root.nwait.Load() >= uint32(seen)
		unlock(&root.lock)
		if !valid {
			return false
		}
	}
	return true
}

// All stacks and group queues have passed preflight; ownership excludes writers.
func (b *goroutineBubble) detachWaitGroups() {
	d := b.deterministic
	for i, entry := range d.waitGroups {
		first := true
		for j := 0; j < i; j++ {
			if d.waitGroups[j].sema == entry.sema {
				first = false
				break
			}
		}
		if !first {
			continue
		}
		root := semtable.rootFor(entry.sema)
		lockWithRank(&root.lock, lockRankRoot)
		for root.bubbleWaitHead(entry.sema) != nil {
			s, _, _ := root.dequeue(entry.sema)
			if s == nil {
				throw("runtime/bubble: missing semaphore waiter")
			}
			root.nwait.Add(-1)
		}
		atomic.Store(entry.sema, 0)
		atomic.Store64(entry.state, bubbleapi.DiscardedWaitGroupState)
		unlock(&root.lock)
	}
}
