// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"internal/runtime/atomic"
	"unsafe"
)

func BubbleWaitGroupSnapshot(state *uint64, sema *uint32, group unsafe.Pointer) (uint64, uint32, int, uint32, int) {
	root := semtable.rootFor(sema)
	lockWithRank(&root.lock, lockRankRoot)
	queued := 0
	for s := root.bubbleWaitHead(sema); s != nil; s = s.waitlink {
		queued++
	}
	nwait := root.nwait.Load()
	tokens := atomic.Load(sema)
	value := atomic.Load64(state)
	unlock(&root.lock)
	return value, tokens, queued, nwait, getOrSetBubbleSpecial(group, 0, false)
}

func BubbleWaitGroupRegistry(handle any) (int, int, int) {
	d := handle.(*goroutineBubble).deterministic
	retained := 0
	for _, entry := range d.waitGroups[:cap(d.waitGroups)] {
		if entry.gp != nil || entry.group != nil || entry.state != nil || entry.sema != nil || entry.waiter != nil {
			retained++
		}
	}
	return len(d.waitGroups), cap(d.waitGroups), retained
}

func BubbleWaitGroupSameRoot(a, b *uint32) bool { return semtable.rootFor(a) == semtable.rootFor(b) }

func BubbleWaitGroupStacks(handle any) (uintptr, int) {
	b := handle.(*goroutineBubble)
	var total uintptr
	count := 0
	systemstack(func() {
		for _, entry := range b.deterministic.waitGroups {
			gp := entry.gp
			status := deterministicLock(gp)
			if status == 0 {
				throw("runtime/bubble: expected paused stack")
			}
			total += gp.stack.hi - gp.stack.lo
			count++
			casfrom_Gscanstatus(gp, status|_Gscan, status)
		}
	})
	return total, count
}
