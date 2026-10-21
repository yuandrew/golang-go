// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

func BubbleUnlinkQueues(send, recv, remove []int) (sendLeft, recvLeft []int) {
	c := new(hchan)
	lockInit(&c.lock, lockRankHchan)
	nodes := make(map[int]*sudog)
	for index, ids := range [][]int{send, recv} {
		q := &c.sendq
		if index == 1 {
			q = &c.recvq
		}
		for _, id := range ids {
			sg := &sudog{ticket: uint32(id)}
			sg.c.set(c)
			nodes[id] = sg
			q.enqueue(sg)
		}
	}
	for _, id := range remove {
		sg := nodes[id]
		deterministicUnlink(sg)
		if sg.prev != nil || sg.next != nil {
			panic("unlinked sudog retains queue links")
		}
	}
	remaining := func(q *waitq) []int {
		var ids []int
		var prev *sudog
		for sg := q.first; sg != nil; sg = sg.next {
			if sg.prev != prev {
				panic("broken channel queue back link")
			}
			ids = append(ids, int(sg.ticket))
			prev = sg
			if len(ids) > len(nodes) {
				panic("channel queue cycle")
			}
		}
		if q.last != prev {
			panic("broken channel queue tail")
		}
		return ids
	}
	return remaining(&c.sendq), remaining(&c.recvq)
}
