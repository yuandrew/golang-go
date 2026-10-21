// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import "unsafe"

// deterministicRandom is used only by the math/rand package-level sources.
// Do not redirect runtime.rand here: map hashes, allocation, GC and other
// runtime internals must not consume application state or call arbitrary Go.
// This function runs on the requesting user goroutine without runtime locks.
//
//go:nosplit
//go:linkname deterministicRandom internal/runtime/bubble.Random
func deterministicRandom() uint64 {
	b := getg().bubble
	if b != nil && b.deterministic != nil {
		return bubbleRandom(b.deterministic)
	}

	// Mirror rand's ordinary loop to avoid an extra call frame per draw.
	// Keep refill pinning identical; runtime-internal draws still use rand.
	mp := getg().m
	c := &mp.chacha8
	for {
		x, ok := c.Next()
		if ok {
			return x
		}
		mp.locks++
		c.Refill()
		mp.locks--
	}
}

// Keep callback/defer state off ordinary package-level random draws.
func bubbleRandom(d *deterministicBubble) uint64 {
	if d.randomActive {
		panic("runtime/bubble: random source must not call package-level randomness")
	}
	d.randomActive = true
	defer func() { d.randomActive = false }()
	return d.randomSource()
}

//go:linkname deterministicLegacyRand internal/runtime/bubble.LegacyRand
func deterministicLegacyRand(init func() any) any {
	b := getg().bubble
	if b == nil || b.deterministic == nil {
		return nil
	}
	d := b.deterministic
	// Check at the API entry, not just when fetching a fresh word: legacy Read
	// can satisfy a request entirely from bytes buffered by an earlier call.
	if d.randomActive {
		panic("runtime/bubble: random source must not call package-level randomness")
	}
	if d.legacyRandom == nil {
		d.legacyRandom = init()
		if raceenabled {
			// Publish the Rand's immutable source fields to peers that were
			// started before this goroutine initialized the legacy facade.
			racereleasemerge(unsafe.Pointer(&d.legacyRandom))
		}
	}
	if raceenabled {
		// Only initialization releases this address. Subsequent calls must
		// not create a blanket synchronization edge between user goroutines.
		raceacquire(unsafe.Pointer(&d.legacyRandom))
	}
	return d.legacyRandom
}
