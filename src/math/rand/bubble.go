// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rand

import (
	runtimebubble "internal/runtime/bubble"
	"sync"
)

func newBubbleRand() any {
	return New(new(bubbleSource))
}

// bubbleSource adapts a bubble's application uint64 stream to the legacy API.
// The runtime synchronizes source draws. Read separately protects the Rand's
// byte buffer, including calls which consume no new source word.
type bubbleSource struct {
	readMu sync.Mutex
}

func (*bubbleSource) Int63() int64   { return int64(runtimebubble.Random() & rngMask) }
func (*bubbleSource) Uint64() uint64 { return runtimebubble.Random() }
func (*bubbleSource) Seed(int64)     {}

func (s *bubbleSource) read(p []byte, val *int64, pos *int8) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	return read(p, s, val, pos)
}
