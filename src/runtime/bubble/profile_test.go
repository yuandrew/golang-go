// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"runtime/bubble"
	"testing"
)

const legacyStrict = "cooperative-v1"
const legacyPrefix = "cooperative-v2-retirement"

// Unsupported protocols must be rejected before the initial function executes.
func TestOldProfileReject(t *testing.T) {
	for _, profile := range []string{legacyStrict, legacyPrefix} {
		t.Run(profile, func(t *testing.T) {
			ran := false
			b, err := bubble.New(bubble.Options{Protocol: profile}, func() { ran = true })
			if b != nil {
				_, _ = b.Step(bubble.Activation{})
				_ = b.Close()
			}

			if err == nil || b != nil || ran {
				t.Fatalf("rejected profile executed root: err=%v ran=%v", err, ran)
			}
		})
	}
}
