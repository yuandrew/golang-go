// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race

package bubble_test

import (
	"os"
	"os/exec"
	"runtime/bubble"
	"strings"
	"testing"
)

// Serial execution is not a user-level synchronization edge. The activation
// boundary must publish work to the controller without concealing peer races.
func TestBubblePeersStillReportDataRaces(t *testing.T) {
	if os.Getenv("GO_BUBBLE_RACE_CHILD") == "1" {
		var unsynchronized int
		b := newBubble(t, func() {
			done := make(chan struct{}, 2)
			go func() {
				unsynchronized = 1 // Intentional race, confined to this subprocess.
				done <- struct{}{}
			}()
			go func() {
				unsynchronized = 2
				done <- struct{}{}
			}()
			<-done
			<-done
		})
		requireCompleted(t, step(t, b, bubble.Activation{}))
		if unsynchronized != 2 {
			t.Fatalf("unexpected FIFO result %d", unsynchronized)
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestBubblePeersStillReportDataRaces$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "GO_BUBBLE_RACE_CHILD=1", "GORACE=halt_on_error=1 exitcode=66")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "WARNING: DATA RACE") {
		t.Fatalf("expected an unsynchronized peer race; err=%v\n%s", err, output)
	}
	if strings.Contains(string(output), "test timed out") {
		t.Fatalf("race subprocess timed out: %s", output)
	}
}
