package bubble_test

import (
	"runtime/bubble"
	"testing"
)

const legacyStrict = "cooperative-v1"
const legacyPrefix = "cooperative-v2-retirement"

// Removed profiles must reject before their workflow can execute.
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
