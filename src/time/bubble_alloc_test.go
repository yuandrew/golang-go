//go:build bubbleexperiment && darwin && arm64

package time_test

import (
	"testing"
	"time"
)

// This measured stock budget excludes bubble-only registration metadata.
const stockTimerBytes = 248

func TestTimerAllocBudget(t *testing.T) {
	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			time.NewTimer(time.Hour).Stop()
		}
	})

	if got := result.AllocedBytesPerOp(); got > stockTimerBytes {
		t.Fatalf("ordinary timer allocates %d B/op; stock budget %d", got, stockTimerBytes)
	}
}
