// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"fmt"
	rand "math/rand"
	randv2 "math/rand/v2"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"runtime/bubble"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestReplayFreshProcesses compares observable execution across fresh runtime
// seeds, processor counts, GC policies, and unrelated host work. For the larger
// acceptance run, set BUBBLE_REPLAY_PROCESSES=1000 explicitly.
func TestReplayFreshProcesses(t *testing.T) {
	if os.Getenv("GO_BUBBLE_REPLAY_CHILD") == "1" {
		runReplayChild(t)
		return
	}
	processes := 24
	if configured := os.Getenv("BUBBLE_REPLAY_PROCESSES"); configured != "" {
		var err error
		processes, err = strconv.Atoi(configured)
		if err != nil || processes < 1 {
			t.Fatal("BUBBLE_REPLAY_PROCESSES must be a positive integer")
		}
	}
	var want string
	for i := 0; i < processes; i++ {
		procs := []string{"1", "2", "8"}[i%3]
		gc := []string{"20", "100", "off"}[(i/3)%3]
		load := strconv.Itoa((i / 9) % 2)
		cmd := exec.Command(os.Args[0], "-test.run=^TestReplayFreshProcesses$", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), "GO_BUBBLE_REPLAY_CHILD=1", "GOMAXPROCS="+procs, "GOGC="+gc, "GO_BUBBLE_REPLAY_LOAD="+load)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("process %d (GOMAXPROCS=%s GOGC=%s load=%s): %v\n%s", i, procs, gc, load, err, output)
		}
		var got string
		for _, line := range strings.Split(string(output), "\n") {
			if strings.HasPrefix(line, "BUBBLE_REPLAY_TRACE=") {
				if got != "" {
					t.Fatalf("process %d emitted multiple traces", i)
				}
				got = line
			}
		}
		if got == "" {
			t.Fatalf("process %d emitted no trace: %s", i, output)
		}
		if i == 0 {
			want = got
		} else if got != want {
			t.Fatalf("process %d (GOMAXPROCS=%s GOGC=%s load=%s) replay diverged\nwant %s\n got %s", i, procs, gc, load, want, got)
		}
	}
}

func runReplayChild(t *testing.T) {
	// Background workers never touch bubble state. Their allocations and CPU
	// work perturb host scheduling and GC without becoming application inputs.
	if os.Getenv("GO_BUBBLE_REPLAY_LOAD") == "1" {
		stop := make(chan struct{})
		var workers sync.WaitGroup
		for i := 0; i < 2; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				var retained [8][]byte
				for n := 0; ; n++ {
					select {
					case <-stop:
						return
					default:
					}
					p := make([]byte, 32<<10)
					for j := range p {
						p[j] = byte(n + j)
					}
					retained[n%len(retained)] = p
					_ = randv2.Uint64() // unrelated host entropy must not enter the bubble
					runtime.KeepAlive(retained)
					if n%32 == 0 {
						runtime.GC()
					}
					runtime.Gosched()
				}
			}()
		}
		defer func() { close(stop); workers.Wait() }()
	}

	var trace []uint64
	b := newBubble(t, func() {
		begin := make(chan struct{})
		left, right := make(chan uint64, 128), make(chan uint64, 128)
		for worker := 0; worker < 3; worker++ {
			go func() {
				<-begin
				for round := 0; round < 32; round++ {
					// The helper allocates on the heap. CPU work is finite and
					// contains no clock-dependent termination condition.
					payload := replayPayload()
					checksum := uint64(worker + round)
					for j := range payload {
						payload[j] = byte(j + round)
						checksum = checksum*33 + uint64(payload[j])
					}
					runtime.KeepAlive(payload)
					value := uint64(worker)<<56 | uint64(round)<<48 | checksum&((1<<48)-1)
					if round%2 == 0 {
						value ^= rand.Uint64()
					} else {
						value ^= randv2.Uint64()
					}
					select {
					case left <- value:
					case right <- value:
					}
					if round%4 == 0 {
						runtime.Gosched()
					}
				}
			}()
		}
		close(begin)
		runtime.Gosched()
		cases := []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(left)},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(right)},
		}
		for i := 0; i < 96; i++ {
			var value uint64
			var chosen int
			if i%5 == 0 {
				var received reflect.Value
				chosen, received, _ = reflect.Select(cases)
				value = received.Uint()
			} else {
				select {
				case value = <-left:
				case value = <-right:
					chosen = 1
				}
			}
			trace = append(trace, value, uint64(chosen))
			if i%7 == 0 {
				runtime.Gosched()
			}
		}
		time.Sleep(time.Second)
		trace = append(trace, uint64(time.Now().UnixNano()), randv2.Uint64())
	})
	s := step(t, b, bubble.Activation{})
	if s.Status != bubble.Quiescent || len(s.TimerChanges) != 1 {
		t.Fatalf("replay timer boundary: %+v", s)
	}
	arm := s.TimerChanges[0]
	s = step(t, b, bubble.Activation{Now: start.Add(2 * time.Second)})
	if s.Status != bubble.Quiescent || len(s.TimerChanges) != 0 {
		t.Fatalf("clock-only activation escaped quiescence: %+v", s)
	}
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(d *bubble.Delivery) {
		if !d.FireTimer(arm.ID, arm.Generation) {
			panic("expected a live timer generation")
		}
	}}))
	fmt.Printf("BUBBLE_REPLAY_TRACE=%x\n", trace)
}

//go:noinline
func replayPayload() []byte {
	return make([]byte, 64<<10)
}
