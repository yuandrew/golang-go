// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build race

package bubble_test

import (
	"bytes"
	"os"
	"os/exec"
	"runtime"
	"runtime/bubble"
	"strings"
	"sync"
	"testing"
)

func TestWaitGroupRaceControls(t *testing.T) {
	if mode := os.Getenv("GO_BUBBLE_WAITGROUP_RACE"); mode != "" {
		raceCase(t, mode)
		return
	}
	for _, mode := range []string{"waiters-race", "after-done-race", "ordinary-after-done-race", "ordinary-publication-valid", "publication-valid", "host-edges-valid", "panic-balance-valid"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestWaitGroupRaceControls$", "-test.timeout=30s")
			cmd.Env = append(os.Environ(), "GO_BUBBLE_WAITGROUP_RACE="+mode, "GORACE=halt_on_error=1 exitcode=66")
			output, err := cmd.CombinedOutput()
			expected := strings.HasSuffix(mode, "-race")
			if expected {
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.ExitCode() != 66 || !bytes.Contains(output, []byte("WARNING: DATA RACE")) {
					t.Fatalf("race not detected: %v\n%s", err, output)
				}
			} else if err != nil || bytes.Contains(output, []byte("WARNING: DATA RACE")) {
				t.Fatalf("valid synchronization failed: %v\n%s", err, output)
			}
		})
	}
}

func raceCase(t *testing.T, mode string) {
	var value int
	if strings.HasPrefix(mode, "ordinary-") {
		var ordinary sync.WaitGroup
		if mode == "ordinary-publication-valid" {
			ordinary.Go(func() { value = 7 })
			ordinary.Wait()
			if value != 7 {
				t.Fatal(value)
			}
			return
		}
		ordinary.Add(1)
		done := make(chan struct{})
		go func() { ordinary.Wait(); _ = value; close(done) }()
		ordinary.Done()
		value = 2
		<-done
		return
	}
	var wg *sync.WaitGroup
	var gate chan struct{}
	b, _ := bubble.New(bubble.Options{}, func() {
		wg = new(sync.WaitGroup)
		wg.Add(1)
		gate = make(chan struct{})
		switch mode {
		case "waiters-race":
			for range 2 {
				go func() { value++; wg.Wait() }()
			}
			<-gate
		case "after-done-race":
			go func() { wg.Wait(); _ = value }()
			runtime.Gosched()
			wg.Done()
			value = 2
		case "publication-valid":
			go func() {
				wg.Wait()
				if value != 7 {
					panic("lost Done/Wait publication")
				}
			}()
			runtime.Gosched()
			value = 7
			wg.Done()
		case "host-edges-valid":
			value = 7
			wg.Wait()
			if value != 8 {
				panic("lost host activation publication")
			}
		case "panic-balance-valid":
			wg.Wait()
		default:
			panic("unknown mode")
		}
	})
	s, err := b.Step(bubble.Activation{})
	if err != nil {
		t.Fatal(s, err)
	}
	switch mode {
	case "host-edges-valid":
		if value != 7 {
			t.Fatal("lost quiescence publication")
		}
		value = 8
		s, err = b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { wg.Done() }})
		if err != nil || s.Status != bubble.Completed {
			t.Fatal(s, err)
		}
		if err = b.Close(); err != nil {
			t.Fatal(err)
		}
	case "waiters-race":
		if err = b.Discard(); err != nil {
			t.Fatal(err)
		}
	case "panic-balance-valid":
		if err = b.Discard(); err != nil {
			t.Fatal(err)
		}
		for _, f := range []func(){wg.Wait, wg.Done, func() { wg.Add(1) }, func() { wg.Go(func() {}) }} {
			func() {
				defer func() {
					if recover() == nil {
						t.Error("missing tombstone panic")
					}
				}()
				f()
			}()
			ch := make(chan struct{})
			var published int
			go func() { published = 9; close(ch) }()
			<-ch
			if published != 9 {
				t.Fatal("publication after recovered panic failed")
			}
		}
	default:
		if s.Status != bubble.Completed {
			t.Fatal(s)
		}
		if err = b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
