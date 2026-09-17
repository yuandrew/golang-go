// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bubble_test

import (
	"bytes"
	rand "math/rand"
	randv2 "math/rand/v2"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"runtime/bubble"
	"strings"
	"sync"
	"testing"
)

type randomLegacyAdapter struct{ source randv2.Source }

func (a randomLegacyAdapter) Int63() int64   { return int64(a.source.Uint64() & ((1 << 63) - 1)) }
func (a randomLegacyAdapter) Uint64() uint64 { return a.source.Uint64() }
func (randomLegacyAdapter) Seed(int64)       { panic("unexpected reference Seed") }

type randomSourceFunc func() uint64

func (f randomSourceFunc) Uint64() uint64 { return f() }

func newRandomBubble(t *testing.T, options bubble.Options, f func()) *bubble.Bubble {
	t.Helper()
	b, err := bubble.New(options, f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close: %v; %s", err, b.StackTrace())
		}
	})
	return b
}

func TestRandomDefaultAndSuppliedSource(t *testing.T) {
	for _, seed := range []uint64{0, 1, 17, 1<<63 + 23} {
		for _, supplied := range []bool{false, true} {
			options := bubble.Options{Seed: seed}
			secondSeed := uint64(0)
			if supplied {
				secondSeed = 31
				options.RandomSource = randv2.NewPCG(seed, secondSeed)
			}
			var got []uint64
			var derived []any
			b := newRandomBubble(t, options, func() {
				got = []uint64{rand.Uint64(), randv2.Uint64(), uint64(rand.Int63()), randv2.Uint64(), rand.Uint64()}
				derived = []any{rand.Intn(73), randv2.IntN(91), rand.Float64(), randv2.Float64(), rand.Perm(8)}
			})
			requireCompleted(t, step(t, b, bubble.Activation{}))
			reference := randv2.NewPCG(seed, secondSeed)
			want := []uint64{reference.Uint64(), reference.Uint64(), reference.Uint64() & ((1 << 63) - 1), reference.Uint64(), reference.Uint64()}
			legacy := rand.New(randomLegacyAdapter{reference})
			modern := randv2.New(reference)
			wantDerived := []any{legacy.Intn(73), modern.IntN(91), legacy.Float64(), modern.Float64(), legacy.Perm(8)}
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(derived, wantDerived) {
				t.Fatalf("seed=%d supplied=%v: words=%v want=%v; derived=%v want=%v", seed, supplied, got, want, derived, wantDerived)
			}
		}
	}
}

func TestRandomSourcePersistenceAndInheritance(t *testing.T) {
	var gate chan struct{}
	var got []uint64
	var delivered uint64
	b := newRandomBubble(t, bubble.Options{RandomSource: randv2.NewPCG(4, 9)}, func() {
		gate = make(chan struct{})
		values := make(chan uint64)
		got = append(got, rand.Uint64())
		go func() { values <- randv2.Uint64() }()
		got = append(got, <-values)
		<-gate
		got = append(got, rand.Uint64())
	})
	s := step(t, b, bubble.Activation{})
	if s.Status != bubble.Quiescent || len(got) != 2 {
		t.Fatalf("initial draw boundary: state=%+v values=%v", s, got)
	}
	// Host draws cannot perturb the paused bubble's source.
	rand.Uint64()
	randv2.Uint64()
	requireCompleted(t, step(t, b, bubble.Activation{Deliver: func(*bubble.Delivery) {
		delivered = randv2.Uint64()
		close(gate)
	}}))
	reference := randv2.NewPCG(4, 9)
	want := []uint64{reference.Uint64(), reference.Uint64()}
	wantDelivery := reference.Uint64()
	want = append(want, reference.Uint64())
	if !reflect.DeepEqual(got, want) || delivered != wantDelivery {
		t.Fatalf("inherited persistent stream=%v delivery=%d; want=%v delivery=%d", got, delivered, want, wantDelivery)
	}
}

func TestRandomConcurrentIndependentBubbles(t *testing.T) {
	var controllers sync.WaitGroup
	for i := 0; i < 8; i++ {
		controllers.Add(1)
		go func() {
			defer controllers.Done()
			seed := uint64(i + 1)
			var got []uint64
			b, err := bubble.New(bubble.Options{RandomSource: randv2.NewPCG(seed, 8)}, func() {
				values := make(chan uint64, 4)
				for j := 0; j < 4; j++ {
					go func() {
						if j%2 == 0 {
							values <- rand.Uint64()
						} else {
							values <- randv2.Uint64()
						}
					}()
				}
				for j := 0; j < 4; j++ {
					got = append(got, <-values)
				}
			})
			if err != nil {
				t.Error(err)
				return
			}
			s, err := b.Step(bubble.Activation{})
			if err != nil || s.Status != bubble.Completed {
				t.Errorf("independent bubble: state=%+v err=%v", s, err)
			}
			if err := b.Close(); err != nil {
				t.Error(err)
			}
			reference := randv2.NewPCG(seed, 8)
			want := []uint64{reference.Uint64(), reference.Uint64(), reference.Uint64(), reference.Uint64()}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("seed %d: got %v want %v", seed, got, want)
			}
		}()
	}
	controllers.Wait()
}

func TestRandomDrawsDoNotChangeSelectChoices(t *testing.T) {
	run := func(extra bool) []int {
		var choices []int
		b := newRandomBubble(t, bubble.Options{Seed: 37}, func() {
			a, b := make(chan struct{}), make(chan struct{})
			close(a)
			close(b)
			for i := 0; i < 100; i++ {
				if extra {
					rand.Uint64()
					randv2.Uint64()
				}
				select {
				case <-a:
					choices = append(choices, 0)
				case <-b:
					choices = append(choices, 1)
				}
			}
		})
		requireCompleted(t, step(t, b, bubble.Activation{}))
		return choices
	}
	if plain, extra := run(false), run(true); !reflect.DeepEqual(plain, extra) {
		t.Fatalf("application draws changed scheduling choices: %v != %v", plain, extra)
	}
}

func TestRandomLegacyReadBuffersAcrossSteps(t *testing.T) {
	lengths := []int{1, 2, 5, 1, 17}
	type execution struct {
		bubble *bubble.Bubble
		gate   chan struct{}
		bytes  []byte
	}
	var executions [2]execution
	for i := range executions {
		execution := &executions[i]
		execution.bubble = newRandomBubble(t, bubble.Options{Seed: uint64(i + 7)}, func() {
			execution.gate = make(chan struct{}, 1)
			for j, length := range lengths {
				p := make([]byte, length)
				if n, err := rand.Read(p); n != length || err != nil {
					panic("unexpected random Read result")
				}
				execution.bytes = append(execution.bytes, p...)
				if j != len(lengths)-1 {
					<-execution.gate
				}
			}
		})
	}
	for round := range lengths {
		for i := range executions {
			execution := &executions[i]
			var activation bubble.Activation
			if round > 0 {
				activation.Deliver = func(*bubble.Delivery) { execution.gate <- struct{}{} }
			}
			s := step(t, execution.bubble, activation)
			if round == len(lengths)-1 {
				requireCompleted(t, s)
			} else if s.Status != bubble.Quiescent {
				t.Fatalf("read boundary: %+v", s)
			}
			rand.Read(make([]byte, round+1))
		}
	}
	for i := range executions {
		reference := rand.New(randomLegacyAdapter{randv2.NewPCG(uint64(i+7), 0)})
		want := make([]byte, len(executions[i].bytes))
		reference.Read(want)
		if !bytes.Equal(executions[i].bytes, want) {
			t.Fatalf("bubble %d Read stream=%x want=%x", i, executions[i].bytes, want)
		}
	}
}

func TestRandomLegacyReadInheritedBuffer(t *testing.T) {
	var got []byte
	b := newRandomBubble(t, bubble.Options{Seed: 29}, func() {
		values := make(chan byte, 4)
		// No legacy call precedes these children: the first one must safely
		// publish the cached generator to peers without hiding their races.
		for i := 0; i < 4; i++ {
			go func() {
				var p [1]byte
				rand.Read(p[:])
				values <- p[0]
			}()
		}
		for i := 0; i < 4; i++ {
			got = append(got, <-values)
		}
		var p [3]byte
		rand.Read(p[:])
		got = append(got, p[:]...)
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	var want [7]byte
	rand.New(randomLegacyAdapter{randv2.NewPCG(29, 0)}).Read(want[:])
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("Read buffer was not inherited: got=%x want=%x", got, want)
	}
}

func TestRandomSelectDoesNotConsumeSource(t *testing.T) {
	draws := uint64(0)
	var first, last uint64
	b := newRandomBubble(t, bubble.Options{RandomSource: randomSourceFunc(func() uint64 { draws++; return draws })}, func() {
		a, b := make(chan struct{}), make(chan struct{})
		close(a)
		close(b)
		first = rand.Uint64()
		for i := 0; i < 100; i++ {
			select {
			case <-a:
			case <-b:
			}
		}
		last = randv2.Uint64()
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if draws != 2 || first != 1 || last != 2 {
		t.Fatalf("scheduler consumed application source: draws=%d first=%d last=%d", draws, first, last)
	}
}

func TestRandomNoDrawAtConstructionOrClose(t *testing.T) {
	draws := 0
	source := randomSourceFunc(func() uint64 { draws++; return 42 })
	b, err := bubble.New(bubble.Options{RandomSource: source}, func() { randv2.Uint64() })
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if draws != 0 {
		t.Fatalf("constructing/closing paused bubble drew %d values", draws)
	}
	b = newRandomBubble(t, bubble.Options{RandomSource: source}, func() {})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if draws != 0 {
		t.Fatalf("nonrandom execution drew %d values", draws)
	}
}

func TestRandomExplicitGeneratorsUnchanged(t *testing.T) {
	var got []uint64
	b := newRandomBubble(t, bubble.Options{RandomSource: randomSourceFunc(func() uint64 { panic("unexpected package draw") })}, func() {
		legacy := rand.New(rand.NewSource(123))
		modern := randv2.New(randv2.NewPCG(123, 45))
		got = []uint64{legacy.Uint64(), modern.Uint64(), legacy.Uint64(), modern.Uint64()}
	})
	requireCompleted(t, step(t, b, bubble.Activation{}))
	legacy := rand.New(rand.NewSource(123))
	modern := randv2.New(randv2.NewPCG(123, 45))
	want := []uint64{legacy.Uint64(), modern.Uint64(), legacy.Uint64(), modern.Uint64()}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("explicit generator streams changed: %v want %v", got, want)
	}
}

func TestRandomSourcePanicRecovery(t *testing.T) {
	for _, read := range []bool{false, true} {
		calls := 0
		source := randomSourceFunc(func() uint64 {
			calls++
			if calls == 1 {
				panic("source failed")
			}
			return 0x0102030405060708
		})
		var recovered bool
		var got uint64
		var p [1]byte
		b := newRandomBubble(t, bubble.Options{RandomSource: source}, func() {
			recovered = rejectedOperation(func() {
				if read {
					rand.Read(p[:])
				} else {
					randv2.Uint64()
				}
			})
			if read {
				rand.Read(p[:])
			}
			got = rand.Uint64()
		})
		requireCompleted(t, step(t, b, bubble.Activation{}))
		if !recovered || got != 0x0102030405060708 || read && p[0] != 8 {
			t.Fatalf("read=%v panic recovery=%v value=%x byte=%x", read, recovered, got, p[0])
		}
	}
}

func TestRandomSeedHostIsolation(t *testing.T) {
	if os.Getenv("GO_BUBBLE_RANDOM_SEED_CHILD") == "1" {
		// The subprocess enables host Seed, making its exact pre/post stream
		// observable even when randautoseed has initialized a locked source.
		rand.Seed(87)
		hostReference := rand.New(rand.NewSource(87))
		if got, want := rand.Uint64(), hostReference.Uint64(); got != want {
			t.Fatalf("host Seed setup: got %d want %d", got, want)
		}
		var hostBefore, wantBefore [1]byte
		rand.Read(hostBefore[:])
		hostReference.Read(wantBefore[:])
		if hostBefore != wantBefore {
			t.Fatal("host Read setup did not match seeded reference")
		}
		var got []uint64
		b := newRandomBubble(t, bubble.Options{Seed: 5}, func() {
			got = append(got, rand.Uint64())
			rand.Seed(999)
			got = append(got, rand.Uint64(), randv2.Uint64())
			var p [2]byte
			rand.Read(p[:])
		})
		requireCompleted(t, step(t, b, bubble.Activation{}))
		reference := randv2.NewPCG(5, 0)
		want := []uint64{reference.Uint64(), reference.Uint64(), reference.Uint64()}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("bubble Seed changed stream: got %v want %v", got, want)
		}
		var hostAfter, wantAfter [10]byte
		rand.Read(hostAfter[:])
		hostReference.Read(wantAfter[:])
		if hostAfter != wantAfter {
			t.Fatalf("bubble changed host Read buffer: got=%x want=%x", hostAfter, wantAfter)
		}
		if got, want := rand.Uint64(), hostReference.Uint64(); got != want {
			t.Fatalf("bubble changed host stream: got %d want %d", got, want)
		}
		rand.Seed(42)
		if got, want := rand.Uint64(), rand.New(rand.NewSource(42)).Uint64(); got != want {
			t.Fatalf("host Seed no longer works: got %d want %d", got, want)
		}
		return
	}
	for _, auto := range []string{"0", "1"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRandomSeedHostIsolation$", "-test.timeout=15s")
		cmd.Env = append(os.Environ(), "GO_BUBBLE_RANDOM_SEED_CHILD=1", "GODEBUG="+os.Getenv("GODEBUG")+",randautoseed="+auto+",randseednop=0")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("randautoseed=%s: %v\n%s", auto, err, output)
		}
	}
}

func TestRandomSourceRecursion(t *testing.T) {
	for _, scenario := range []string{"legacy", "v2", "buffered-read"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			var recursive bool
			source := randomSourceFunc(func() uint64 {
				calls++
				if recursive {
					switch scenario {
					case "legacy":
						rand.Uint64()
					case "v2":
						randv2.Uint64()
					case "buffered-read":
						var p [1]byte
						rand.Read(p[:])
					}
				}
				return 0x12345678
			})
			var rejected bool
			var later uint64
			b := newRandomBubble(t, bubble.Options{RandomSource: source}, func() {
				if scenario == "buffered-read" {
					var p [1]byte
					rand.Read(p[:]) // Leaves six bytes buffered, so a guard must precede refill.
				}
				recursive = true
				rejected = rejectedOperation(func() { randv2.Uint64() })
				recursive = false
				later = rand.Uint64()
			})
			requireCompleted(t, step(t, b, bubble.Activation{}))
			if !rejected || later != 0x12345678 {
				t.Fatalf("recursion rejected=%v subsequent value=%x calls=%d", rejected, later, calls)
			}
		})
	}
}

func TestRandomSourceRestrictions(t *testing.T) {
	if scenario := os.Getenv("GO_BUBBLE_RANDOM_RESTRICTION"); scenario != "" {
		source := randomSourceFunc(func() uint64 {
			switch scenario {
			case "yield":
				runtime.Gosched()
			case "spawn":
				go func() {}()
			case "goexit":
				runtime.Goexit()
			case "block":
				<-make(chan struct{})
			}
			return 42
		})
		var rejected bool
		b := newRandomBubble(t, bubble.Options{RandomSource: source}, func() {
			rejected = rejectedOperation(func() { randv2.Uint64() })
		})
		requireCompleted(t, step(t, b, bubble.Activation{}))
		if !rejected {
			t.Fatal("random source restriction was not rejected")
		}
		return
	}
	for _, scenario := range []string{"yield", "spawn", "goexit", "block"} {
		t.Run(scenario, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestRandomSourceRestrictions$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), "GO_BUBBLE_RANDOM_RESTRICTION="+scenario)
			output, err := cmd.CombinedOutput()
			if err != nil && (!strings.Contains(string(output), "runtime/bubble: random source") || strings.Contains(string(output), "test timed out")) {
				t.Fatalf("restriction %s failed without an explicit source diagnostic: %v\n%s", scenario, err, output)
			}
		})
	}
}

func TestRandomSourceDeliveryFault(t *testing.T) {
	if os.Getenv("GO_BUBBLE_RANDOM_DELIVERY_FAULT") == "1" {
		b, err := bubble.New(bubble.Options{RandomSource: randomSourceFunc(func() uint64 { panic("source failed") })}, func() {
			<-make(chan struct{})
		})
		if err != nil {
			t.Fatal(err)
		}
		step(t, b, bubble.Activation{})
		s, err := b.Step(bubble.Activation{Deliver: func(*bubble.Delivery) { randv2.Uint64() }})
		if err == nil || s.Status != bubble.Faulted || s.LiveGoroutines != 1 {
			t.Fatalf("source panic did not fault delivery: state=%+v err=%v", s, err)
		}
		if _, err := b.Step(bubble.Activation{}); err == nil {
			t.Fatal("faulted source delivery resumed")
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRandomSourceDeliveryFault$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "GO_BUBBLE_RANDOM_DELIVERY_FAULT=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("source delivery fault: %v\n%s", err, output)
	}
}
