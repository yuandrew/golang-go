// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime

import (
	"internal/runtime/atomic"
	bubbleapi "internal/runtime/bubble"
	"internal/runtime/sys"
	"unsafe"
)

// deterministicBubble supplies an opt-in scheduling policy for synctestBubble.
// The ordinary scheduler sees only the logical owner. Other runnable goroutines
// remain _Grunnable (and visible to GC) in runq, not in a P or global run queue.
type deterministicBubble struct {
	activation   byte           // host-to-user race edge, separate from quiescence collection
	owner        atomic.Uintptr // *g, read without bubble.mu by GC-assist wakeups
	runq         gQueue         // protected by bubble.mu; no write barriers
	f            func()
	started      bool
	closed       bool
	disposing    bool
	failure      string
	stepping     bool
	waiting      bool // controller is waiting for quiescence
	delivering   bool
	random       uint64
	randomSource func() uint64
	randomActive bool
	legacyRandom any // *math/rand.Rand, including its independent Read buffer
	timers       deterministicTimers
}

// deterministicEnqueue reports whether gp was withheld from the normal scheduler.
// Called after gp becomes runnable, with a P, including from nowritebarrier paths.
func deterministicEnqueue(gp *g) bool {
	b := gp.bubble
	if b == nil || b.deterministic == nil {
		return false
	}
	d := b.deterministic
	// GC can resume the logical owner while holding assistQueue.lock. Do not
	// acquire bubble.mu in that path (nor change the logical schedule).
	if d.owner.Load() == uintptr(unsafe.Pointer(gp)) {
		return false
	}
	lock(&b.mu)
	if d.owner.Load() == 0 && d.stepping {
		d.owner.Store(uintptr(unsafe.Pointer(gp)))
		unlock(&b.mu)
		return false
	}
	d.runq.pushBack(gp)
	unlock(&b.mu)
	return true
}

// deterministicRelease ends a logical turn after a confirmed user park, explicit
// yield, or exit. Physical preemption and runtime/GC waits retain the owner.
// Called on g0 with a P. An exiting goroutine may still be in runtime cleanup,
// but must have finished all user code before releasing its turn.
func deterministicRelease(gp *g, yield bool) {
	b := gp.bubble
	d := b.deterministic
	lock(&b.mu)
	if d.owner.Load() != uintptr(unsafe.Pointer(gp)) {
		throw("runtime/bubble: releasing a non-owner")
	}
	d.owner.Store(0)
	if yield {
		d.runq.pushBack(gp)
	}
	var next *g
	// A fault pauses peers; teardown wakes the controller after the exit.
	if d.failure == "" || d.disposing {
		next = d.runq.pop()
	}
	if next != nil {
		d.owner.Store(uintptr(unsafe.Pointer(next)))
	}
	unlock(&b.mu)
	if next != nil {
		runqput(getg().m.p.ptr(), next, false)
		wakep()
	}
}

func (b *synctestBubble) deterministicFaultWake() {
	d := b.deterministic
	lock(&b.mu)
	var controller *g
	if d.failure != "" && !d.disposing && d.waiting {
		d.waiting = false
		controller = b.root
	}
	unlock(&b.mu)
	if controller != nil {
		goready(controller, 0)
	}
}

// deterministicSelect implements cooperative-v1's private SplitMix64 stream
// and rejection sampling. It is only consumed by the logical owner, never by
// runtime housekeeping or application randomness.
func (b *synctestBubble) deterministicSelect(n uint32) uint32 {
	d := b.deterministic
	threshold := -n % n
	for {
		d.random += 0x9e3779b97f4a7c15
		z := d.random
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		x := uint32((z ^ (z >> 31)) >> 32)
		if x >= threshold {
			return x % n
		}
	}
}

func deterministicCheckChan(c *hchan) {
	if c == nil {
		return
	}
	b := getg().bubble
	if b != nil && b.deterministic != nil && c.bubble != b {
		fatal("runtime/bubble: channel belongs to another execution domain")
	}
}

// Validate unsupported waits only on the opted-in scheduler path.
func bubblePark(gp *g, reason waitReason) {
	d := gp.bubble.deterministic
	if d == nil {
		return
	}
	if d.randomActive && reason.isIdleInSynctest() {
		fatal("runtime/bubble: random source must not block")
	}
	if d.delivering && reason.isIdleInSynctest() {
		fatal("runtime/bubble: delivery callback must not block")
	}
	if reason.isMutexWait() || reason == waitReasonSyncWaitGroupWait || reason == waitReasonIOWait {
		fatal("runtime/bubble: unsupported external blocking operation")
	}
}

//go:linkname deterministicNew internal/runtime/bubble.New
func deterministicNew(now int64, seed uint64, f func(), random func() uint64) any {
	if getg().bubble != nil {
		panic(errorString("runtime/bubble: nested bubble"))
	}
	b := &synctestBubble{
		id:            bubbleGen.Add(1),
		now:           now,
		deterministic: &deterministicBubble{f: f, random: seed, randomSource: random},
	}
	lockInit(&b.mu, lockRankSynctest)
	lockInit(&b.timers.mu, lockRankTimers)
	return b
}

//go:linkname deterministicStep internal/runtime/bubble.Step
func deterministicStep(handle any, now int64, deliver func()) (state bubbleapi.State) {
	b := handle.(*synctestBubble)
	d := b.deterministic
	gp := getg()
	if gp.bubble != nil || d.closed || d.stepping {
		panic(errorString("runtime/bubble: invalid activation"))
	}
	// The controller temporarily belongs to the bubble so native operations in
	// Deliver use the same ownership checks. It owns the first logical turn.
	b.now = now
	b.root = gp
	d.stepping = true
	d.owner.Store(uintptr(unsafe.Pointer(gp)))
	gp.bubble = b
	b.changegstatus(gp, _Gdead, _Grunning)
	defer func() {
		// No user goroutine can run while the controller owns the turn. This
		// also detaches safely when a control callback panics before execution.
		lock(&b.mu)
		d.delivering = false
		d.waiting = false
		d.stepping = false
		d.owner.Store(0)
		b.root = nil
		b.total--
		b.running--
		gp.bubble = nil
		unlock(&b.mu)
	}()
	if !d.started {
		d.started = true
		f := d.f
		d.f = nil
		pc := sys.GetCallerPC()
		systemstack(func() {
			fv := *(**funcval)(unsafe.Pointer(&f))
			b.main = newproc1(fv, gp, pc, false, waitReasonZero)
			if !deterministicEnqueue(b.main) {
				throw("runtime/bubble: root escaped paused activation")
			}
		})
	}
	if deliver != nil {
		d.delivering = true
		deliver()
		d.delivering = false
	}
	if raceenabled {
		racereleasemergeg(gp, unsafe.Pointer(&d.activation))
	}
	d.waiting = true
	gopark(deterministicIdle, nil, waitReasonSynctestRun, traceBlockSynctest, 0)
	if raceenabled {
		raceacquireg(gp, b.raceaddr())
	}
	state.RootDone = b.done
	state.Failure = d.failure
	state.LiveGoroutines = b.total - 1 // exclude the attached controller
	state.TimerChanges = b.deterministicTimerChanges()
	return state
}

func deterministicIdle(gp *g, _ unsafe.Pointer) bool {
	b := gp.bubble
	lock(&b.mu)
	// park_m holds one active reference. If nobody else can run, cancel the
	// park rather than manufacturing a new activation or advancing time.
	idle := b.running == 0 && b.active == 1
	if idle {
		b.deterministic.waiting = false
	}
	unlock(&b.mu)
	return !idle
}

//go:linkname deterministicSnapshot internal/runtime/bubble.Snapshot
func deterministicSnapshot(handle any) bubbleapi.State {
	b := handle.(*synctestBubble)
	return bubbleapi.State{
		Failure:        b.deterministic.failure,
		RootDone:       b.done,
		LiveGoroutines: b.total,
		TimerChanges:   b.deterministicTimerChanges(),
	}
}

//go:linkname deterministicClose internal/runtime/bubble.Close
func deterministicClose(handle any) bool {
	b := handle.(*synctestBubble)
	d := b.deterministic
	if d.stepping {
		return false
	}
	if b.total != 0 && !b.deterministicDispose() {
		return false
	}
	d.closed = true
	d.f = nil
	d.timers = deterministicTimers{}
	d.randomSource = nil
	d.legacyRandom = nil
	return true
}

// deterministicDispose discards paused execution without running user defers.
// Preflight all waits before detaching any; unsupported states remain usable.
func (b *synctestBubble) deterministicDispose() bool {
	d := b.deterministic
	stw := stopTheWorld(stwAllGoroutinesStack)
	for _, gp := range allGsSnapshot() {
		if gp.bubble != b {
			continue
		}
		status := readgstatus(gp)
		if status == _Grunnable {
			if gp.sched.lr != 0 && gp.sched.pc != gp.startpc {
				startTheWorld(stw)
				return false
			}
			continue
		}
		if status != _Gwaiting || !deterministicDisposable(gp) {
			startTheWorld(stw)
			return false
		}
		for sg := gp.waiting; sg != nil; sg = sg.waitlink {
			if sg.c.get() == nil || sg.c.get().bubble != b {
				startTheWorld(stw)
				return false
			}
		}
	}
	d.disposing = true
	for _, entry := range d.timers.registered {
		reg := entry.timer
		if reg == nil {
			continue
		}
		if reg.timer != nil {
			reg.timer.when = 0
		}
		reg.armed = false
		reg.timer = nil
	}
	f := deterministicExit
	fv := *(**funcval)(unsafe.Pointer(&f))
	for _, gp := range allGsSnapshot() {
		if gp.bubble != b {
			continue
		}
		deterministicUnlink(gp)
		// Insert a runtime-only exit frame; existing stack frames remain valid
		// for GC until normal scheduler teardown destroys this goroutine.
		if gp.sched.lr != 0 {
			// A never-started goroutine already has its goexit return frame.
			gp.sched.pc = fv.fn
			gp.sched.ctxt = unsafe.Pointer(fv)
		} else {
			gostartcallfn(&gp.sched, fv)
		}
		if readgstatus(gp) == _Gwaiting {
			goready(gp, 0)
		}
	}
	startTheWorld(stw)
	deterministicStep(b, b.now, nil)
	return b.total == 0
}

func deterministicDisposable(gp *g) bool {
	switch gp.waitreason {
	case waitReasonSynctestChanReceive, waitReasonSynctestChanSend,
		waitReasonSynctestSelect, waitReasonChanReceiveNilChan,
		waitReasonChanSendNilChan, waitReasonSelectNoCases, waitReasonSleep:
		return true
	}
	return false
}

// deterministicUnlink removes owned channel registrations before stack disposal.
// The world is stopped; channel locks also preserve stack-copying invariants.
func deterministicUnlink(gp *g) {
	for sg := gp.waiting; sg != nil; {
		next := sg.waitlink
		c := sg.c.get()
		lock(&c.lock)
		found := false
		for item := c.sendq.first; item != nil; item = item.next {
			if item == sg {
				c.sendq.dequeueSudoG(sg)
				found = true
				break
			}
		}
		if !found {
			c.recvq.dequeueSudoG(sg)
		}
		if c.timer != nil {
			unblockTimerChan(c)
		}
		unlock(&c.lock)
		sg.elem.set(nil)
		sg.c.set(nil)
		sg.isSelect = false
		sg.waitlink = nil
		releaseSudog(sg)
		sg = next
	}
	gp.waiting = nil
	gp.activeStackChans = false
	gp.selectDone.Store(0)
	gp.param = nil
}

func deterministicExit() {
	// A discarded goroutine may have parked while unwinding a panic.
	for p := getg()._panic; p != nil; p = p.link {
		if !p.goexit {
			runningPanicDefers.Add(-1)
		}
	}
	getg()._panic = nil
	goexit1()
}

//go:linkname deterministicFire internal/runtime/bubble.FireTimer
func deterministicFire(handle any, id, generation uint64) bool {
	b := handle.(*synctestBubble)
	if getg().bubble != b || !b.deterministic.delivering || getg() != b.root {
		panic(errorString("runtime/bubble: timer delivery outside activation"))
	}
	return b.deterministicFireTimer(id, generation)
}

//go:linkname deterministicRetireRange internal/runtime/bubble.RetireTimerRange
func deterministicRetireRange(handle any, first, last uint64) {
	b := handle.(*synctestBubble)
	if getg().bubble != b || !b.deterministic.delivering || getg() != b.root {
		panic(errorString("runtime/bubble: timer retirement outside activation"))
	}
	b.deterministic.timers.retireRange(first, last)
}

//go:linkname deterministicIsBubbled internal/runtime/bubble.IsBubbled
func deterministicIsBubbled() bool {
	return getg().bubble != nil
}

//go:linkname deterministicStackTrace internal/runtime/bubble.StackTrace
func deterministicStackTrace(handle any) string {
	b := handle.(*synctestBubble)
	for size := 4096; ; size *= 2 {
		buf := make([]byte, size)
		n := 0
		gp := getg()
		stw := stopTheWorld(stwAllGoroutinesStack)
		systemstack(func() {
			g0 := getg()
			g0.m.traceback = 1
			g0.writebuf = buf[:0:len(buf)]
			tracebacksomeothers(gp, func(other *g) bool { return other.bubble == b })
			n = len(g0.writebuf)
			g0.writebuf = nil
			g0.m.traceback = 0
		})
		startTheWorld(stw)
		if n < len(buf) {
			return string(buf[:n])
		}
	}
}
