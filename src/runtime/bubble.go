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

// deterministicBubble supplies an opt-in scheduling policy for goroutineBubble.
// The ordinary scheduler sees only the logical owner. Other runnable goroutines
// remain _Grunnable (and visible to GC) in runq, not in a P or global run queue.
type deterministicBubble struct {
	members      []*g           // live owned goroutines; exclusive logical execution
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
	waitGroups   []bubbleWaitGroup
	timers       deterministicTimers
}

const maxBubbleSlot = ^uint16(0)

// Small indices fit g's existing padding; larger executions retain search fallback.
func (d *deterministicBubble) deterministicTrack(gp *g) {
	if gp.bubbleSlot != 0 {
		throw("runtime/bubble: invalid member registration")
	}
	d.members = append(d.members, gp)
	deterministicSetSlot(gp, len(d.members))
}

func deterministicSetSlot(gp *g, slot int) {
	gp.bubbleSlot = 0
	if slot <= int(maxBubbleSlot) {
		gp.bubbleSlot = uint16(slot)
	}
}

func (d *deterministicBubble) deterministicUntrack(gp *g) {
	index := int(gp.bubbleSlot) - 1
	if index < 0 {
		// Overflow is a performance fallback, not a concurrency limit.
		for i, member := range d.members {
			if member == gp {
				index = i
				break
			}
		}
		if index < 0 {
			return
		}
	}
	last := len(d.members) - 1
	if index > last || d.members[index] != gp {
		throw("runtime/bubble: invalid member index")
	}
	moved := d.members[last]
	d.members[index] = moved
	deterministicSetSlot(moved, index+1)
	d.members[last] = nil
	d.members = d.members[:last]
	gp.bubbleSlot = 0
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

func (b *goroutineBubble) deterministicFaultWake() {
	d := b.deterministic
	lock(&b.mu)
	var controller *g
	// Return faults only after logical execution and runtime teardown stop.
	if d.owner.Load() == 0 && b.active == 0 && d.failure != "" && !d.disposing && d.waiting {
		d.waiting = false
		controller = b.root
	}
	unlock(&b.mu)
	if controller != nil {
		goready(controller, 0)
	}
}

// deterministicSelect uses a private SplitMix64 stream
// and rejection sampling. It is only consumed by the logical owner, never by
// runtime housekeeping or application randomness.
func (b *goroutineBubble) deterministicSelect(n uint32) uint32 {
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

const deterministicChannelAccessError = "runtime/bubble: channel belongs to another execution domain"

func (b *goroutineBubble) channelAccessError(legacy string) string {
	if b.deterministic != nil {
		return deterministicChannelAccessError
	}
	return legacy
}

func deterministicCheckChan(c *hchan) {
	if c == nil {
		return
	}
	b := getg().bubble
	if b != nil && b.deterministic != nil && c.bubble != b {
		panic(deterministicChannelAccessError)
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
	b := &goroutineBubble{
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
	b := handle.(*goroutineBubble)
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
		d.deterministicTrack(b.main)
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
	b := handle.(*goroutineBubble)
	return bubbleapi.State{
		Failure:        b.deterministic.failure,
		RootDone:       b.done,
		LiveGoroutines: b.total,
		TimerChanges:   b.deterministicTimerChanges(),
	}
}

//go:linkname deterministicClose internal/runtime/bubble.Close
func deterministicClose(handle any) bool {
	b := handle.(*goroutineBubble)
	d := b.deterministic
	if d.stepping || b.total != 0 {
		return false
	}
	d.clear()
	return true
}

//go:linkname deterministicDiscard internal/runtime/bubble.Discard
func deterministicDiscard(handle any) bool {
	b := handle.(*goroutineBubble)
	d := b.deterministic
	if d.stepping || b.total != 0 && !b.deterministicDispose() {
		return false
	}
	d.clear()
	return true
}

func (d *deterministicBubble) clear() {
	d.closed = true
	d.members = nil
	d.f = nil
	d.timers = deterministicTimers{}
	d.randomSource = nil
	d.legacyRandom = nil
	d.waitGroups = nil
}

// deterministicDispose discards paused execution without running user defers.
// Preflight all waits before detaching any; unsupported states remain usable.
func (b *goroutineBubble) deterministicDispose() bool {
	d := b.deterministic
	lock(&b.mu)
	stable := !d.stepping && d.owner.Load() == 0 && b.active == 0 && len(d.members) == b.total
	unlock(&b.mu)
	if !stable {
		return false
	}
	members := d.members

	// Claim one paused stack at a time; rejection must precede mutation.
	valid := true
	systemstack(func() {
		for _, gp := range members {
			status := deterministicLock(gp)
			if status == 0 {
				valid = false
				break
			}
			if entry := d.waitGroupFor(gp); entry != nil {
				valid = entry.waiter != nil && entry.waiter.g == gp
				if status == _Gwaiting {
					valid = valid && gp.waitreason == waitReasonSynctestWaitGroupWait && gp.waiting == entry.waiter && entry.waiter.elem.get() == unsafe.Pointer(entry.sema)
				} else {
					valid = valid && gp.waiting == nil && entry.waiter.elem.get() == nil
				}
			} else if status == _Grunnable {
				valid = gp.sched.lr == 0 || gp.sched.pc == gp.startpc
			} else {
				valid = deterministicDisposable(gp)
				for sg := gp.waiting; valid && sg != nil; sg = sg.waitlink {
					valid = sg.c.get() != nil && sg.c.get().bubble == b
				}
			}
			casfrom_Gscanstatus(gp, status|_Gscan, status)
			if !valid {
				break
			}
		}
	})
	if !valid || !b.preflightWaitGroups() {
		return false
	}

	d.disposing = true
	b.detachWaitGroups()
	for _, entry := range d.timers.registered {
		reg := entry.timer
		if reg == nil {
			continue
		}
		if reg.timer != nil {
			reg.timer.lock()
			reg.timer.when = 0
			reg.timer.unlock()
		}
		reg.armed = false
		reg.timer = nil
	}
	f := deterministicExit
	fv := *(**funcval)(unsafe.Pointer(&f))
	for _, gp := range members {
		var status uint32
		var waiting *sudog
		systemstack(func() {
			status = deterministicLock(gp)
			if status == 0 {
				throw("runtime/bubble: paused member changed state")
			}
			waiting = gp.waiting
			casfrom_Gscanstatus(gp, status|_Gscan, status)
		})
		// WaitGroup queues were detached as groups before any stack mutation.
		if entry := d.waitGroupFor(gp); entry != nil {
			waiting = entry.waiter
		} else {
			deterministicUnlink(waiting)
		}
		systemstack(func() {
			// Record the original stack before mutation. Staying on g0 prevents
			// a new profiling generation between this check and the rewrite.
			if goroutineProfile.active {
				tryRecordGoroutineProfile(gp, nil, osyield)
			}
			status = deterministicLock(gp)
			if status == 0 {
				throw("runtime/bubble: detached member changed state")
			}
			for sg := waiting; sg != nil; sg = sg.waitlink {
				sg.elem.set(nil)
			}
			gp.waiting = nil
			gp.activeStackChans = false
			gp.selectDone.Store(0)
			gp.param = nil
			// Keep original frames valid for GC until ordinary exit teardown.
			if gp.sched.lr != 0 {
				gp.sched.pc = fv.fn
				gp.sched.ctxt = unsafe.Pointer(fv)
			} else {
				gostartcallfn(&gp.sched, fv)
			}
			casfrom_Gscanstatus(gp, status|_Gscan, status)
		})
		// Pool locks must be acquired after scan ownership is released.
		for sg := waiting; sg != nil; {
			next := sg.waitlink
			sg.c.set(nil)
			sg.isSelect = false
			sg.waitlink = nil
			releaseSudog(sg)
			sg = next
		}
		if status == _Gwaiting {
			goready(gp, 0)
		}
	}

	deterministicStep(b, b.now, nil)
	return b.total == 0
}

// deterministicLock owns a paused stack against GC and tracing transitions.
// Unlike suspendG, this is an explicit exclusive claim, with no running preemption.
func deterministicLock(gp *g) uint32 {
	for {
		status := readgstatus(gp)
		if status&_Gscan != 0 {
			osyield()
			continue
		}
		if status != _Gwaiting && status != _Grunnable {
			return 0
		}
		if castogscanstatus(gp, status, status|_Gscan) {
			return status
		}
	}
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

// deterministicUnlink changes channel queues without holding a stack scan lock.
// Paused ownership excludes wakeups; GC can still adjust each element pointer.
func deterministicUnlink(waiting *sudog) {
	for sg := waiting; sg != nil; sg = sg.waitlink {
		c := sg.c.get()
		lock(&c.lock)
		// Interior nodes need only their links; dequeueSudoG uses q only at endpoints.
		if c.sendq.first == sg || c.sendq.last == sg {
			c.sendq.dequeueSudoG(sg)
		} else {
			c.recvq.dequeueSudoG(sg)
		}
		if c.timer != nil {
			unblockTimerChan(c)
		}
		unlock(&c.lock)
	}
}

func deterministicExit() {
	// Balance pending panic counts because this goroutine will not resume
	// panic unwinding or enter fatalpanic.
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
	b := handle.(*goroutineBubble)
	if getg().bubble != b || !b.deterministic.delivering || getg() != b.root {
		panic(errorString("runtime/bubble: timer delivery outside activation"))
	}
	return b.deterministicFireTimer(id, generation)
}

//go:linkname deterministicRetireRange internal/runtime/bubble.RetireTimerRange
func deterministicRetireRange(handle any, first, last uint64) {
	b := handle.(*goroutineBubble)
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
	b := handle.(*goroutineBubble)
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
