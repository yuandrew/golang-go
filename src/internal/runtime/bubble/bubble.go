// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bubble connects the experimental runtime/bubble API to the runtime.
package bubble

import _ "unsafe"

type TimerChange struct {
	Kind       uint8
	ID         uint64
	Generation uint64
	Deadline   int64
}

type State struct {
	RootDone       bool
	LiveGoroutines int
	TimerChanges   []TimerChange
}

//go:linkname New
func New(now int64, seed uint64, f func(), random func() uint64) any

// Random supplies package-level math randomness. Outside a deterministic bubble
// it uses the ordinary runtime random source. Runtime housekeeping must continue
// to use runtime.rand, never this callback-capable function.
//
//go:linkname Random
func Random() uint64

// LegacyRand returns the bubble's cached math/rand state, constructing it with
// init on first use. It returns nil outside a deterministic bubble.
//
//go:linkname LegacyRand
func LegacyRand(init func() any) any

//go:linkname Step
func Step(handle any, now int64, deliver func()) State

//go:linkname Snapshot
func Snapshot(handle any) State

//go:linkname Close
func Close(handle any) bool

//go:linkname FireTimer
func FireTimer(handle any, id, generation uint64) bool

//go:linkname StackTrace
func StackTrace(handle any) string

//go:linkname IsBubbled
func IsBubbled() bool
