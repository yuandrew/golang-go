// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package runtime_test

import (
	"runtime"
	"slices"
	"testing"
)

func TestBubbleUnlinkQueues(t *testing.T) {
	send := []int{0, 1, 2, 3, 4}
	recv := []int{5, 6, 7, 8, 9}
	for _, test := range []struct {
		name  string
		order []int
	}{
		{"head", []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
		{"tail", []int{4, 3, 2, 1, 0, 9, 8, 7, 6, 5}},
		{"interior", []int{2, 7, 0, 5, 4, 9, 1, 6, 3, 8}},
		{"already-removed", []int{2, 2, 7, 7, 0, 5, 4, 9, 1, 6, 3, 8, 3, 8}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for prefix := 0; prefix <= len(test.order); prefix++ {
				remove := test.order[:prefix]
				wantSend := slices.DeleteFunc(slices.Clone(send), func(id int) bool { return slices.Contains(remove, id) })
				wantRecv := slices.DeleteFunc(slices.Clone(recv), func(id int) bool { return slices.Contains(remove, id) })
				gotSend, gotRecv := runtime.BubbleUnlinkQueues(send, recv, remove)
				if !slices.Equal(gotSend, wantSend) || !slices.Equal(gotRecv, wantRecv) {
					t.Fatalf("remove %v: send=%v recv=%v; want send=%v recv=%v", remove, gotSend, gotRecv, wantSend, wantRecv)
				}
			}
		})
	}
}
