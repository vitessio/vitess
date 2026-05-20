/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package smartconnpool

import (
	"math/rand/v2"
	"runtime"
	"sync/atomic"

	"vitess.io/vitess/go/atomic2"
)

// elimSlots is the size of an elimination array. Must be a power of two.
// Asymmetric Rendezvous (Scherer/Scott, also covered in Ilya Sergey's
// YSC4231 lecture, week 8): pops publish a popMarker in a vacant slot and
// spin; pushes scan a span of slots hunting for a waiting pop and deliver
// the item directly. Push never publishes its own item — if it finds no
// waiting pop, the caller yields and retries the main stack.
const (
	elimSlots = 16
	elimMask  = elimSlots - 1
	// elimSpin bounds how long a publishing popper waits for a pusher to
	// arrive before retracting. Each iteration is one atomic Load; on arm64
	// roughly 1-2ns, so ~64 spins ≈ 100ns — long enough to catch a paired
	// push on another CPU, short enough that lonely poppers don't stall.
	elimSpin = 64
	// pushHuntSpan is how many slots a push scans looking for a waiting
	// popper before giving up. A quarter of the array balances the cost of
	// the scan against the probability of finding a peer.
	pushHuntSpan = elimSlots / 4
)

// elimSlot is one rendezvous point in an elimination array. Cache-line
// padded to avoid false sharing between adjacent slots under contention.
//
// pending holds one of:
//   - nil:                       EMPTY — slot is free
//   - elimArray.popMarker:       a popper is here, awaiting an item
//   - any other *Pooled[C]:      a pusher just delivered, awaiting pickup
type elimSlot[C Connection] struct {
	pending atomic.Pointer[Pooled[C]]
	_       [64 - 8]byte
}

// elimArray is the elimination array attached to a connStack. The popMarker
// is a per-array sentinel pointer used to distinguish "a pop is waiting"
// from "a push left an item." It must be initialized before use (see init);
// callers in pool.go go through lazyElim which handles this.
type elimArray[C Connection] struct {
	slots     [elimSlots]elimSlot[C]
	popMarker *Pooled[C]
}

func (e *elimArray[C]) init() {
	e.popMarker = &Pooled[C]{}
}

// newElimArray returns a ready-to-use elimArray. Tests use this; the pool
// uses inline elimArrays inside stackElim and initializes them via init.
func newElimArray[C Connection]() *elimArray[C] {
	e := &elimArray[C]{}
	e.init()
	return e
}

// connStack is a lock-free stack for Connection objects. It is safe to
// use from several goroutines. When an `elim` array is provided, Push/Pop
// can hand off directly to a concurrent peer on CAS failure without
// touching the main stack.
//
// Why a separate elim array (instead of an embedded field): the top field
// requires 16-byte alignment for arm64 LDAXP/CASPD. Growing connStack
// beyond 16 bytes risks pushing the enclosing ConnPool past 512 bytes,
// which makes the Go runtime prepend an 8-byte GC type header and breaks
// 16-byte alignment of the top field.
type connStack[C Connection] struct {
	// top is a pointer to the top node on the stack and to an increasing
	// counter of pop operations, to prevent A-B-A races.
	// See: https://en.wikipedia.org/wiki/ABA_problem
	top atomic2.PointerAndUint64[Pooled[C]]
}

func (s *connStack[C]) Push(item *Pooled[C], elim *elimArray[C]) {
	for {
		oldHead, popCount := s.top.Load()
		item.next.Store(oldHead)
		if s.top.CompareAndSwap(oldHead, popCount, item, popCount) {
			return
		}
		// CAS failed → contended. Try eliminating against a popper; if no
		// elim array or no peer shows up, yield to the scheduler so this
		// goroutine doesn't burn a CPU re-CASing the same losing head.
		if elim != nil && tryElimPush(elim, item) {
			return
		}
		runtime.Gosched()
	}
}

func (s *connStack[C]) Pop(elim *elimArray[C]) (*Pooled[C], bool) {
	for {
		oldHead, popCount := s.top.Load()
		if oldHead == nil {
			return nil, false
		}

		newHead := oldHead.next.Load()
		if s.top.CompareAndSwap(oldHead, popCount, newHead, popCount+1) {
			oldHead.next.Store(nil)
			return oldHead, true
		}
		if elim != nil {
			if item, ok := tryElimPop(elim); ok {
				return item, true
			}
		}
		runtime.Gosched()
	}
}

func (s *connStack[C]) Peek() *Pooled[C] {
	top, _ := s.top.Load()
	return top
}

// tryElimPush first hunts for a waiting popper across a span of slots and
// delivers if one is found; failing that, it publishes the item into a
// vacant slot and waits briefly so a contended popper can opportunistically
// pick it up. Returns true if the push was satisfied either way.
func tryElimPush[C Connection](elim *elimArray[C], item *Pooled[C]) bool {
	start := uint32(rand.Uint64()) & elimMask
	// Phase 1: hunt for a waiting popper.
	for i := range uint32(pushHuntSpan) {
		slot := &elim.slots[(start+i)&elimMask]
		if slot.pending.Load() != elim.popMarker {
			continue
		}
		if slot.pending.CompareAndSwap(elim.popMarker, item) {
			return true
		}
	}
	// Phase 2: park the item in a vacant slot for a later contended popper.
	var parked *elimSlot[C]
	for i := range uint32(pushHuntSpan) {
		slot := &elim.slots[(start+pushHuntSpan+i)&elimMask]
		if slot.pending.Load() != nil {
			continue
		}
		if slot.pending.CompareAndSwap(nil, item) {
			parked = slot
			break
		}
	}
	if parked == nil {
		return false
	}
	runtime.Gosched()
	for range elimSpin {
		if parked.pending.Load() == nil {
			return true
		}
	}
	// CAS true → we retracted cleanly, no popper showed up: retry the main
	// stack. CAS false → a popper just took the item: push is done.
	return !parked.pending.CompareAndSwap(item, nil)
}

// tryElimPop scans the array for an already-delivered item to steal, then
// publishes a popMarker in the first vacant slot and spins briefly waiting
// for a pusher to deliver.
//
// The opportunistic steal handles the case where a pusher delivered into
// another popper's slot but the intended popper hasn't yet picked up —
// this prevents items from getting stuck if their original popper is slow.
func tryElimPop[C Connection](elim *elimArray[C]) (*Pooled[C], bool) {
	start := uint32(rand.Uint64()) & elimMask
	var occupied *elimSlot[C]
	for i := range uint32(elimSlots) {
		slot := &elim.slots[(start+i)&elimMask]
		cur := slot.pending.Load()
		switch {
		case cur == nil:
			// Vacant — try to occupy with our popMarker.
			if slot.pending.CompareAndSwap(nil, elim.popMarker) {
				occupied = slot
			}
		case cur != elim.popMarker:
			// A pusher's delivered item is parked here — steal it.
			if slot.pending.CompareAndSwap(cur, nil) {
				cur.next.Store(nil)
				return cur, true
			}
		}
		if occupied != nil {
			break
		}
	}
	if occupied == nil {
		// All slots full of popMarkers. Caller will retry the main stack.
		return nil, false
	}

	runtime.Gosched()
	for range elimSpin {
		v := occupied.pending.Load()
		if v == elim.popMarker {
			continue
		}
		if v == nil {
			// Another popper opportunistically stole the item destined for
			// us. Our rendezvous is gone.
			return nil, false
		}
		// A pusher delivered an item — try to claim it.
		if occupied.pending.CompareAndSwap(v, nil) {
			v.next.Store(nil)
			return v, true
		}
		return nil, false
	}

	// Timeout — retract our marker.
	if occupied.pending.CompareAndSwap(elim.popMarker, nil) {
		return nil, false
	}
	// Retract lost — a pusher just delivered. Try to claim the item.
	v := occupied.pending.Load()
	if v != nil && v != elim.popMarker {
		if occupied.pending.CompareAndSwap(v, nil) {
			v.next.Store(nil)
			return v, true
		}
	}
	return nil, false
}
