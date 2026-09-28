/*
Copyright 2023 The Vitess Authors.
Copyright 2023 Yiling-J

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

package theine

import (
	"sync/atomic"
)

type TinyLfu[K cachekey, V any] struct {
	slru      *Slru[K, V]
	sketch    *CountMinSketch
	size      uint
	counter   uint
	total     atomic.Uint32
	hit       atomic.Uint32
	hr        float32
	threshold atomic.Int32
	lruFactor uint8
	step      int8
}

func NewTinyLfu[K cachekey, V any](size uint) *TinyLfu[K, V] {
	tlfu := &TinyLfu[K, V]{
		size:   size,
		slru:   NewSlru[K, V](size),
		sketch: NewCountMinSketch(),
		step:   1,
	}
	// default threshold to -1 so all entries are admitted until cache is full
	tlfu.threshold.Store(-1)
	return tlfu
}

func (t *TinyLfu[K, V]) climb() {
	total := t.total.Load()
	hit := t.hit.Load()
	current := float32(hit) / float32(total)
	delta := current - t.hr
	var diff int8
	if delta > 0.0 {
		if t.step < 0 {
			t.step -= 1
		} else {
			t.step += 1
		}
		if t.step < -13 {
			t.step = -13
		} else if t.step > 13 {
			t.step = 13
		}
		newFactor := int8(t.lruFactor) + t.step
		if newFactor < 0 {
			newFactor = 0
		} else if newFactor > 16 {
			newFactor = 16
		}
		diff = newFactor - int8(t.lruFactor)
		t.lruFactor = uint8(newFactor)
	} else if delta < 0.0 {
		// reset
		if t.step > 0 {
			t.step = -1
		} else {
			t.step = 1
		}
		newFactor := int8(t.lruFactor) + t.step
		if newFactor < 0 {
			newFactor = 0
		} else if newFactor > 16 {
			newFactor = 16
		}
		diff = newFactor - int8(t.lruFactor)
		t.lruFactor = uint8(newFactor)
	}
	t.threshold.Add(-int32(diff))
	t.hr = current
	t.hit.Store(0)
	t.total.Store(0)
}

func (t *TinyLfu[K, V]) Set(entry *Entry[K, V]) *Entry[K, V] {
	t.counter++
	if t.counter > 10*t.size {
		t.climb()
		t.counter = 0
	}
	if entry.meta.prev == nil {
		if victim := t.slru.victim(); victim != nil {
			freq := int(entry.frequency.Load())
			if freq == -1 {
				freq = int(t.sketch.Estimate(entry.key.Hash()))
			}
			evictedCount := uint(freq) + uint(t.lruFactor)
			victimCount := t.sketch.Estimate(victim.key.Hash())
			if evictedCount <= uint(victimCount) {
				return entry
			}
		} else {
			count := t.slru.probation.count + t.slru.protected.count
			t.sketch.EnsureCapacity(uint(count + count/100))
		}
		evicted := t.slru.insert(entry)
		return evicted
	}

	return nil
}

func (t *TinyLfu[K, V]) Access(item ReadBufItem[K, V]) {
	t.counter++
	if t.counter > 10*t.size {
		t.climb()
		t.counter = 0
	}
	if entry := item.entry; entry != nil {
		reset := t.sketch.Add(item.hash)
		if reset {
			t.threshold.Store(t.threshold.Load() / 2)
		}
		if entry.meta.prev != nil {
			var tail bool
			if entry == t.slru.victim() {
				tail = true
			}
			t.slru.access(entry)
			if tail {
				t.UpdateThreshold()
			}
		} else {
			entry.frequency.Store(int32(t.sketch.Estimate(item.hash)))
		}
	} else {
		reset := t.sketch.Add(item.hash)
		if reset {
			t.threshold.Store(t.threshold.Load() / 2)
		}
	}
}

func (t *TinyLfu[K, V]) Remove(entry *Entry[K, V]) {
	t.slru.remove(entry)
}

func (t *TinyLfu[K, V]) UpdateCost(entry *Entry[K, V], delta int64) {
	t.slru.updateCost(entry, delta)
}

func (t *TinyLfu[K, V]) EvictEntries() []*Entry[K, V] {
	removed := []*Entry[K, V]{}

	for t.slru.probation.Len()+t.slru.protected.Len() > int(t.slru.maxsize) {
		entry := t.slru.probation.PopTail()
		if entry == nil {
			break
		}
		removed = append(removed, entry)
	}
	for t.slru.probation.Len()+t.slru.protected.Len() > int(t.slru.maxsize) {
		entry := t.slru.protected.PopTail()
		if entry == nil {
			break
		}
		removed = append(removed, entry)
	}
	return removed
}

func (t *TinyLfu[K, V]) UpdateThreshold() {
	if t.slru.probation.Len()+t.slru.protected.Len() < int(t.slru.maxsize) {
		t.threshold.Store(-1)
	} else {
		tail := t.slru.victim()
		if tail != nil {
			t.threshold.Store(
				int32(t.sketch.Estimate(tail.key.Hash()) - uint(t.lruFactor)),
			)
		} else {
			// cache is not full
			t.threshold.Store(-1)
		}
	}
}
