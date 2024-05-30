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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gammazero/deque"

	"vitess.io/vitess/go/cache/theine/bf"
	"vitess.io/vitess/go/hack"
)

const (
	MaxReadBuffSize  = 64
	MinWriteBuffSize = 4
	MaxWriteBuffSize = 1024
)

type RemoveReason uint8

const (
	REMOVED RemoveReason = iota
	EVICTED
	EXPIRED
)

type Shard[K cachekey, V any] struct {
	hashmap    map[K]*Entry[K, V]
	doorkeeper *bf.Bloomfilter
	deque      *deque.Deque[*Entry[K, V]]
	group      *Group[K, V]
	qsize      uint
	qlen       int
	counter    uint
	mu         sync.RWMutex
}

func NewShard[K cachekey, V any](qsize uint, doorkeeper bool) *Shard[K, V] {
	s := &Shard[K, V]{
		hashmap: make(map[K]*Entry[K, V]),
		qsize:   qsize,
		deque:   deque.New[*Entry[K, V]](),
		group:   NewGroup[K, V](),
	}
	if doorkeeper {
		s.doorkeeper = bf.New(0.01)
	}
	return s
}

func (s *Shard[K, V]) set(key K, entry *Entry[K, V]) {
	s.hashmap[key] = entry
	if s.doorkeeper != nil {
		ds := 20 * len(s.hashmap)
		if ds > s.doorkeeper.Capacity {
			s.doorkeeper.EnsureCapacity(ds)
		}
	}
}

func (s *Shard[K, V]) get(key K) (entry *Entry[K, V], ok bool) {
	entry, ok = s.hashmap[key]
	return
}

func (s *Shard[K, V]) delete(entry *Entry[K, V]) bool {
	var deleted bool
	exist, ok := s.hashmap[entry.key]
	if ok && exist == entry {
		delete(s.hashmap, exist.key)
		deleted = true
	}
	return deleted
}

func (s *Shard[K, V]) len() int {
	return len(s.hashmap)
}

type Metrics struct {
	evicted atomic.Int64
	hits    atomic.Int64
	misses  atomic.Int64
}

func (m *Metrics) Evicted() int64 {
	return m.evicted.Load()
}

func (m *Metrics) Hits() int64 {
	return m.hits.Load()
}

func (m *Metrics) Misses() int64 {
	return m.misses.Load()
}

func (m *Metrics) Accesses() int64 {
	return m.Hits() + m.Misses()
}

type cachekey interface {
	comparable
	Hash() uint64
	Hash2() (uint64, uint64)
}

type HashKey256 [32]byte

func (h HashKey256) Hash() uint64 {
	return uint64(h[0]) | uint64(h[1])<<8 | uint64(h[2])<<16 | uint64(h[3])<<24 |
		uint64(h[4])<<32 | uint64(h[5])<<40 | uint64(h[6])<<48 | uint64(h[7])<<56
}

func (h HashKey256) Hash2() (uint64, uint64) {
	h0 := h.Hash()
	h1 := uint64(h[8]) | uint64(h[9])<<8 | uint64(h[10])<<16 | uint64(h[11])<<24 |
		uint64(h[12])<<32 | uint64(h[13])<<40 | uint64(h[14])<<48 | uint64(h[15])<<56
	return h0, h1
}

type StringKey string

func (h StringKey) Hash() uint64 {
	return hack.RuntimeStrhash(string(h), 13850135847636357301)
}

func (h StringKey) Hash2() (uint64, uint64) {
	h0 := h.Hash()
	h1 := ((h0 >> 16) ^ h0) * 0x45d9f3b
	h1 = ((h1 >> 16) ^ h1) * 0x45d9f3b
	h1 = (h1 >> 16) ^ h1
	return h0, h1
}

type cacheval interface {
	CachedSize(alloc bool) int64
}

type Store[K cachekey, V cacheval] struct {
	Metrics   Metrics
	OnRemoval func(K, V, RemoveReason)

	entryPool    sync.Pool
	writebuf     chan WriteBufItem[K, V]
	policy       *TinyLfu[K, V]
	readbuf      *Queue[ReadBufItem[K, V]]
	shards       []*Shard[K, V]
	cap          uint
	shardCount   uint
	writebufsize int64
	tailUpdate   bool
	doorkeeper   bool

	mlock       sync.Mutex
	readCounter atomic.Uint32
	open        atomic.Bool
}

func NewStore[K cachekey, V cacheval](maxsize int64, doorkeeper bool) *Store[K, V] {
	writeBufSize := maxsize / 100
	if writeBufSize < MinWriteBuffSize {
		writeBufSize = MinWriteBuffSize
	}
	if writeBufSize > MaxWriteBuffSize {
		writeBufSize = MaxWriteBuffSize
	}
	shardCount := 1
	for shardCount < runtime.GOMAXPROCS(0)*2 {
		shardCount *= 2
	}
	if shardCount < 16 {
		shardCount = 16
	}
	if shardCount > 128 {
		shardCount = 128
	}
	dequeSize := int(maxsize) / 100 / shardCount
	policySize := int(maxsize) - (dequeSize * shardCount)

	s := &Store[K, V]{
		cap:          uint(maxsize),
		policy:       NewTinyLfu[K, V](uint(policySize)),
		readbuf:      NewQueue[ReadBufItem[K, V]](),
		writebuf:     make(chan WriteBufItem[K, V], writeBufSize),
		entryPool:    sync.Pool{New: func() any { return &Entry[K, V]{} }},
		shardCount:   uint(shardCount),
		doorkeeper:   doorkeeper,
		writebufsize: writeBufSize,
	}
	s.shards = make([]*Shard[K, V], 0, s.shardCount)
	for range s.shardCount {
		s.shards = append(s.shards, NewShard[K, V](uint(dequeSize), doorkeeper))
	}

	go s.maintenance()
	s.open.Store(true)
	return s
}

func (s *Store[K, V]) EnsureOpen() {
	if s.open.Swap(true) {
		return
	}
	s.writebuf = make(chan WriteBufItem[K, V], s.writebufsize)
	go s.maintenance()
}

func (s *Store[K, V]) getFromShard(key K, hash uint64, shard *Shard[K, V], epoch uint32) (V, bool) {
	new := s.readCounter.Add(1)
	shard.mu.RLock()
	entry, ok := shard.get(key)
	var value V
	if ok {
		if entry.epoch.Load() < epoch {
			s.Metrics.misses.Add(1)
			ok = false
		} else {
			s.Metrics.hits.Add(1)
			s.policy.hit.Add(1)
			value = entry.value
		}
	} else {
		s.Metrics.misses.Add(1)
	}
	shard.mu.RUnlock()
	switch {
	case new < MaxReadBuffSize:
		var send ReadBufItem[K, V]
		send.hash = hash
		if ok {
			send.entry = entry
		}
		s.readbuf.Push(send)
	case new == MaxReadBuffSize:
		var send ReadBufItem[K, V]
		send.hash = hash
		if ok {
			send.entry = entry
		}
		s.readbuf.Push(send)
		s.drainRead()
	}
	return value, ok
}

func (s *Store[K, V]) Get(key K, epoch uint32) (V, bool) {
	h, index := s.index(key)
	shard := s.shards[index]
	return s.getFromShard(key, h, shard, epoch)
}

func (s *Store[K, V]) GetOrLoad(key K, epoch uint32, load func() (V, error)) (V, bool, error) {
	h, index := s.index(key)
	shard := s.shards[index]
	v, ok := s.getFromShard(key, h, shard, epoch)
	if !ok {
		loaded, err, _ := shard.group.Do(key, func() (V, error) {
			loaded, err := load()
			if err == nil {
				s.Set(key, loaded, 0, epoch)
			}
			return loaded, err
		})
		return loaded, false, err
	}
	return v, true, nil
}

func (s *Store[K, V]) setEntry(shard *Shard[K, V], cost int64, epoch uint32, entry *Entry[K, V]) {
	shard.set(entry.key, entry)
	// cost larger than deque size, send to policy directly
	if cost > int64(shard.qsize) {
		shard.mu.Unlock()
		s.writebuf <- WriteBufItem[K, V]{entry: entry, code: NEW}
		return
	}
	entry.deque = true
	shard.deque.PushFront(entry)
	shard.qlen += int(cost)
	s.processDeque(shard, epoch)
}

func (s *Store[K, V]) setInternal(key K, value V, cost int64, epoch uint32) (*Shard[K, V], *Entry[K, V], bool) {
	h, index := s.index(key)
	shard := s.shards[index]
	shard.mu.Lock()
	exist, ok := shard.get(key)
	if ok {
		var costChange int64
		exist.value = value
		oldCost := exist.cost.Swap(cost)
		if oldCost != cost {
			costChange = cost - oldCost
			if exist.deque {
				shard.qlen += int(costChange)
			}
		}
		shard.mu.Unlock()
		exist.epoch.Store(epoch)
		if costChange != 0 {
			s.writebuf <- WriteBufItem[K, V]{
				entry: exist, code: UPDATE, costChange: costChange,
			}
		}
		return shard, exist, true
	}
	if s.doorkeeper {
		if shard.counter > uint(shard.doorkeeper.Capacity) {
			shard.doorkeeper.Reset()
			shard.counter = 0
		}
		hit := shard.doorkeeper.Insert(h)
		if !hit {
			shard.counter += 1
			shard.mu.Unlock()
			return shard, nil, false
		}
	}
	entry := s.entryPool.Get().(*Entry[K, V])
	entry.frequency.Store(-1)
	entry.key = key
	entry.value = value
	entry.cost.Store(cost)
	entry.epoch.Store(epoch)
	s.setEntry(shard, cost, epoch, entry)
	return shard, entry, true

}

func (s *Store[K, V]) Set(key K, value V, cost int64, epoch uint32) bool {
	if cost == 0 {
		cost = value.CachedSize(true)
	}
	if cost > int64(s.cap) {
		return false
	}
	_, _, ok := s.setInternal(key, value, cost, epoch)
	return ok
}

type dequeKV[K cachekey, V cacheval] struct {
	k K
	v V
}

func (s *Store[K, V]) processDeque(shard *Shard[K, V], epoch uint32) {
	if shard.qlen <= int(shard.qsize) {
		shard.mu.Unlock()
		return
	}
	var evictedkv []dequeKV[K, V]

	// send to slru
	send := make([]*Entry[K, V], 0, 2)
	for shard.qlen > int(shard.qsize) {
		evicted := shard.deque.PopBack()
		evicted.deque = false
		shard.qlen -= int(evicted.cost.Load())

		if evicted.epoch.Load() < epoch {
			deleted := shard.delete(evicted)
			if deleted {
				if s.OnRemoval != nil {
					evictedkv = append(evictedkv, dequeKV[K, V]{evicted.key, evicted.value})
				}
				s.postDelete(evicted)
				s.Metrics.evicted.Add(1)
			}
		} else {
			count := evicted.frequency.Load()
			threshold := s.policy.threshold.Load()
			if count == -1 {
				send = append(send, evicted)
			} else {
				if int32(count) >= threshold {
					send = append(send, evicted)
				} else {
					deleted := shard.delete(evicted)
					// double check because entry maybe removed already by Delete API
					if deleted {
						if s.OnRemoval != nil {
							evictedkv = append(evictedkv, dequeKV[K, V]{evicted.key, evicted.value})
						}
						s.postDelete(evicted)
						s.Metrics.evicted.Add(1)
					}
				}
			}
		}
	}

	shard.mu.Unlock()
	for _, entry := range send {
		s.writebuf <- WriteBufItem[K, V]{entry: entry, code: NEW}
	}
	if s.OnRemoval != nil {
		for _, kv := range evictedkv {
			s.OnRemoval(kv.k, kv.v, EVICTED)
		}
	}
}

func (s *Store[K, V]) Delete(key K) {
	_, index := s.index(key)
	shard := s.shards[index]
	shard.mu.Lock()
	entry, ok := shard.get(key)
	if ok {
		shard.delete(entry)
	}
	shard.mu.Unlock()
	if ok {
		s.writebuf <- WriteBufItem[K, V]{entry: entry, code: REMOVE}
	}
}

func (s *Store[K, V]) Len() int {
	total := 0
	for _, s := range s.shards {
		s.mu.RLock()
		total += s.len()
		s.mu.RUnlock()
	}
	return total
}

func (s *Store[K, V]) UsedCapacity() int {
	total := 0
	for _, s := range s.shards {
		s.mu.RLock()
		total += s.qlen
		s.mu.RUnlock()
	}
	return total
}

func (s *Store[K, V]) MaxCapacity() int {
	return int(s.cap)
}

// spread hash before get index
func (s *Store[K, V]) index(key K) (uint64, int) {
	h0, h1 := key.Hash2()
	return h0, int(h1 & uint64(s.shardCount-1))
}

func (s *Store[K, V]) postDelete(entry *Entry[K, V]) {
	var zero V
	entry.value = zero
	s.entryPool.Put(entry)
}

// remove entry from cache/policy/timingwheel and add back to pool
func (s *Store[K, V]) removeEntry(entry *Entry[K, V], reason RemoveReason) {
	if prev := entry.meta.prev; prev != nil {
		s.policy.Remove(entry)
	}
	switch reason {
	case EVICTED, EXPIRED:
		_, index := s.index(entry.key)
		shard := s.shards[index]
		shard.mu.Lock()
		deleted := shard.delete(entry)
		shard.mu.Unlock()
		if deleted {
			if s.OnRemoval != nil {
				s.OnRemoval(entry.key, entry.value, reason)
			}
			s.postDelete(entry)
			s.Metrics.evicted.Add(1)
		}
	case REMOVED:
		// already removed from shard map
		if s.OnRemoval != nil {
			s.OnRemoval(entry.key, entry.value, reason)
		}
	}
}

func (s *Store[K, V]) drainRead() {
	s.policy.total.Add(MaxReadBuffSize)
	s.mlock.Lock()
	for {
		v, ok := s.readbuf.Pop()
		if !ok {
			break
		}
		s.policy.Access(v)
	}
	s.mlock.Unlock()
	s.readCounter.Store(0)
}

func (s *Store[K, V]) maintenanceItem(item WriteBufItem[K, V]) {
	s.mlock.Lock()
	defer s.mlock.Unlock()

	entry := item.entry
	if entry == nil {
		return
	}

	// lock free because store API never read/modify entry metadata
	switch item.code {
	case NEW:
		if entry.removed {
			return
		}
		evicted := s.policy.Set(entry)
		if evicted != nil {
			s.removeEntry(evicted, EVICTED)
			s.tailUpdate = true
		}
		removed := s.policy.EvictEntries()
		for _, e := range removed {
			s.tailUpdate = true
			s.removeEntry(e, EVICTED)
		}
	case REMOVE:
		entry.removed = true
		s.removeEntry(entry, REMOVED)
		s.policy.threshold.Store(-1)
	case UPDATE:
		if item.costChange != 0 {
			s.policy.UpdateCost(entry, item.costChange)
			removed := s.policy.EvictEntries()
			for _, e := range removed {
				s.tailUpdate = true
				s.removeEntry(e, EVICTED)
			}
		}
	}
	item.entry = nil
	if s.tailUpdate {
		s.policy.UpdateThreshold()
		s.tailUpdate = false
	}
}

func (s *Store[K, V]) maintenance() {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			s.mlock.Lock()
			s.policy.UpdateThreshold()
			s.mlock.Unlock()

		case item, ok := <-s.writebuf:
			if !ok {
				return
			}
			s.maintenanceItem(item)
		}
	}
}

func (s *Store[K, V]) Range(epoch uint32, f func(key K, value V) bool) {
	for _, shard := range s.shards {
		shard.mu.RLock()
		for _, entry := range shard.hashmap {
			if entry.epoch.Load() < epoch {
				continue
			}
			if !f(entry.key, entry.value) {
				shard.mu.RUnlock()
				return
			}
		}
		shard.mu.RUnlock()
	}
}

func (s *Store[K, V]) Close() {
	if !s.open.Swap(false) {
		panic("theine.Store: double close")
	}

	for _, s := range s.shards {
		s.mu.Lock()
		clear(s.hashmap)
		s.mu.Unlock()
	}
	close(s.writebuf)
}
