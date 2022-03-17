/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 * Copyright 2021 The Vitess Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ristretto

import (
	"sync"
)

// TODO: Do we need this to be a separate struct from Item?
type storeItem[I any] struct {
	key      uint64
	conflict uint64
	value    I
}

// store is the interface fulfilled by all hash map implementations in this
// file. Some hash map implementations are better suited for certain data
// distributions than others, so this allows us to abstract that out for use
// in Ristretto.
//
// Every store is safe for concurrent usage.
type store[I any] interface {
	// Get returns the value associated with the key parameter.
	Get(uint64, uint64) (I, bool)
	// Set adds the key-value pair to the Map or updates the value if it's
	// already present. The key-value pair is passed as a pointer to an
	// item object.
	Set(*Item[I])
	// Del deletes the key-value pair from the Map.
	Del(uint64, uint64) (uint64, I)
	// Update attempts to update the key with a new value and returns true if
	// successful.
	Update(*Item[I]) (I, bool)
	// Clear clears all contents of the store.
	Clear(onEvict func(*Item[I]))
	// ForEach yields all the values in the store
	ForEach(forEach func(I) bool)
	// Len returns the number of entries in the store
	Len() int
}

// newStore returns the default store implementation.
func newStore[I any]() store[I] {
	return newShardedMap[I]()
}

const numShards uint64 = 256

type shardedMap[I any] struct {
	shards []*lockedMap[I]
}

func newShardedMap[I any]() *shardedMap[I] {
	sm := &shardedMap[I]{
		shards: make([]*lockedMap[I], int(numShards)),
	}
	for i := range sm.shards {
		sm.shards[i] = newLockedMap[I]()
	}
	return sm
}

func (sm *shardedMap[I]) Get(key, conflict uint64) (I, bool) {
	return sm.shards[key%numShards].get(key, conflict)
}

func (sm *shardedMap[I]) Set(i *Item[I]) {
	if i == nil {
		// If item is nil make this Set a no-op.
		return
	}

	sm.shards[i.Key%numShards].Set(i)
}

func (sm *shardedMap[I]) Del(key, conflict uint64) (uint64, I) {
	return sm.shards[key%numShards].Del(key, conflict)
}

func (sm *shardedMap[I]) Update(newItem *Item[I]) (I, bool) {
	return sm.shards[newItem.Key%numShards].Update(newItem)
}

func (sm *shardedMap[I]) ForEach(forEach func(I) bool) {
	for _, shard := range sm.shards {
		if !shard.foreach(forEach) {
			break
		}
	}
}

func (sm *shardedMap[I]) Len() int {
	l := 0
	for _, shard := range sm.shards {
		l += shard.Len()
	}
	return l
}

func (sm *shardedMap[I]) Clear(onEvict func(*Item[I])) {
	for i := uint64(0); i < numShards; i++ {
		sm.shards[i].Clear(onEvict)
	}
}

type lockedMap[I any] struct {
	sync.RWMutex
	data map[uint64](storeItem[I])
}

func newLockedMap[I any]() *lockedMap[I] {
	return &lockedMap[I]{
		data: make(map[uint64]storeItem[I]),
	}
}

func (m *lockedMap[I]) get(key, conflict uint64) (I, bool) {
	m.RLock()
	item, ok := m.data[key]
	m.RUnlock()
	if !ok {
		var empty I
		return empty, false
	}
	if conflict != 0 && (conflict != item.conflict) {
		var empty I
		return empty, false
	}
	return item.value, true
}

func (m *lockedMap[I]) Set(i *Item[I]) {
	if i == nil {
		// If the item is nil make this Set a no-op.
		return
	}

	m.Lock()
	defer m.Unlock()
	item, ok := m.data[i.Key]

	if ok {
		// The item existed already. We need to check the conflict key and reject the
		// update if they do not match. Only after that the expiration map is updated.
		if i.Conflict != 0 && (i.Conflict != item.conflict) {
			return
		}
	}

	m.data[i.Key] = storeItem[I]{
		key:      i.Key,
		conflict: i.Conflict,
		value:    i.Value,
	}
}

func (m *lockedMap[I]) Del(key, conflict uint64) (uint64, I) {
	m.Lock()
	item, ok := m.data[key]
	if !ok {
		m.Unlock()
		var empty I
		return 0, empty
	}
	if conflict != 0 && (conflict != item.conflict) {
		m.Unlock()
		var empty I
		return 0, empty
	}

	delete(m.data, key)
	m.Unlock()
	return item.conflict, item.value
}

func (m *lockedMap[I]) Update(newItem *Item[I]) (I, bool) {
	m.Lock()
	item, ok := m.data[newItem.Key]
	if !ok {
		m.Unlock()
		var empty I
		return empty, false
	}
	if newItem.Conflict != 0 && (newItem.Conflict != item.conflict) {
		m.Unlock()
		var empty I
		return empty, false
	}

	m.data[newItem.Key] = storeItem[I]{
		key:      newItem.Key,
		conflict: newItem.Conflict,
		value:    newItem.Value,
	}

	m.Unlock()
	return item.value, true
}

func (m *lockedMap[I]) Len() int {
	m.RLock()
	l := len(m.data)
	m.RUnlock()
	return l
}

func (m *lockedMap[I]) Clear(onEvict func(*Item[I])) {
	m.Lock()
	i := &Item[I]{}
	if onEvict != nil {
		for _, si := range m.data {
			i.Key = si.key
			i.Conflict = si.conflict
			i.Value = si.value
			onEvict(i)
		}
	}
	m.data = make(map[uint64]storeItem[I])
	m.Unlock()
}

func (m *lockedMap[I]) foreach(forEach func(I) bool) bool {
	m.RLock()
	defer m.RUnlock()
	for _, si := range m.data {
		if !forEach(si.value) {
			return false
		}
	}
	return true
}
