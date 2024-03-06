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
	"testing"

	"github.com/stretchr/testify/require"
)

type cachedint int

func (ci cachedint) CachedSize(bool) int64 {
	return 1
}

type keyint int

func (k keyint) Hash() uint64 {
	return uint64(k)
}

func (k keyint) Hash2() (uint64, uint64) {
	return uint64(k), uint64(k) * 333
}

func TestProcessDeque(t *testing.T) {
	store := NewStore[keyint, cachedint](20000, false)

	evicted := map[keyint]cachedint{}
	store.OnRemoval = func(key keyint, value cachedint, reason RemoveReason) {
		if reason == EVICTED {
			evicted[key] = value
		}
	}
	_, index := store.index(123)
	shard := store.shards[index]
	shard.qsize = 10

	for i := range keyint(5) {
		entry := &Entry[keyint, cachedint]{key: i}
		entry.cost.Store(1)
		store.shards[index].deque.PushFront(entry)
		store.shards[index].qlen += 1
		store.shards[index].hashmap[i] = entry
	}

	// move 0,1,2 entries to slru
	store.Set(123, 123, 8, 0)
	require.Equal(t, store.shards[index].deque.Len(), 3)
	var keys []keyint
	for store.shards[index].deque.Len() != 0 {
		e := store.shards[index].deque.PopBack()
		keys = append(keys, e.key)
	}
	require.Equal(t, []keyint{3, 4, 123}, keys)
}

func TestDoorKeeperDynamicSize(t *testing.T) {
	store := NewStore[keyint, cachedint](200000, true)
	shard := store.shards[0]
	require.True(t, shard.doorkeeper.Capacity == 512)
	for i := range keyint(5000) {
		shard.set(i, &Entry[keyint, cachedint]{})
	}
	require.True(t, shard.doorkeeper.Capacity > 100000)
}
