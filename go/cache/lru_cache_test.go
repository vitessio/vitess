/*
Copyright 2019 The Vitess Authors.

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

package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type CacheValue struct {
	size int64
}

func TestInitialState(t *testing.T) {
	cache := NewLRUCache[*CacheValue](5)
	l, sz, c, e, h, m := cache.Len(), cache.UsedCapacity(), cache.MaxCapacity(), cache.Evictions(), cache.Hits(), cache.Misses()
	assert.Zero(t, l)
	assert.EqualValues(t, 0, sz)
	assert.EqualValues(t, 5, c)
	assert.EqualValues(t, 0, e)
	assert.EqualValues(t, 0, h)
	assert.EqualValues(t, 0, m)
}

func TestSetInsertsValue(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)
	data := &CacheValue{0}
	key := "key"
	cache.Set(key, data)

	v, ok := cache.Get(key)
	assert.True(t, ok && v == data, "Cache has incorrect value: expected %v, got %v", data, v)

	values := cache.Items()
	assert.True(t, len(values) == 1 && values[0].Key == key, "Cache.Values() returned incorrect values: %v", values)
}

func TestGetValueWithMultipleTypes(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)
	data := &CacheValue{0}
	key := "key"
	cache.Set(key, data)

	v, ok := cache.Get("key")
	assert.True(t, ok && v == data, "Cache has incorrect value for \"key\": expected %v, got %v", data, v)

	v, ok = cache.Get(string([]byte{'k', 'e', 'y'}))
	assert.True(t, ok && v == data, "Cache has incorrect value for []byte {'k','e','y'}: expected %v, got %v", data, v)
}

func TestSetWithOldKeyUpdatesValue(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)
	emptyValue := &CacheValue{0}
	key := "key1"
	cache.Set(key, emptyValue)
	someValue := &CacheValue{20}
	cache.Set(key, someValue)

	v, ok := cache.Get(key)
	assert.True(t, ok && v == someValue, "Cache has incorrect value: expected %v, got %v", someValue, v)
}

func TestGetNonExistent(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)

	_, ok := cache.Get("notthere")
	assert.False(t, ok, "Cache returned a notthere value after no inserts.")
}

func TestDelete(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)
	value := &CacheValue{1}
	key := "key"

	cache.Delete(key)
	cache.Set(key, value)
	cache.Delete(key)

	sz := cache.UsedCapacity()
	assert.Zero(t, sz, "cache.UsedCapacity() = %v, expected 0", sz)

	_, ok := cache.Get(key)
	assert.False(t, ok, "Cache returned a value after deletion.")
}

func TestCapacityIsObeyed(t *testing.T) {
	size := int64(3)
	cache := NewLRUCache[*CacheValue](100)
	cache.SetCapacity(size)
	value := &CacheValue{1}

	// Insert up to the cache's capacity.
	cache.Set("key1", value)
	cache.Set("key2", value)
	cache.Set("key3", value)
	sz := cache.UsedCapacity()
	assert.Equal(t, size, sz, "cache.UsedCapacity() = %v, expected %v", sz, size)
	// Insert one more; something should be evicted to make room.
	cache.Set("key4", value)
	sz, evictions := cache.UsedCapacity(), cache.Evictions()
	assert.Equal(t, size, sz)
	assert.EqualValues(t, 1, evictions)

	// Check various other stats
	l := cache.Len()
	assert.Equal(t, size, int64(l), "cache.Len() returned bad length: %v", l)

	s := cache.UsedCapacity()
	assert.Equal(t, size, s, "cache.UsedCapacity() returned bad size: %v", s)

	c := cache.MaxCapacity()
	assert.Equal(t, size, c, "cache.UsedCapacity() returned bad length: %v", c)

	hits := cache.Hits()
	assert.Zero(t, hits, "cache.Hits() returned hits when there should be none: %v", hits)

	misses := cache.Misses()
	assert.Zero(t, misses, "cache.Misses() returned misses when there should be none: %v", misses)
}

func TestLRUIsEvicted(t *testing.T) {
	size := int64(3)
	cache := NewLRUCache[*CacheValue](size)

	cache.Set("key1", &CacheValue{1})
	cache.Set("key2", &CacheValue{1})
	cache.Set("key3", &CacheValue{1})
	// lru: [key3, key2, key1]

	// Look up the elements. This will rearrange the LRU ordering.
	cache.Get("key3")
	cache.Get("key2")
	cache.Get("key1")
	// lru: [key1, key2, key3]

	cache.Set("key0", &CacheValue{1})
	// lru: [key0, key1, key2]

	// The least recently used one should have been evicted.
	_, ok := cache.Get("key3")
	assert.False(t, ok, "Least recently used element was not evicted.")

	e := cache.Evictions()
	assert.Equal(t, int64(1), e, "evictions: got %d, want: %d", e, 1)

	h := cache.Hits()
	assert.Equal(t, int64(3), h, "hits: got %d, want: %d", h, 3)

	m := cache.Misses()
	assert.Equal(t, int64(1), m, "misses: got %d, want: %d", m, 1)
}
