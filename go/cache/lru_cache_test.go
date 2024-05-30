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
	if !ok || v != data {
		t.Errorf("Cache has incorrect value: %v != %v", data, v)
	}

	values := cache.Items()
	if len(values) != 1 || values[0].Key != key {
		t.Errorf("Cache.Values() returned incorrect values: %v", values)
	}
}

func TestGetValueWithMultipleTypes(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)
	data := &CacheValue{0}
	key := "key"
	cache.Set(key, data)

	v, ok := cache.Get("key")
	if !ok || v != data {
		t.Errorf("Cache has incorrect value for \"key\": %v != %v", data, v)
	}

	v, ok = cache.Get(string([]byte{'k', 'e', 'y'}))
	if !ok || v != data {
		t.Errorf("Cache has incorrect value for []byte {'k','e','y'}: %v != %v", data, v)
	}
}

func TestSetWithOldKeyUpdatesValue(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)
	emptyValue := &CacheValue{0}
	key := "key1"
	cache.Set(key, emptyValue)
	someValue := &CacheValue{20}
	cache.Set(key, someValue)

	v, ok := cache.Get(key)
	if !ok || v != someValue {
		t.Errorf("Cache has incorrect value: %v != %v", someValue, v)
	}
}

func TestGetNonExistent(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)

	if _, ok := cache.Get("notthere"); ok {
		t.Error("Cache returned a notthere value after no inserts.")
	}
}

func TestDelete(t *testing.T) {
	cache := NewLRUCache[*CacheValue](100)
	value := &CacheValue{1}
	key := "key"

	cache.Delete(key)
	cache.Set(key, value)
	cache.Delete(key)

	if sz := cache.UsedCapacity(); sz != 0 {
		t.Errorf("cache.UsedCapacity() = %v, expected 0", sz)
	}

	if _, ok := cache.Get(key); ok {
		t.Error("Cache returned a value after deletion.")
	}
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
	if sz := cache.UsedCapacity(); sz != size {
		t.Errorf("cache.UsedCapacity() = %v, expected %v", sz, size)
	}
	// Insert one more; something should be evicted to make room.
	cache.Set("key4", value)
	sz, evictions := cache.UsedCapacity(), cache.Evictions()
	assert.Equal(t, size, sz)
	assert.EqualValues(t, 1, evictions)

	// Check various other stats
	if l := cache.Len(); int64(l) != size {
		t.Errorf("cache.Len() returned bad length: %v", l)
	}
	if s := cache.UsedCapacity(); s != size {
		t.Errorf("cache.UsedCapacity() returned bad size: %v", s)
	}
	if c := cache.MaxCapacity(); c != size {
		t.Errorf("cache.UsedCapacity() returned bad length: %v", c)
	}
	if c := cache.Hits(); c != 0 {
		t.Errorf("cache.Hits() returned hits when there should be none: %v", c)
	}
	if c := cache.Misses(); c != 0 {
		t.Errorf("cache.Misses() returned misses when there should be none: %v", c)
	}
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
	if _, ok := cache.Get("key3"); ok {
		t.Error("Least recently used element was not evicted.")
	}

	if e, want := cache.Evictions(), int64(1); e != want {
		t.Errorf("evictions: %d, want: %d", e, want)
	}

	if h, want := cache.Hits(), int64(3); h != want {
		t.Errorf("hits: %d, want: %d", h, want)
	}

	if m, want := cache.Misses(), int64(1); m != want {
		t.Errorf("misses: %d, want: %d", m, want)
	}
}
