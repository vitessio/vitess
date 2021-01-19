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
)

type CacheValue struct{}

func TestInitialState(t *testing.T) {
	cache := NewLRUCache(5)
	if cache.Len() != 0 {
		t.Errorf("length = %v, want 0", cache.Len())
	}
	if cache.UsedCapacity() != 0 {
		t.Errorf("size = %v, want 0", cache.UsedCapacity())
	}
	if cache.MaxCapacity() != 5 {
		t.Errorf("capacity = %v, want 5", cache.MaxCapacity())
	}
	if cache.Evictions() != 0 {
		t.Errorf("evictions = %v, want 0", cache.Evictions())
	}
}

func TestSetInsertsValue(t *testing.T) {
	cache := NewLRUCache(100)
	data := &CacheValue{}
	key := "key"
	cache.Set(key, data, 0)

	v, ok := cache.Get(key)
	if !ok || v.(*CacheValue) != data {
		t.Errorf("Cache has incorrect value: %v != %v", data, v)
	}

	values := cache.Items()
	if len(values) != 1 || values[0].Key != key {
		t.Errorf("Cache.Values() returned incorrect values: %v", values)
	}
}

func TestGetValueWithMultipleTypes(t *testing.T) {
	cache := NewLRUCache(100)
	data := &CacheValue{}
	key := "key"
	cache.Set(key, data, 0)

	v, ok := cache.Get("key")
	if !ok || v.(*CacheValue) != data {
		t.Errorf("Cache has incorrect value for \"key\": %v != %v", data, v)
	}

	v, ok = cache.Get(string([]byte{'k', 'e', 'y'}))
	if !ok || v.(*CacheValue) != data {
		t.Errorf("Cache has incorrect value for []byte {'k','e','y'}: %v != %v", data, v)
	}
}

func TestSetUpdatesSize(t *testing.T) {
	cache := NewLRUCache(100)
	emptyValue := &CacheValue{}
	key := "key1"
	cache.Set(key, emptyValue, 0)
	if size := cache.UsedCapacity(); size != 0 {
		t.Errorf("cache.CachedSize() = %v, expected 0", size)
	}
	someValue := &CacheValue{}
	key = "key2"
	cache.Set(key, someValue, 20)
	if size := cache.UsedCapacity(); size != 20 {
		t.Errorf("cache.CachedSize() = %v, expected 20", size)
	}
}

func TestSetWithOldKeyUpdatesValue(t *testing.T) {
	cache := NewLRUCache(100)
	emptyValue := &CacheValue{}
	key := "key1"
	cache.Set(key, emptyValue, 0)
	someValue := &CacheValue{}
	cache.Set(key, someValue, 20)

	v, ok := cache.Get(key)
	if !ok || v.(*CacheValue) != someValue {
		t.Errorf("Cache has incorrect value: %v != %v", someValue, v)
	}
}

func TestSetWithOldKeyUpdatesSize(t *testing.T) {
	cache := NewLRUCache(100)
	emptyValue := &CacheValue{}
	key := "key1"
	cache.Set(key, emptyValue, 0)

	if size := cache.UsedCapacity(); size != 0 {
		t.Errorf("cache.CachedSize() = %v, expected %v", size, 0)
	}

	someValue := &CacheValue{}
	cache.Set(key, someValue, 20)
	expected := int64(20)
	if size := cache.UsedCapacity(); size != expected {
		t.Errorf("cache.CachedSize() = %v, expected %v", size, expected)
	}
}

func TestGetNonExistent(t *testing.T) {
	cache := NewLRUCache(100)

	if _, ok := cache.Get("notthere"); ok {
		t.Error("Cache returned a notthere value after no inserts.")
	}
}

func TestDelete(t *testing.T) {
	cache := NewLRUCache(100)
	value := &CacheValue{}
	key := "key"

	if cache.delete(key) {
		t.Error("Item unexpectedly already in cache.")
	}

	cache.Set(key, value, 1)

	if !cache.delete(key) {
		t.Error("Expected item to be in cache.")
	}

	if size := cache.UsedCapacity(); size != 0 {
		t.Errorf("cache.CachedSize() = %v, expected 0", size)
	}

	if _, ok := cache.Get(key); ok {
		t.Error("Cache returned a value after deletion.")
	}
}

func TestClear(t *testing.T) {
	cache := NewLRUCache(100)
	value := &CacheValue{}
	key := "key"

	cache.Set(key, value, 1)
	cache.Clear()

	if size := cache.UsedCapacity(); size != 0 {
		t.Errorf("cache.CachedSize() = %v, expected 0 after Clear()", size)
	}
}

func TestCapacityIsObeyed(t *testing.T) {
	size := int64(3)
	cache := NewLRUCache(100)
	cache.SetCapacity(size)
	value := &CacheValue{}

	// Insert up to the cache's capacity.
	cache.Set("key1", value, 1)
	cache.Set("key2", value, 1)
	cache.Set("key3", value, 1)
	if usedCap := cache.UsedCapacity(); usedCap != size {
		t.Errorf("cache.CachedSize() = %v, expected %v", usedCap, size)
	}
	// Insert one more; something should be evicted to make room.
	cache.Set("key4", value, 1)
	if cache.UsedCapacity() != size {
		t.Errorf("post-evict cache.CachedSize() = %v, expected %v", cache.UsedCapacity(), size)
	}
	if cache.Evictions() != 1 {
		t.Errorf("post-evict cache.evictions = %v, expected 1", cache.Evictions())
	}
	// Check various other stats
	if cache.Len() != int(size) {
		t.Errorf("cache.StatsJSON() returned bad length: %v", cache.Len())
	}
}

func TestLRUIsEvicted(t *testing.T) {
	size := int64(3)
	cache := NewLRUCache(size)

	cache.Set("key1", &CacheValue{}, 1)
	cache.Set("key2", &CacheValue{}, 1)
	cache.Set("key3", &CacheValue{}, 1)
	// lru: [key3, key2, key1]

	// Look up the elements. This will rearrange the LRU ordering.
	cache.Get("key3")
	cache.Get("key2")
	cache.Get("key1")
	// lru: [key1, key2, key3]

	cache.Set("key0", &CacheValue{}, 1)
	// lru: [key0, key1, key2]

	// The least recently used one should have been evicted.
	if _, ok := cache.Get("key3"); ok {
		t.Error("Least recently used element was not evicted.")
	}

	if e, want := cache.Evictions(), int64(1); e != want {
		t.Errorf("evictions: %d, want: %d", e, want)
	}
}
