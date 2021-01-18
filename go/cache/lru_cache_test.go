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
	"encoding/json"
	"testing"
	"time"
)

type CacheValue struct{}

func TestInitialState(t *testing.T) {
	cache := NewLRUCache(5)
	stats := cache.Stats()
	if stats.Length != 0 {
		t.Errorf("length = %v, want 0", stats.Length)
	}
	if stats.Size != 0 {
		t.Errorf("size = %v, want 0", stats.Size)
	}
	if stats.Capacity != 5 {
		t.Errorf("capacity = %v, want 5", stats.Capacity)
	}
	if stats.Evictions != 0 {
		t.Errorf("evictions = %v, want 0", stats.Evictions)
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
	if stats := cache.Stats(); stats.Size != 0 {
		t.Errorf("cache.CachedSize() = %v, expected 0", stats.Size)
	}
	someValue := &CacheValue{}
	key = "key2"
	cache.Set(key, someValue, 20)
	if stats := cache.Stats(); stats.Size != 20 {
		t.Errorf("cache.CachedSize() = %v, expected 20", stats.Size)
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

	if stats := cache.Stats(); stats.Size != 0 {
		t.Errorf("cache.CachedSize() = %v, expected %v", stats.Size, 0)
	}

	someValue := &CacheValue{}
	cache.Set(key, someValue, 20)
	expected := int64(20)
	if stats := cache.Stats(); stats.Size != expected {
		t.Errorf("cache.CachedSize() = %v, expected %v", stats.Size, expected)
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

	if stats := cache.Stats(); stats.Size != 0 {
		t.Errorf("cache.CachedSize() = %v, expected 0", stats.Size)
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

	if stats := cache.Stats(); stats.Size != 0 {
		t.Errorf("cache.CachedSize() = %v, expected 0 after Clear()", stats.Size)
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
	if stats := cache.Stats(); stats.Size != size {
		t.Errorf("cache.CachedSize() = %v, expected %v", stats.Size, size)
	}
	// Insert one more; something should be evicted to make room.
	cache.Set("key4", value, 1)
	st := cache.Stats()
	if st.Size != size {
		t.Errorf("post-evict cache.CachedSize() = %v, expected %v", st.Size, size)
	}
	if st.Evictions != 1 {
		t.Errorf("post-evict cache.evictions = %v, expected 1", st.Evictions)
	}

	// Check json stats
	data := st.JSON()
	m := make(map[string]interface{})
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		t.Errorf("cache.StatsJSON() returned bad json data: %v %v", data, err)
	}
	if m["CachedSize"].(float64) != float64(size) {
		t.Errorf("cache.StatsJSON() returned bad size: %v", m)
	}

	// Check various other stats
	if st.Length != size {
		t.Errorf("cache.StatsJSON() returned bad length: %v", st.Length)
	}
	if st.Size != size {
		t.Errorf("cache.StatsJSON() returned bad size: %v", st.Size)
	}
	if c := cache.Capacity(); c != size {
		t.Errorf("cache.StatsJSON() returned bad length: %v", c)
	}

	// checks StatsJSON on nil
	cache = nil
	if s := cache.Stats().JSON(); s != "{}" {
		t.Errorf("cache.StatsJSON() on nil object returned %v", s)
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
	beforeKey2 := time.Now()
	cache.Get("key2")
	afterKey2 := time.Now()
	cache.Get("key1")
	// lru: [key1, key2, key3]

	cache.Set("key0", &CacheValue{}, 1)
	// lru: [key0, key1, key2]

	// The least recently used one should have been evicted.
	if _, ok := cache.Get("key3"); ok {
		t.Error("Least recently used element was not evicted.")
	}

	st := cache.Stats()

	// Check oldest
	if o := st.Oldest; o.Before(beforeKey2) || o.After(afterKey2) {
		t.Errorf("cache.Oldest returned an unexpected value: got %v, expected a value between %v and %v", o, beforeKey2, afterKey2)
	}

	if e, want := st.Evictions, int64(1); e != want {
		t.Errorf("evictions: %d, want: %d", e, want)
	}
}
