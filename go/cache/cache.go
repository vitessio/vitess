/*
Copyright 2021 The Vitess Authors.

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

// DefaultCacheSize is the default size for a Vitess cache instance.
// If this value is specified in BYTES, Vitess will use a LFU-based cache that keeps track of the total
// memory usage of all cached entries accurately. If this value is specified in ENTRIES, Vitess will
// use the legacy LRU cache implementation which only tracks the amount of entries being stored.
// Changing this value affects:
// - the default values for CLI arguments in VTGate
// - the default values for the config files in VTTablet
// - the default values for the test caches used in integration and end-to-end tests
// Regardless of the default value used here, the user can always override Vitess' configuration to
// force a specific cache type (e.g. when passing a value in ENTRIES to vtgate, the service will use
// a LRU cache).
const DefaultCacheSize = SizeInEntries(5000)

// const DefaultCacheSize = SizeInBytes(64 * 1024 * 1024)

// Cache is a generic interface type for a data structure that keeps recently used
// objects in memory and evicts them when it becomes full.
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, val interface{}) bool
	ForEach(callback func(interface{}) bool)

	Delete(key string)
	Clear()
	Wait()

	Len() int
	Evictions() int64
	UsedCapacity() int64
	MaxCapacity() int64
	SetCapacity(int64)
}

type cachedObject interface {
	CachedSize(alloc bool) int64
}

// NewDefaultCacheImpl returns the default cache implementation for Vitess. If the given capacity
// is given in bytes, the implementation will be LFU-based and keep track of the total memory usage
// for the cache. If the implementation is given in entries, the legacy LRU implementation will be used,
// keeping track
func NewDefaultCacheImpl(capacity Capacity, averageItemSize int64) Cache {
	switch {
	case capacity == nil || (capacity.Entries() == 0 && capacity.Bytes() == 0):
		return &nullCache{}

	case capacity.Bytes() != 0:
		return NewRistrettoCache(capacity.Bytes(), averageItemSize, func(val interface{}) int64 {
			return val.(cachedObject).CachedSize(true)
		})

	default:
		return NewLRUCache(capacity.Entries(), func(_ interface{}) int64 {
			return 1
		})
	}
}

// Capacity is the interface implemented by numeric types that define a cache's capacity
type Capacity interface {
	Bytes() int64
	Entries() int64
}

// SizeInBytes is a Capacity that measures the total size of the cache in Bytes
type SizeInBytes int64

// Bytes returns the size of the cache in Bytes
func (s SizeInBytes) Bytes() int64 {
	return int64(s)
}

// Entries returns 0 because this Capacity measures the cache size in Bytes
func (s SizeInBytes) Entries() int64 {
	return 0
}

// SizeInEntries is a Capacity that measures the total size of the cache in Entries
type SizeInEntries int64

// Bytes returns 0 because this Capacity measures the cache size in Entries
func (s SizeInEntries) Bytes() int64 {
	return 0
}

// Entries returns the size of the cache in Entries
func (s SizeInEntries) Entries() int64 {
	return int64(s)
}
