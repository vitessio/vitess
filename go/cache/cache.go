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

// Cache is a generic interface type for a data structure that keeps recently used
// objects in memory and evicts them when it becomes full.
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, val interface{}) bool
	ForEach(callback func(interface{}) bool)

	Delete(key string)
	Clear()

	// Wait waits for all pending operations on the cache to settle. Since cache writes
	// are asynchronous, a write may not be immediately accessible unless the user
	// manually calls Wait.
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

// NewDefaultCacheImpl returns the default cache implementation for Vitess. The options in the
// Config struct control the memory and entry limits for the cache, and the underlying cache
// implementation.
func NewDefaultCacheImpl(cfg *Config) Cache {
	switch {
	case cfg == nil:
		return &nullCache{}

	case cfg.LFU:
		if cfg.MaxEntries == 0 || cfg.MaxMemoryUsage == 0 {
			return &nullCache{}
		}
		return NewRistrettoCache(cfg.MaxEntries, cfg.MaxMemoryUsage, func(val interface{}) int64 {
			return val.(cachedObject).CachedSize(true)
		})

	default:
		if cfg.MaxEntries == 0 {
			return &nullCache{}
		}
		return NewLRUCache(cfg.MaxEntries, func(_ interface{}) int64 {
			return 1
		})
	}
}

// Config is the configuration options for a cache instance
type Config struct {
	// MaxEntries is the estimated amount of entries that the cache will hold at capacity
	MaxEntries int64
	// MaxMemoryUsage is the maximum amount of memory the cache can handle
	MaxMemoryUsage int64
	// LFU toggles whether to use a new cache implementation with a TinyLFU admission policy
	LFU bool
}

// DefaultConfig is the default configuration for a cache instance in Vitess
var DefaultConfig = &Config{
	MaxEntries:     5000,
	MaxMemoryUsage: 32 * 1024 * 1024,
	LFU:            true,
}
