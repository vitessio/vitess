// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakecacheservice provides a fake implementation of cacheservice.CacheService
package fakecacheservice

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	cs "github.com/youtube/vitess/go/cacheservice"
	"github.com/youtube/vitess/go/sync2"
)

var errCacheService = "cacheservice error"

// FakeCacheService is a fake implementation of CacheService
type FakeCacheService struct {
	cache *Cache
}

// Cache is a cache like data structure.
type Cache struct {
	mu                      sync.Mutex
	data                    map[string]*cs.Result
	enableCacheServiceError sync2.AtomicInt32
}

// Set sets a key and associated value to the cache.
func (cache *Cache) Set(key string, val *cs.Result) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	var newVal cs.Result
	newVal = *val
	cache.data[key] = &newVal
}

// Get gets the value from cache given the key.
func (cache *Cache) Get(key string) (*cs.Result, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	val, ok := cache.data[key]
	if !ok {
		return nil, ok
	}
	var newVal cs.Result
	newVal = *val
	return &newVal, ok
}

// Delete deletes the given key from the cache.
func (cache *Cache) Delete(key string) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	delete(cache.data, key)
}

// Clear empties the cache.
func (cache *Cache) Clear() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.data = make(map[string]*cs.Result)
}

// EnableCacheServiceError makes cache service return error.
func (cache *Cache) EnableCacheServiceError() {
	cache.enableCacheServiceError.Set(1)
}

// DisableCacheServiceError makes cache service back to normal.
func (cache *Cache) DisableCacheServiceError() {
	cache.enableCacheServiceError.Set(0)
}

// NewFakeCacheService creates a FakeCacheService
func NewFakeCacheService(cache *Cache) *FakeCacheService {
	return &FakeCacheService{
		cache: cache,
	}
}

// Get returns cached data for given keys.
func (service *FakeCacheService) Get(keys ...string) ([]cs.Result, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return nil, fmt.Errorf(errCacheService)
	}
	results := make([]cs.Result, 0, len(keys))
	for _, key := range keys {
		if val, ok := service.cache.Get(key); ok {
			results = append(results, *val)
		}
	}
	return results, nil
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (service *FakeCacheService) Gets(keys ...string) ([]cs.Result, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return nil, fmt.Errorf(errCacheService)
	}
	results := make([]cs.Result, 0, len(keys))
	for _, key := range keys {
		if val, ok := service.cache.Get(key); ok {
			val.Cas = uint64(rand.Int63())
			service.cache.Set(key, val)
			results = append(results, *val)
		}
	}
	return results, nil
}

// Set set the value with specified cache key.
func (service *FakeCacheService) Set(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return false, fmt.Errorf(errCacheService)
	}
	service.cache.Set(key, &cs.Result{
		Key:   key,
		Value: value,
		Flags: flags,
		Cas:   0,
	})
	return true, nil
}

// Add store the value only if it does not already exist.
func (service *FakeCacheService) Add(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return false, fmt.Errorf(errCacheService)
	}
	if _, ok := service.cache.Get(key); ok {
		return false, nil
	}
	service.cache.Set(key, &cs.Result{
		Key:   key,
		Value: value,
		Flags: flags,
		Cas:   0,
	})
	return true, nil
}

// Replace replaces the value, only if the value already exists,
// for the specified cache key.
func (service *FakeCacheService) Replace(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return false, fmt.Errorf(errCacheService)
	}
	result, ok := service.cache.Get(key)
	if !ok {
		return false, nil
	}
	result.Flags = flags
	result.Value = value
	service.cache.Set(key, result)
	return true, nil
}

// Append appends the value after the last bytes in an existing item.
func (service *FakeCacheService) Append(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return false, fmt.Errorf(errCacheService)
	}
	result, ok := service.cache.Get(key)
	if !ok {
		return false, nil
	}
	result.Flags = flags
	result.Value = append(result.Value, value...)
	service.cache.Set(key, result)
	return true, nil
}

// Prepend prepends the value before existing value.
func (service *FakeCacheService) Prepend(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return false, fmt.Errorf(errCacheService)
	}
	result, ok := service.cache.Get(key)
	if !ok {
		return false, nil
	}
	result.Flags = flags
	result.Value = append(value, result.Value...)
	service.cache.Set(key, result)
	return true, nil
}

// Cas stores the value only if no one else has updated the data since you read it last.
func (service *FakeCacheService) Cas(key string, flags uint16, timeout uint64, value []byte, cas uint64) (bool, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return false, fmt.Errorf(errCacheService)
	}
	result, ok := service.cache.Get(key)
	if !ok || result.Cas != cas {
		return false, nil
	}
	result.Flags = flags
	result.Value = value
	result.Cas = cas
	service.cache.Set(key, result)
	return true, nil
}

// Delete delete the value for the specified cache key.
func (service *FakeCacheService) Delete(key string) (bool, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return false, fmt.Errorf(errCacheService)
	}
	service.cache.Delete(key)
	return true, nil
}

// FlushAll purges the entire cache.
func (service *FakeCacheService) FlushAll() error {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return fmt.Errorf(errCacheService)
	}
	service.cache.Clear()
	return nil
}

// Stats returns a list of basic stats.
func (service *FakeCacheService) Stats(key string) ([]byte, error) {
	if service.cache.enableCacheServiceError.Get() == 1 {
		return nil, fmt.Errorf(errCacheService)
	}
	return []byte{}, nil
}

// Close closes the CacheService
func (service *FakeCacheService) Close() {
}

// Register registers a fake implementation of cacheservice.CacaheService and returns its registered name
func Register() *Cache {
	name := fmt.Sprintf("fake-%d", rand.Int63())
	cache := &Cache{data: make(map[string]*cs.Result)}
	cs.Register(name, func(cs.Config) (cs.CacheService, error) {
		return NewFakeCacheService(cache), nil
	})
	cs.DefaultCacheService = name
	return cache
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
