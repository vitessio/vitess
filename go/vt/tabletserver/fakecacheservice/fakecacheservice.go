// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fakecacheservice provides a fake implementation of cacheservice.CacheService
package fakecacheservice

import (
	"fmt"
	"math/rand"
	"time"

	cs "github.com/youtube/vitess/go/cacheservice"
)

// FakeCacheService is a fake implementation of CacheService
type FakeCacheService struct {
	cacheMap map[string]*cs.Result
}

// NewFakeCacheService creates a FakeCacheService
func NewFakeCacheService() *FakeCacheService {
	return &FakeCacheService{
		cacheMap: make(map[string]*cs.Result),
	}
}

// Get returns cached data for given keys.
func (service *FakeCacheService) Get(keys ...string) ([]cs.Result, error) {
	results := make([]cs.Result, len(keys))
	for i, key := range keys {
		if val, ok := service.cacheMap[key]; ok {
			results[i] = *val
		}
	}
	return results, nil
}

// Gets returns cached data for given keys, it is an alternative Get api
// for using with CAS. Gets returns a CAS identifier with the item. If
// the item's CAS value has changed since you Gets'ed it, it will not be stored.
func (service *FakeCacheService) Gets(keys ...string) ([]cs.Result, error) {
	results := make([]cs.Result, len(keys))
	for i, key := range keys {
		if val, ok := service.cacheMap[key]; ok {
			val.Cas = uint64(rand.Int63())
			results[i] = *val
		}
	}
	return results, nil
}

// Set set the value with specified cache key.
func (service *FakeCacheService) Set(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	service.cacheMap[key] = &cs.Result{
		Key:   key,
		Value: value,
		Flags: flags,
		Cas:   0,
	}
	return true, nil
}

// Add store the value only if it does not already exist.
func (service *FakeCacheService) Add(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	if _, ok := service.cacheMap[key]; ok {
		return false, nil
	}
	service.cacheMap[key] = &cs.Result{
		Key:   key,
		Value: value,
		Flags: flags,
		Cas:   0,
	}
	return true, nil
}

// Replace replaces the value, only if the value already exists,
// for the specified cache key.
func (service *FakeCacheService) Replace(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	result, ok := service.cacheMap[key]
	if !ok {
		return false, nil
	}
	result.Flags = flags
	result.Value = value
	return true, nil
}

// Append appends the value after the last bytes in an existing item.
func (service *FakeCacheService) Append(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	result, ok := service.cacheMap[key]
	if !ok {
		return false, nil
	}
	result.Flags = flags
	result.Value = append(result.Value, value...)
	return true, nil
}

// Prepend prepends the value before existing value.
func (service *FakeCacheService) Prepend(key string, flags uint16, timeout uint64, value []byte) (bool, error) {
	result, ok := service.cacheMap[key]
	if !ok {
		return false, nil
	}
	result.Flags = flags
	result.Value = append(value, result.Value...)
	return true, nil
}

// Cas stores the value only if no one else has updated the data since you read it last.
func (service *FakeCacheService) Cas(key string, flags uint16, timeout uint64, value []byte, cas uint64) (bool, error) {
	result, ok := service.cacheMap[key]
	if !ok || result.Cas != cas {
		return false, nil
	}
	result.Flags = flags
	result.Value = value
	result.Cas = cas
	return true, nil
}

// Delete delete the value for the specified cache key.
func (service *FakeCacheService) Delete(key string) (bool, error) {
	delete(service.cacheMap, key)
	return true, nil
}

// FlushAll purges the entire cache.
func (service *FakeCacheService) FlushAll() error {
	service.cacheMap = make(map[string]*cs.Result)
	return nil
}

// Stats returns a list of basic stats.
func (service *FakeCacheService) Stats(argument string) ([]byte, error) {
	return []byte{}, nil
}

// Close closes the CacheService
func (service *FakeCacheService) Close() {
	service.cacheMap = make(map[string]*cs.Result)
}

// Register registers a fake implementation of cacheservice.CacaheService and returns its registered name
func Register() string {
	name := fmt.Sprintf("fake-%d", rand.Int63())
	cs.Register(name, func(cs.Config) (cs.CacheService, error) {
		return NewFakeCacheService(), nil
	})
	cs.DefaultCacheService = name
	return name
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
