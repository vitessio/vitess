// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cacheservice

import (
	"fmt"
	"sync"
	"time"
)

// NewConnFunc is a factory method that creates a CacheService instance
// using given CacheServiceConfig.
type NewConnFunc func(config Config) (CacheService, error)

// services stores all supported cache service.
var services = make(map[string]NewConnFunc)

var mu sync.Mutex

// DefaultCacheService decides the default cache service connection.
var DefaultCacheService string

// Config carries config data for CacheService.
type Config struct {
	Address string
	Timeout time.Duration
}

// Result gives the cached data.
type Result struct {
	Key   string
	Value []byte
	Flags uint16
	Cas   uint64
}

// CacheService defines functions to use a cache service.
type CacheService interface {
	// Get returns cached data for given keys.
	Get(keys ...string) (results []Result, err error)
	// Gets returns cached data for given keys, it is an alternative Get api
	// for using with CAS. Gets returns a CAS identifier with the item. If
	// the item's CAS value has changed since you Gets'ed it, it will not be stored.
	Gets(keys ...string) (results []Result, err error)
	// Set set the value with specified cache key.
	Set(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error)
	// Add store the value only if it does not already exist.
	Add(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error)
	// Replace replaces the value, only if the value already exists,
	// for the specified cache key.
	Replace(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error)
	// Append appends the value after the last bytes in an existing item.
	Append(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error)
	// Prepend prepends the value before existing value.
	Prepend(key string, flags uint16, timeout uint64, value []byte) (stored bool, err error)
	// Cas stores the value only if no one else has updated the data since you read it last.
	Cas(key string, flags uint16, timeout uint64, value []byte, cas uint64) (stored bool, err error)
	// Delete delete the value for the specified cache key.
	Delete(key string) (deleted bool, err error)
	// FlushAll purges the entire cache.
	FlushAll() (err error)
	// Stats returns a list of basic stats.
	Stats(argument string) (result []byte, err error)
	// Close closes the CacheService
	Close()
}

// Register a db connection.
func Register(name string, fn NewConnFunc) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := services[name]; ok {
		panic(fmt.Sprintf("register a registered key: %s", name))
	}
	services[name] = fn
}

// Connect returns a CacheService using the given config.
func Connect(config Config) (CacheService, error) {
	mu.Lock()
	defer mu.Unlock()
	if DefaultCacheService == "" {
		if len(services) == 1 {
			for _, fn := range services {
				return fn(config)
			}
		}
		panic("there are more than one service connect func " +
			"registered but no default cache service has been specified.")
	}
	fn, ok := services[DefaultCacheService]
	if !ok {
		panic(fmt.Sprintf("service connect function for given default cache service: %s is not found.", DefaultCacheService))
	}
	return fn(config)
}
