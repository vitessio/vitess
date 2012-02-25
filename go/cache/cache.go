/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Copyright 2009 Google Inc.  All rights reserved.
// Author: jacobsa@google.com (Aaron Jacobs)
//
// This package defines an interface for a cache. There is currently one
// implementation, lru_cache, but there may be more in the future. For example,
// we may define an object that accepts a Cache and makes it thread-safe by
// owning it in a goroutine that calls are forwarded to over a channel.
package cache

type Value interface {
	Size() int
}

type Item struct {
	Key   string
	Value Value
}

// A function that can modify a cache value "atomically", at least with respect
// to other cache clients.
type CacheModifyFunction func(Value) Value

type CacheOptions struct{}

type Cache interface {
	// Find an element with the given key in the cache. If it exists, returns the
	// value. The second return parameter indicates whether it was found.
	// The caller should not modify the value's underlying bytes.
	Get(key string) (v Value, ok bool)

	// Insert an element into the cache.
	Set(key string, value Value)

	// Insert element only if not already present
	SetIfAbsent(key string, value Value)

	// Remove the cache entry with the given key, if any. Return a bool indicating
	// whether there was such an entry.
	Delete(key string) bool

	// Change capacity of cache
	SetCapacity(capacity uint64)

	// Clear all elements of the cache. Note that this might not necessarily free
	// the associated memory.
	Clear()

	// Returns the number of items in the cache.
	Len() uint64

	// Return the total size in bytes of the entries currently in the cache.
	Size() uint64

	// Return the maximum size in bytes for the entries in the cache.
	Capacity() uint64

	String() string

	Keys() []string
	Items() []Item
}
