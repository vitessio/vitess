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

// Package cache implements a LRU cache.
//
// The implementation borrows heavily from SmallLRUCache
// (originally by Nathan Schrenk). The object maintains a doubly-linked list of
// elements. When an element is accessed, it is promoted to the head of the
// list. When space is needed, the element at the tail of the list
// (the least recently used element) is evicted.
package cache

import (
	"container/list"
	"sync"
	"time"
)

// LRUCache is a typical LRU cache implementation.  If the cache
// reaches the capacity, the least recently used item is deleted from
// the cache.
type LRUCache[T any] struct {
	mu sync.Mutex

	// list & table contain *entry objects.
	list  *list.List
	table map[string]*list.Element

	size      int64
	capacity  int64
	evictions int64
	hits      int64
	misses    int64
}

// Item is what is stored in the cache
type Item[T any] struct {
	Key   string
	Value T
}

type entry[T any] struct {
	key          string
	value        T
	timeAccessed time.Time
}

// NewLRUCache creates a new empty cache with the given capacity.
func NewLRUCache[T any](capacity int64) *LRUCache[T] {
	return &LRUCache[T]{
		list:     list.New(),
		table:    make(map[string]*list.Element),
		capacity: capacity,
	}
}

// Get returns a value from the cache, and marks the entry as most
// recently used.
func (lru *LRUCache[T]) Get(key string) (v T, ok bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	element := lru.table[key]
	if element == nil {
		lru.misses++
		return *new(T), false
	}
	lru.moveToFront(element)
	lru.hits++
	return element.Value.(*entry[T]).value, true
}

// Set sets a value in the cache.
func (lru *LRUCache[T]) Set(key string, value T) bool {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if element := lru.table[key]; element != nil {
		lru.updateInplace(element, value)
	} else {
		lru.addNew(key, value)
	}
	// the LRU cache cannot fail to insert items; it always returns true
	return true
}

// Delete removes an entry from the cache, and returns if the entry existed.
func (lru *LRUCache[T]) delete(key string) bool {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	element := lru.table[key]
	if element == nil {
		return false
	}

	lru.list.Remove(element)
	delete(lru.table, key)
	lru.size--
	return true
}

// Delete removes an entry from the cache
func (lru *LRUCache[T]) Delete(key string) {
	lru.delete(key)
}

// Len returns the size of the cache (in entries)
func (lru *LRUCache[T]) Len() int {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.list.Len()
}

// SetCapacity will set the capacity of the cache. If the capacity is
// smaller, and the current cache size exceed that capacity, the cache
// will be shrank.
func (lru *LRUCache[T]) SetCapacity(capacity int64) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	lru.capacity = capacity
	lru.checkCapacity()
}

// UsedCapacity returns the size of the cache (in bytes)
func (lru *LRUCache[T]) UsedCapacity() int64 {
	return lru.size
}

// MaxCapacity returns the cache maximum capacity.
func (lru *LRUCache[T]) MaxCapacity() int64 {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.capacity
}

// Evictions returns the number of evictions
func (lru *LRUCache[T]) Evictions() int64 {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.evictions
}

// Hits returns number of cache hits since creation
func (lru *LRUCache[T]) Hits() int64 {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.hits
}

// Misses returns number of cache misses since creation
func (lru *LRUCache[T]) Misses() int64 {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.misses
}

// Items returns all the values for the cache, ordered from most recently
// used to least recently used.
func (lru *LRUCache[T]) Items() []Item[T] {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	items := make([]Item[T], 0, lru.list.Len())
	for e := lru.list.Front(); e != nil; e = e.Next() {
		v := e.Value.(*entry[T])
		items = append(items, Item[T]{Key: v.key, Value: v.value})
	}
	return items
}

func (lru *LRUCache[T]) updateInplace(element *list.Element, value T) {
	element.Value.(*entry[T]).value = value
	lru.moveToFront(element)
	lru.checkCapacity()
}

func (lru *LRUCache[T]) moveToFront(element *list.Element) {
	lru.list.MoveToFront(element)
	element.Value.(*entry[T]).timeAccessed = time.Now()
}

func (lru *LRUCache[T]) addNew(key string, value T) {
	newEntry := &entry[T]{key, value, time.Now()}
	element := lru.list.PushFront(newEntry)
	lru.table[key] = element
	lru.size++
	lru.checkCapacity()
}

func (lru *LRUCache[T]) checkCapacity() {
	// Partially duplicated from Delete
	for lru.size > lru.capacity {
		delElem := lru.list.Back()
		delValue := delElem.Value.(*entry[T])
		lru.list.Remove(delElem)
		delete(lru.table, delValue.key)
		lru.size--
		lru.evictions++
	}
}
