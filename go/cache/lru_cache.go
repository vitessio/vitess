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

var _ Cache = &LRUCache{}

// LRUCache is a typical LRU cache implementation.  If the cache
// reaches the capacity, the least recently used item is deleted from
// the cache. Note the capacity is not the number of items, but the
// total sum of the CachedSize() of each item.
type LRUCache struct {
	mu sync.Mutex

	// list & table contain *entry objects.
	list  *list.List
	table map[string]*list.Element
	cost  func(interface{}) int64

	size      int64
	capacity  int64
	evictions int64
}

// Item is what is stored in the cache
type Item struct {
	Key   string
	Value interface{}
}

type entry struct {
	key          string
	value        interface{}
	size         int64
	timeAccessed time.Time
}

// NewLRUCache creates a new empty cache with the given capacity.
func NewLRUCache(capacity int64, cost func(interface{}) int64) *LRUCache {
	return &LRUCache{
		list:     list.New(),
		table:    make(map[string]*list.Element),
		capacity: capacity,
		cost:     cost,
	}
}

// Get returns a value from the cache, and marks the entry as most
// recently used.
func (lru *LRUCache) Get(key string) (v interface{}, ok bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	element := lru.table[key]
	if element == nil {
		return nil, false
	}
	lru.moveToFront(element)
	return element.Value.(*entry).value, true
}

// Set sets a value in the cache.
func (lru *LRUCache) Set(key string, value interface{}) bool {
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
func (lru *LRUCache) delete(key string) bool {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	element := lru.table[key]
	if element == nil {
		return false
	}

	lru.list.Remove(element)
	delete(lru.table, key)
	lru.size -= element.Value.(*entry).size
	return true
}

// Delete removes an entry from the cache
func (lru *LRUCache) Delete(key string) {
	lru.delete(key)
}

// Clear will clear the entire cache.
func (lru *LRUCache) Clear() {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	lru.list.Init()
	lru.table = make(map[string]*list.Element)
	lru.size = 0
}

// Len returns the size of the cache (in entries)
func (lru *LRUCache) Len() int {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.list.Len()
}

// SetCapacity will set the capacity of the cache. If the capacity is
// smaller, and the current cache size exceed that capacity, the cache
// will be shrank.
func (lru *LRUCache) SetCapacity(capacity int64) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	lru.capacity = capacity
	lru.checkCapacity()
}

// Wait is a no-op in the LRU cache
func (lru *LRUCache) Wait() {}

// UsedCapacity returns the size of the cache (in bytes)
func (lru *LRUCache) UsedCapacity() int64 {
	return lru.size
}

// MaxCapacity returns the cache maximum capacity.
func (lru *LRUCache) MaxCapacity() int64 {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.capacity
}

// Evictions returns the number of evictions
func (lru *LRUCache) Evictions() int64 {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	return lru.evictions
}

// ForEach yields all the values for the cache, ordered from most recently
// used to least recently used.
func (lru *LRUCache) ForEach(callback func(value interface{}) bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	for e := lru.list.Front(); e != nil; e = e.Next() {
		v := e.Value.(*entry)
		if !callback(v.value) {
			break
		}
	}
}

// Items returns all the values for the cache, ordered from most recently
// used to least recently used.
func (lru *LRUCache) Items() []Item {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	items := make([]Item, 0, lru.list.Len())
	for e := lru.list.Front(); e != nil; e = e.Next() {
		v := e.Value.(*entry)
		items = append(items, Item{Key: v.key, Value: v.value})
	}
	return items
}

func (lru *LRUCache) updateInplace(element *list.Element, value interface{}) {
	valueSize := lru.cost(value)
	sizeDiff := valueSize - element.Value.(*entry).size
	element.Value.(*entry).value = value
	element.Value.(*entry).size = valueSize
	lru.size += sizeDiff
	lru.moveToFront(element)
	lru.checkCapacity()
}

func (lru *LRUCache) moveToFront(element *list.Element) {
	lru.list.MoveToFront(element)
	element.Value.(*entry).timeAccessed = time.Now()
}

func (lru *LRUCache) addNew(key string, value interface{}) {
	newEntry := &entry{key, value, lru.cost(value), time.Now()}
	element := lru.list.PushFront(newEntry)
	lru.table[key] = element
	lru.size += newEntry.size
	lru.checkCapacity()
}

func (lru *LRUCache) checkCapacity() {
	// Partially duplicated from Delete
	for lru.size > lru.capacity {
		delElem := lru.list.Back()
		delValue := delElem.Value.(*entry)
		lru.list.Remove(delElem)
		delete(lru.table, delValue.key)
		lru.size -= delValue.size
		lru.evictions++
	}
}
