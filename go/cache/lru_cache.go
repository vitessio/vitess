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

// The implementation borrows heavily from SmallLRUCache (originally by Nathan
// Schrenk). The object maintains a doubly-linked list of elements in the
// When an element is accessed it is promoted to the head of the list, and when
// space is needed the element at the tail of the list (the least recently used
// element) is evicted.
package cache

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type LRUCache struct {
	mu sync.Mutex

	// list & table of *entry objects
	list  *list.List
	table map[string]*list.Element

	// Our current size, in bytes. Obviously a gross simplification and low-grade
	// approximation.
	size uint64

	// How many bytes we are limiting the cache to.
	capacity uint64
}

// Values that go into LRUCache need to satisfy this interface.
type Value interface {
	Size() int
}

type Item struct {
	Key   string
	Value Value
}

type entry struct {
	key           string
	value         Value
	size          int
	time_accessed time.Time
}

func NewLRUCache(capacity uint64) *LRUCache {
	return &LRUCache{
		list:     list.New(),
		table:    make(map[string]*list.Element),
		capacity: capacity,
	}
}

func (self *LRUCache) Get(key string) (v Value, ok bool) {
	self.mu.Lock()
	defer self.mu.Unlock()

	element := self.table[key]
	if element == nil {
		return nil, false
	}
	self.moveToFront(element)
	return element.Value.(*entry).value, true
}

func (self *LRUCache) Set(key string, value Value) {
	self.mu.Lock()
	defer self.mu.Unlock()

	if element := self.table[key]; element != nil {
		self.updateInplace(element, value)
	} else {
		self.addNew(key, value)
	}
}

func (self *LRUCache) SetIfAbsent(key string, value Value) {
	self.mu.Lock()
	defer self.mu.Unlock()

	if element := self.table[key]; element != nil {
		self.moveToFront(element)
	} else {
		self.addNew(key, value)
	}
}

func (self *LRUCache) Delete(key string) bool {
	self.mu.Lock()
	defer self.mu.Unlock()

	element := self.table[key]
	if element == nil {
		return false
	}

	self.list.Remove(element)
	delete(self.table, key)
	self.size -= uint64(element.Value.(*entry).size)
	return true
}

func (self *LRUCache) Clear() {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.list.Init()
	self.table = make(map[string]*list.Element)
	self.size = 0
}

func (self *LRUCache) SetCapacity(capacity uint64) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.capacity = capacity
	self.checkCapacity()
}

func (self *LRUCache) Stats() (length, size, capacity uint64, oldest time.Time) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if lastElem := self.list.Back(); lastElem != nil {
		oldest = lastElem.Value.(*entry).time_accessed
	}
	return uint64(self.list.Len()), self.size, self.capacity, oldest
}

func (self *LRUCache) StatsJSON() string {
	l, s, c, o := self.Stats()
	return fmt.Sprintf("{\"Length\": %v, \"Size\": %v, \"Capacity\": %v, \"OldestAccess\": \"%v\"}", l, s, c, o)
}

func (self *LRUCache) Keys() []string {
	self.mu.Lock()
	defer self.mu.Unlock()

	keys := make([]string, 0, self.list.Len())
	for e := self.list.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(*entry).key)
	}
	return keys
}

func (self *LRUCache) Items() []Item {
	self.mu.Lock()
	defer self.mu.Unlock()

	items := make([]Item, 0, self.list.Len())
	for e := self.list.Front(); e != nil; e = e.Next() {
		v := e.Value.(*entry)
		items = append(items, Item{Key: v.key, Value: v.value})
	}
	return items
}

func (self *LRUCache) updateInplace(element *list.Element, value Value) {
	valueSize := value.Size()
	sizeDiff := valueSize - element.Value.(*entry).size
	element.Value.(*entry).value = value
	element.Value.(*entry).size = valueSize
	self.size += uint64(sizeDiff)
	self.moveToFront(element)
	self.checkCapacity()
}

func (self *LRUCache) moveToFront(element *list.Element) {
	self.list.MoveToFront(element)
	element.Value.(*entry).time_accessed = time.Now()
}

func (self *LRUCache) addNew(key string, value Value) {
	newEntry := &entry{key, value, value.Size(), time.Now()}
	element := self.list.PushFront(newEntry)
	self.table[key] = element
	self.size += uint64(newEntry.size)
	self.checkCapacity()
}

func (self *LRUCache) checkCapacity() {
	// Partially duplicated from Delete
	for self.size > self.capacity {
		delElem := self.list.Back()
		delValue := delElem.Value.(*entry)
		self.list.Remove(delElem)
		delete(self.table, delValue.key)
		self.size -= uint64(delValue.size)
	}
}
