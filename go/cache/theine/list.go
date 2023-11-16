/*
Copyright 2023 The Vitess Authors.
Copyright 2023 Yiling-J

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

package theine

import (
	"fmt"
	"strings"
)

const (
	LIST_PROBATION uint8 = 1
	LIST_PROTECTED uint8 = 2
)

// List represents a doubly linked list.
// The zero value for List is an empty list ready to use.
type List[K cachekey, V any] struct {
	root     Entry[K, V] // sentinel list element, only &root, root.prev, and root.next are used
	len      int         // current list length(sum of costs) excluding (this) sentinel element
	count    int         // count of entries in list
	capacity uint
	bounded  bool
	listType uint8 // 1 tinylfu list, 2 timerwheel list
}

// New returns an initialized list.
func NewList[K cachekey, V any](size uint, listType uint8) *List[K, V] {
	l := &List[K, V]{listType: listType, capacity: size, root: Entry[K, V]{}}
	l.root.root = true
	l.root.setNext(&l.root)
	l.root.setPrev(&l.root)
	l.len = 0
	l.capacity = size
	if size > 0 {
		l.bounded = true
	}
	return l
}

func (l *List[K, V]) Reset() {
	l.root.setNext(&l.root)
	l.root.setPrev(&l.root)
	l.len = 0
}

// Len returns the number of elements of list l.
// The complexity is O(1).
func (l *List[K, V]) Len() int { return l.len }

func (l *List[K, V]) display() string {
	var s []string
	for e := l.Front(); e != nil; e = e.Next() {
		s = append(s, fmt.Sprintf("%v", e.key))
	}
	return strings.Join(s, "/")
}

func (l *List[K, V]) displayReverse() string {
	var s []string
	for e := l.Back(); e != nil; e = e.Prev() {
		s = append(s, fmt.Sprintf("%v", e.key))
	}
	return strings.Join(s, "/")
}

// Front returns the first element of list l or nil if the list is empty.
func (l *List[K, V]) Front() *Entry[K, V] {
	e := l.root.next()
	if e != &l.root {
		return e
	}
	return nil
}

// Back returns the last element of list l or nil if the list is empty.
func (l *List[K, V]) Back() *Entry[K, V] {
	e := l.root.prev()
	if e != &l.root {
		return e
	}
	return nil
}

// insert inserts e after at, increments l.len, and evicted entry if capacity exceed
func (l *List[K, V]) insert(e, at *Entry[K, V]) *Entry[K, V] {
	var evicted *Entry[K, V]
	if l.bounded && l.len >= int(l.capacity) {
		evicted = l.PopTail()
	}
	e.list = l.listType
	e.setPrev(at)
	e.setNext(at.next())
	e.prev().setNext(e)
	e.next().setPrev(e)
	if l.bounded {
		l.len += int(e.cost.Load())
		l.count += 1
	}
	return evicted
}

// PushFront push entry to list head
func (l *List[K, V]) PushFront(e *Entry[K, V]) *Entry[K, V] {
	return l.insert(e, &l.root)
}

// Push push entry to the back of list
func (l *List[K, V]) PushBack(e *Entry[K, V]) *Entry[K, V] {
	return l.insert(e, l.root.prev())
}

// remove removes e from its list, decrements l.len
func (l *List[K, V]) remove(e *Entry[K, V]) {
	e.prev().setNext(e.next())
	e.next().setPrev(e.prev())
	e.setNext(nil)
	e.setPrev(nil)
	e.list = 0
	if l.bounded {
		l.len -= int(e.cost.Load())
		l.count -= 1
	}
}

// move moves e to next to at.
func (l *List[K, V]) move(e, at *Entry[K, V]) {
	if e == at {
		return
	}
	e.prev().setNext(e.next())
	e.next().setPrev(e.prev())

	e.setPrev(at)
	e.setNext(at.next())
	e.prev().setNext(e)
	e.next().setPrev(e)
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (l *List[K, V]) Remove(e *Entry[K, V]) {
	l.remove(e)
}

// MoveToFront moves element e to the front of list l.
// If e is not an element of l, the list is not modified.
// The element must not be nil.
func (l *List[K, V]) MoveToFront(e *Entry[K, V]) {
	l.move(e, &l.root)
}

// MoveToBack moves element e to the back of list l.
// If e is not an element of l, the list is not modified.
// The element must not be nil.
func (l *List[K, V]) MoveToBack(e *Entry[K, V]) {
	l.move(e, l.root.prev())
}

// MoveBefore moves element e to its new position before mark.
// If e or mark is not an element of l, or e == mark, the list is not modified.
// The element and mark must not be nil.
func (l *List[K, V]) MoveBefore(e, mark *Entry[K, V]) {
	l.move(e, mark.prev())
}

// MoveAfter moves element e to its new position after mark.
// If e or mark is not an element of l, or e == mark, the list is not modified.
// The element and mark must not be nil.
func (l *List[K, V]) MoveAfter(e, mark *Entry[K, V]) {
	l.move(e, mark)
}

func (l *List[K, V]) PopTail() *Entry[K, V] {
	entry := l.root.prev()
	if entry != nil && entry != &l.root {
		l.remove(entry)
		return entry
	}
	return nil
}

func (l *List[K, V]) Contains(entry *Entry[K, V]) bool {
	for e := l.Front(); e != nil; e = e.Next() {
		if e == entry {
			return true
		}
	}
	return false
}
