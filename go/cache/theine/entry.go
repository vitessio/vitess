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

import "sync/atomic"

const (
	NEW int8 = iota
	REMOVE
	UPDATE
)

type ReadBufItem[K cachekey, V any] struct {
	entry *Entry[K, V]
	hash  uint64
}
type WriteBufItem[K cachekey, V any] struct {
	entry      *Entry[K, V]
	costChange int64
	code       int8
}

type MetaData[K cachekey, V any] struct {
	prev *Entry[K, V]
	next *Entry[K, V]
}

type Entry[K cachekey, V any] struct {
	key       K
	value     V
	meta      MetaData[K, V]
	cost      atomic.Int64
	frequency atomic.Int32
	epoch     atomic.Uint32
	removed   bool
	deque     bool
	root      bool
	list      uint8 // used in slru, probation or protected
}

func NewEntry[K cachekey, V any](key K, value V, cost int64) *Entry[K, V] {
	entry := &Entry[K, V]{
		key:   key,
		value: value,
	}
	entry.cost.Store(cost)
	return entry
}

func (e *Entry[K, V]) Next() *Entry[K, V] {
	if p := e.meta.next; !p.root {
		return e.meta.next
	}
	return nil
}

func (e *Entry[K, V]) Prev() *Entry[K, V] {
	if p := e.meta.prev; !p.root {
		return e.meta.prev
	}
	return nil
}

func (e *Entry[K, V]) prev() *Entry[K, V] {
	return e.meta.prev
}

func (e *Entry[K, V]) next() *Entry[K, V] {
	return e.meta.next
}

func (e *Entry[K, V]) setPrev(entry *Entry[K, V]) {
	e.meta.prev = entry
}

func (e *Entry[K, V]) setNext(entry *Entry[K, V]) {
	e.meta.next = entry
}
