/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package history implements a circular buffer with adjacent-item deduplication.
package history

import (
	"sync"
)

// Deduplicable is an interface that records should implement if the
// history should perform their deduplication. An example would be
// deduplicating records whose only difference is their timestamp.
type Deduplicable interface {
	// IsDuplicate returns true if other is considered to be a
	// duplicate of the calling instance.
	IsDuplicate(interface{}) bool
}

// History is a data structure that allows you to keep some number of
// records.
type History struct {
	mu        sync.Mutex
	records   []interface{}
	lastAdded interface{}
	latest    interface{}
	next      int
	length    int
}

// New returns a History with the specified maximum length.
func New(length int) *History {
	return &History{records: make([]interface{}, length)}
}

// Add a new record in a threadsafe manner. If record implements
// Deduplicable, and IsDuplicate returns true when called on the last
// previously added record, it will not be added.
func (history *History) Add(record interface{}) {
	history.mu.Lock()
	defer history.mu.Unlock()

	history.latest = record

	if equiv, ok := record.(Deduplicable); ok && history.length > 0 {
		if equiv.IsDuplicate(history.lastAdded) {
			return
		}
	}

	history.records[history.next] = record
	history.lastAdded = record

	if history.length < len(history.records) {
		history.length++
	}

	history.next = (history.next + 1) % len(history.records)
}

// Records returns the kept records in reverse chronological order in a
// threadsafe manner.
func (history *History) Records() []interface{} {
	history.mu.Lock()
	defer history.mu.Unlock()

	records := make([]interface{}, 0, history.length)
	records = append(records, history.records[history.next:history.length]...)
	records = append(records, history.records[:history.next]...)

	// In place reverse.
	for i := 0; i < history.length/2; i++ {
		records[i], records[history.length-i-1] = records[history.length-i-1], records[i]
	}

	return records
}

// Latest returns the record most recently passed to Add(),
// regardless of whether it was actually added or dropped as a duplicate.
func (history *History) Latest() interface{} {
	history.mu.Lock()
	defer history.mu.Unlock()
	return history.latest
}
