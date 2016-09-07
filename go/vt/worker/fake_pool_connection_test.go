// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconnpool"
)

const appendEntry = -1

// FakePoolConnection fakes out a MySQL database and implements
// dbconnpool.PoolConnection.
// The fake will error the test if an unexpected query was received.
type FakePoolConnection struct {
	t *testing.T
	// name is used in log output to reference this specific instance.
	name   string
	closed bool

	// mu guards all fields in the group below.
	// (vtworker writes multi-threaded. Therefore, synchronization is required.)
	// This mutex effectively serializes the parallel writes and makes the test
	// more deterministic. For example, you can set a callback on the last write
	// and have the guarantee that no other writes occur during the callback.
	mu                        sync.Mutex
	expectedExecuteFetch      []ExpectedExecuteFetch
	expectedExecuteFetchIndex int
	// Infinite is true when executed queries beyond our expectation list should
	// respond with the last entry from the list.
	infinite bool
}

// ExpectedExecuteFetch defines for an expected query the to be faked output.
type ExpectedExecuteFetch struct {
	Query       string
	MaxRows     int
	WantFields  bool
	QueryResult *sqltypes.Result
	Error       error
	// AfterFunc is a callback which is executed while the query is executed i.e.,
	// before the fake responds to the client.
	AfterFunc func()
}

// NewFakePoolConnectionQuery creates a new fake database.
// Set "name" to distinguish between the different instances in your tests.
func NewFakePoolConnectionQuery(t *testing.T, name string) *FakePoolConnection {
	return &FakePoolConnection{t: t, name: name}
}

func (f *FakePoolConnection) addExpectedExecuteFetch(entry ExpectedExecuteFetch) {
	f.addExpectedExecuteFetchAtIndex(appendEntry, entry)
}

func (f *FakePoolConnection) enableInfinite() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.infinite = true
}

// addExpectedExecuteFetchAtIndex inserts a new entry at index.
// index values start at 0.
func (f *FakePoolConnection) addExpectedExecuteFetchAtIndex(index int, entry ExpectedExecuteFetch) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.expectedExecuteFetch == nil || index < 0 || index >= len(f.expectedExecuteFetch) {
		index = appendEntry
	}
	if index == appendEntry {
		f.expectedExecuteFetch = append(f.expectedExecuteFetch, entry)
	} else {
		// Grow the slice by one element.
		if cap(f.expectedExecuteFetch) == len(f.expectedExecuteFetch) {
			f.expectedExecuteFetch = append(f.expectedExecuteFetch, make([]ExpectedExecuteFetch, 1)...)
		} else {
			f.expectedExecuteFetch = f.expectedExecuteFetch[0 : len(f.expectedExecuteFetch)+1]
		}
		// Use copy to move the upper part of the slice out of the way and open a hole.
		copy(f.expectedExecuteFetch[index+1:], f.expectedExecuteFetch[index:])
		// Store the new value.
		f.expectedExecuteFetch[index] = entry
	}
}

func (f *FakePoolConnection) addExpectedQuery(query string, err error) {
	f.addExpectedExecuteFetch(ExpectedExecuteFetch{
		Query:       query,
		QueryResult: &sqltypes.Result{},
		Error:       err,
	})
}

func (f *FakePoolConnection) addExpectedQueryAtIndex(index int, query string, err error) {
	f.addExpectedExecuteFetchAtIndex(index, ExpectedExecuteFetch{
		Query:       query,
		QueryResult: &sqltypes.Result{},
		Error:       err,
	})
}

// getEntry returns the expected entry at "index". If index is out of bounds,
// the return value will be nil.
func (f *FakePoolConnection) getEntry(index int) *ExpectedExecuteFetch {
	f.mu.Lock()
	defer f.mu.Unlock()

	if index < 0 || index >= len(f.expectedExecuteFetch) {
		panic(fmt.Sprintf("index out of range. current length: %v", len(f.expectedExecuteFetch)))
	}

	return &f.expectedExecuteFetch[index]
}

func (f *FakePoolConnection) deleteAllEntries() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.expectedExecuteFetch = make([]ExpectedExecuteFetch, 0)
	f.expectedExecuteFetchIndex = 0
}

func (f *FakePoolConnection) deleteAllEntriesAfterIndex(index int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if index < 0 || index >= len(f.expectedExecuteFetch) {
		panic(fmt.Sprintf("index out of range. current length: %v", len(f.expectedExecuteFetch)))
	}

	if index+1 < f.expectedExecuteFetchIndex {
		// Don't delete entries which were already answered.
		return
	}

	f.expectedExecuteFetch = f.expectedExecuteFetch[:index+1]
}

// verifyAllExecutedOrFail checks that all expected queries where actually
// received and executed. If not, it will let the test fail.
func (f *FakePoolConnection) verifyAllExecutedOrFail() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.expectedExecuteFetchIndex != len(f.expectedExecuteFetch) {
		f.t.Errorf("%v: not all expected queries were executed. leftovers: %v", f.name, f.expectedExecuteFetch[f.expectedExecuteFetchIndex:])
	}
}

func (f *FakePoolConnection) getFactory() func() (dbconnpool.PoolConnection, error) {
	return func() (dbconnpool.PoolConnection, error) {
		return f, nil
	}
}

// ExecuteFetch implements dbconnpool.PoolConnection.
func (f *FakePoolConnection) ExecuteFetch(query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	index := f.expectedExecuteFetchIndex
	if f.infinite && index == len(f.expectedExecuteFetch) {
		// Although we already executed all queries, we'll continue to answer the
		// last one in the infinite mode.
		index--
	}
	if index >= len(f.expectedExecuteFetch) {
		f.t.Errorf("%v: got unexpected out of bound fetch: %v >= %v", f.name, index, len(f.expectedExecuteFetch))
		return nil, errors.New("unexpected out of bound fetch")
	}
	entry := f.expectedExecuteFetch[index]

	f.expectedExecuteFetchIndex++
	// If the infinite mode is on, reverse the increment and keep the index at
	// len(f.expectedExecuteFetch).
	if f.infinite && f.expectedExecuteFetchIndex > len(f.expectedExecuteFetch) {
		f.expectedExecuteFetchIndex--
	}

	if entry.AfterFunc != nil {
		defer entry.AfterFunc()
	}

	expected := entry.Query
	if strings.HasSuffix(expected, "*") {
		if !strings.HasPrefix(query, expected[0:len(expected)-1]) {
			f.t.Errorf("%v: got unexpected query start (index=%v): %v != %v", f.name, index, query, expected)
		}
	} else {
		if query != expected {
			f.t.Errorf("%v: got unexpected query (index=%v): %v != %v", f.name, index, query, expected)
			return nil, errors.New("unexpected query")
		}
	}
	f.t.Logf("ExecuteFetch: %v: %v", f.name, query)
	if entry.Error != nil {
		return nil, entry.Error
	}
	return entry.QueryResult, nil
}

// ExecuteStreamFetch implements dbconnpool.PoolConnection.
func (f *FakePoolConnection) ExecuteStreamFetch(query string, callback func(*sqltypes.Result) error, streamBufferSize int) error {
	return nil
}

// ID implements dbconnpool.PoolConnection.
func (f *FakePoolConnection) ID() int64 {
	return 1
}

// Close implements dbconnpool.PoolConnection.
func (f *FakePoolConnection) Close() {
	f.closed = true
}

// IsClosed implements dbconnpool.PoolConnection.
func (f *FakePoolConnection) IsClosed() bool {
	return f.closed
}

// Recycle implements dbconnpool.PoolConnection.
func (f *FakePoolConnection) Recycle() {
}

// Reconnect implements dbconnpool.PoolConnection.
func (f *FakePoolConnection) Reconnect() error {
	return nil
}
