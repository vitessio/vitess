package worker

import (
	"errors"
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
}

// ExpectedExecuteFetch defines for an expected query the to be faked output.
type ExpectedExecuteFetch struct {
	Query       string
	MaxRows     int
	WantFields  bool
	QueryResult *sqltypes.Result
	Error       error
}

// NewFakePoolConnectionQuery creates a new fake database.
// Set "name" to distinguish between the different instances in your tests.
func NewFakePoolConnectionQuery(t *testing.T, name string) *FakePoolConnection {
	return &FakePoolConnection{t: t, name: name}
}

func (f *FakePoolConnection) addExpectedExecuteFetch(entry ExpectedExecuteFetch) {
	f.addExpectedExecuteFetchAtIndex(appendEntry, entry)
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
		f.expectedExecuteFetch = f.expectedExecuteFetch[0 : len(f.expectedExecuteFetch)+1]
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
	if index >= len(f.expectedExecuteFetch) {
		f.t.Errorf("%v: got unexpected out of bound fetch: %v >= %v", f.name, index, len(f.expectedExecuteFetch))
		return nil, errors.New("unexpected out of bound fetch")
	}
	entry := f.expectedExecuteFetch[index]

	f.expectedExecuteFetchIndex++

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
