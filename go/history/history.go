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
	mu      sync.Mutex
	records []interface{}
	last    interface{}
	next    int
	length  int
}

// Return a history with the specified maximum length.
func New(length int) *History {
	return &History{records: make([]interface{}, length)}
}

// Add a new record in a treadsafe manner. If record implements
// Equivalent, and IsEquivalent returns true when called on the last
// previously added record, it will not be added.
func (history *History) Add(record interface{}) {
	history.mu.Lock()
	defer history.mu.Unlock()

	if equiv, ok := record.(Deduplicable); ok && history.length > 0 {
		if equiv.IsDuplicate(history.last) {
			return
		}
	}

	history.records[history.next] = record
	history.last = record

	if history.length < len(history.records) {
		history.length++
	}

	history.next = (history.next + 1) % len(history.records)
}

// Return the kept records in reverse chronological order in a
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
