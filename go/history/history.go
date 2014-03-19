package history

import (
	"sync"
)

// History is a data structure that allows you to keep some number of
// records.
type History struct {
	mu      sync.Mutex
	records []interface{}
	next    int
	length  int
}

// Return a history with the specified maximum length.
func New(length int) *History {
	return &History{records: make([]interface{}, length)}
}

// Add a new record in a treadsafe manner.
func (history *History) Add(record interface{}) {
	history.mu.Lock()
	defer history.mu.Unlock()

	history.records[history.next] = record

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
