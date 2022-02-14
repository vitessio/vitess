package automation

import (
	"strconv"
	"sync/atomic"
)

// IDGenerator generates unique task and cluster operation IDs.
type IDGenerator struct {
	counter int64
}

// GetNextID returns an ID which wasn't returned before.
func (ig *IDGenerator) GetNextID() string {
	return strconv.FormatInt(atomic.AddInt64(&ig.counter, 1), 10)
}
