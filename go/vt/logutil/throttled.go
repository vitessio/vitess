package logutil

import (
	"sync"
	"time"

	log "github.com/golang/glog"
)

// ThrottledLogger will allow logging of messages but won't spam the
// logs.
type ThrottledLogger struct {
	// set at construction
	name        string
	maxInterval time.Duration

	// mu protects the following members
	mu           sync.Mutex
	lastlogTime  time.Time
	skippedCount int
}

// NewThrottledLogger will create a ThrottledLogger with the given
// name and throttling interval.
func NewThrottledLogger(name string, maxInterval time.Duration) *ThrottledLogger {
	return &ThrottledLogger{
		name:        name,
		maxInterval: maxInterval,
	}
}

func (tl *ThrottledLogger) shouldLog() bool {
	now := time.Now()

	tl.mu.Lock()
	defer tl.mu.Unlock()
	if now.Sub(tl.lastlogTime) < tl.maxInterval {
		tl.skippedCount++
		return false
	}
	tl.lastlogTime = now
	if tl.skippedCount > 0 {
		log.Warningf("%v: skipped %v log messages", tl.name, tl.skippedCount)
		tl.skippedCount = 0
	}
	return true
}

// Infof logs an info if not throttled.
func (tl *ThrottledLogger) Infof(format string, v ...interface{}) {
	if !tl.shouldLog() {
		return
	}
	log.Infof(tl.name+": "+format, v...)
}

// Warningf logs a warning if not throttled.
func (tl *ThrottledLogger) Warningf(format string, v ...interface{}) {
	if !tl.shouldLog() {
		return
	}
	log.Warningf(tl.name+": "+format, v...)
}

// Errorf logs an error if not throttled.
func (tl *ThrottledLogger) Errorf(format string, v ...interface{}) {
	if !tl.shouldLog() {
		return
	}
	log.Errorf(tl.name+": "+format, v...)
}
