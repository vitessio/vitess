package throttler

import (
	"time"
)

// record is a single observation.
type record struct {
	// time is the time at which "value" was observed.
	time time.Time
	// value is the value of interest at the given time e.g. the number of
	// transactions per seconds.
	value int64
}
