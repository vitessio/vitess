package vttime

import (
	"flag"
	"time"
)

var (
	uncertainty = flag.Duration("vttime_time_clock_uncertainty", 10*time.Millisecond, "An assumed time uncertainty on the local machine that will be used by time-based implementation of Clock.")
)

// TimeClock is an implementation of Clock that uses time.Now() and a
// flag-configured uncertainty.
type TimeClock struct{}

// Now is part of the Clock interface.
func (t TimeClock) Now() (Interval, error) {
	now := time.Now()
	return NewInterval(now.Add(-(*uncertainty)), now.Add(*uncertainty))
}

func init() {
	clockTypes["time"] = TimeClock{}
}
