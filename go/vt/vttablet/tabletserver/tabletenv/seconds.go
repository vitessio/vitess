package tabletenv

import (
	"flag"
	"time"
)

// Seconds provides convenience functions for extracting
// duration from flaot64 seconds values.
type Seconds float64

// SecondsVar is like a flag.Float64Var, but it works for Seconds.
func SecondsVar(p *Seconds, name string, value Seconds, usage string) {
	flag.Float64Var((*float64)(p), name, float64(value), usage)
}

// Get converts Seconds to time.Duration
func (s Seconds) Get() time.Duration {
	return time.Duration(s * Seconds(1*time.Second))
}

// Set sets the value from time.Duration
func (s *Seconds) Set(d time.Duration) {
	*s = Seconds(d) / Seconds(1*time.Second)
}
