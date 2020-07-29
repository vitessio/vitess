/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
