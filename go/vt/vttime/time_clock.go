/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
