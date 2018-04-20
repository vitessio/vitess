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

	log "github.com/golang/glog"
)

var (
	// clockTypes maps implementation name to Clock object.
	// Should only be written to at init() time.
	clockTypes = make(map[string]Clock)

	// defaultClockType is the flag used to define the runtime clock type.
	defaultClockType = flag.String("vttime_default_clock_type", "time", "The type of clock to be used by default by vttime library.")
)

// Clock returns the current time.
type Clock interface {
	// Now returns the current time as Interval.
	// This method should be thread safe (i.e. multipe go routines can
	// safely call this at the same time).
	// The returned interval is guaranteed to have earliest <= latest,
	// and all implementations enforce it.
	Now() (Interval, error)
}

// GetClock returns the global Clock object.
// Since it depends on flags, be sure to call this after they have been parsed
// (i.e. *not* in init() functions), otherwise this will panic.
func GetClock() Clock {
	if !flag.Parsed() {
		panic("GetClock() called before flags are parsed")
	}

	c, ok := clockTypes[*defaultClockType]
	if !ok {
		log.Fatalf("No Clock type named %v", *defaultClockType)
	}
	return c
}
