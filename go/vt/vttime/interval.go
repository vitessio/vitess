/*
Copyright 2019 The Vitess Authors.

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

package vttime

import (
	"fmt"
	"time"
)

// Interval describes a time interval
type Interval struct {
	earliest time.Time
	latest   time.Time
}

// NewInterval creates a new Interval from the provided times.
// earliest has to be smaller or equal to latest, or an error is returned.
func NewInterval(earliest, latest time.Time) (Interval, error) {
	if latest.Sub(earliest) < 0 {
		return Interval{}, fmt.Errorf("NewInterval: earliest has to be smaller or equal to latest, but got: earliest=%v latest=%v", earliest, latest)
	}
	return Interval{
		earliest: earliest,
		latest:   latest,
	}, nil
}

// Earliest returns the earliest time in the interval. If Interval was
// from calling Now(), it is guaranteed the real time was greater or
// equal than Earliest().
func (i Interval) Earliest() time.Time {
	return i.earliest
}

// Latest returns the latest time in the interval. If Interval was
// from calling Now(), it is guaranteed the real time was lesser or
// equal than Latest().
func (i Interval) Latest() time.Time {
	return i.latest
}

// Less returns true if the provided interval is earlier than the parameter.
// Since both intervals are inclusive, comparison has to be strict.
func (i Interval) Less(other Interval) bool {
	return i.latest.Sub(other.earliest) < 0
}

// IsValid returns true iff latest >= earliest, meaning the interval
// is actually a real valid interval.
func (i Interval) IsValid() bool {
	return i.latest.Sub(i.earliest) >= 0
}
