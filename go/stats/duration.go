/*
Copyright 2018 The Vitess Authors

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

package stats

import (
	"strconv"
	"time"

	"vitess.io/vitess/go/sync2"
)

// Duration exports a time.Duration
type Duration struct {
	i sync2.AtomicDuration
}

// NewDuration returns a new Duration
func NewDuration(name string) *Duration {
	v := new(Duration)
	publish(name, v)
	return v
}

// Add adds the provided value to the Duration
func (v *Duration) Add(delta time.Duration) {
	v.i.Add(delta)
}

// Set sets the value
func (v *Duration) Set(value time.Duration) {
	v.i.Set(value)
}

// Get returns the value
func (v *Duration) Get() time.Duration {
	return v.i.Get()
}

// String is the implementation of expvar.var
func (v *Duration) String() string {
	return strconv.FormatInt(int64(v.i.Get()), 10)
}

// DurationFunc converts a function that returns
// an time.Duration as an expvar.
type DurationFunc func() time.Duration

// String is the implementation of expvar.var
func (f DurationFunc) String() string {
	return strconv.FormatInt(int64(f()), 10)
}

// FloatVal is the implementation of MetricFunc
func (f DurationFunc) FloatVal() float64 {
	return f().Seconds()
}
