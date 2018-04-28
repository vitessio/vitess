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
	i    sync2.AtomicDuration
	help string
}

// NewDuration returns a new Duration.
func NewDuration(name, help string) *Duration {
	v := &Duration{
		help: help,
	}
	publish(name, v)
	return v
}

// Help implements the Variable interface.
func (v Duration) Help() string {
	return v.help
}

// String is the implementation of expvar.var.
func (v Duration) String() string {
	return strconv.FormatInt(int64(v.i.Get()), 10)
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

// DurationFunc allows to provide the value via a custom function.
type DurationFunc struct {
	F    func() time.Duration
	help string
}

// NewDurationFunc creates a new DurationFunc instance and publishes it if name
// is set.
func NewDurationFunc(name string, help string, f func() time.Duration) *DurationFunc {
	df := &DurationFunc{
		F:    f,
		help: help,
	}

	if name != "" {
		publish(name, df)
	}
	return df
}

// Help implements the Variable interface.
func (df DurationFunc) Help() string {
	return df.help
}

// String is the implementation of expvar.var.
func (df DurationFunc) String() string {
	return strconv.FormatInt(int64(df.F()), 10)
}
