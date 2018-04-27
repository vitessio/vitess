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
	"vitess.io/vitess/go/vt/logutil"
)

// logCounterNegative is for throttling adding a negative value to a counter messages in logs
var logCounterNegative = logutil.NewThrottledLogger("StatsCounterNegative", 1*time.Minute)

// Counter is expvar.Int+Get+hook
type Counter struct {
	i    sync2.AtomicInt64
	help string
}

// NewCounter returns a new Counter
func NewCounter(name string, help string) *Counter {
	v := &Counter{help: help}
	if name != "" {
		publish(name, v)
	}
	return v
}

// Add adds the provided value to the Counter
func (v *Counter) Add(delta int64) {
	if delta < 0 {
		logCounterNegative.Warningf("Adding a negative value to a counter, %v should be a gauge instead", v)
	}
	v.i.Add(delta)
}

// Reset resets the counter value to 0
func (v *Counter) Reset() {
	v.i.Set(int64(0))
}

// Get returns the value
func (v *Counter) Get() int64 {
	return v.i.Get()
}

// String is the implementation of expvar.var
func (v *Counter) String() string {
	return strconv.FormatInt(v.i.Get(), 10)
}

// Help returns the help string
func (v *Counter) Help() string {
	return v.help
}

// CounterFunc converts a function that returns
// an int64 as an expvar.
// For implementations that differentiate between Counters/Gauges,
// CounterFunc's values only go up (or are reset to 0)
type CounterFunc struct {
	Mf   MetricFunc
	help string
}

// NewCounterFunc creates a new CounterFunc instance and publishes it if name is set
func NewCounterFunc(name string, help string, Mf MetricFunc) *CounterFunc {
	c := &CounterFunc{
		Mf:   Mf,
		help: help,
	}

	if name != "" {
		publish(name, c)
	}
	return c
}

// Help returns the help string
func (cf *CounterFunc) Help() string {
	return cf.help
}

// String implements expvar.Var
func (cf *CounterFunc) String() string {
	return cf.Mf.String()
}

// MetricFunc defines an interface for things that can be exported with calls to stats.CounterFunc/stats.GaugeFunc
type MetricFunc interface {
	FloatVal() float64
	String() string
}

// IntFunc converst a function that returns an int64 as both an expvar and a MetricFunc
type IntFunc func() int64

// FloatVal is the implementation of MetricFunc
func (f IntFunc) FloatVal() float64 {
	return float64(f())
}

// String is the implementation of expvar.var
func (f IntFunc) String() string {
	return strconv.FormatInt(f(), 10)
}

// Gauge is an unlabeled metric whose values can go up/down.
type Gauge struct {
	Counter
}

// NewGauge creates a new Gauge and publishes it if name is set
func NewGauge(name string, help string) *Gauge {
	v := &Gauge{Counter: Counter{help: help}}

	if name != "" {
		publish(name, v)
	}
	return v
}

// Set sets the value
func (v *Gauge) Set(value int64) {
	v.Counter.i.Set(value)
}

// Add adds the provided value to the Gauge
func (v *Gauge) Add(delta int64) {
	v.Counter.i.Add(delta)
}

// GaugeFunc converts a function that returns an int64 as an expvar.
// It's a wrapper around CounterFunc for values that go up/down
// for implementations (like Prometheus) that need to differ between Counters and Gauges.
type GaugeFunc struct {
	CounterFunc
}

// NewGaugeFunc creates a new GaugeFunc instance and publishes it if name is set
func NewGaugeFunc(name string, help string, Mf MetricFunc) *GaugeFunc {
	i := &GaugeFunc{
		CounterFunc: CounterFunc{
			Mf:   Mf,
			help: help,
		}}

	if name != "" {
		publish(name, i)
	}
	return i
}
