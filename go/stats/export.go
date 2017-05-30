/*
Copyright 2017 Google Inc.

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

// Package stats is a wrapper for expvar. It addtionally
// exports new types that can be used to track performance.
// It also provides a callback hook that allows a program
// to export the variables using methods other than /debug/vars.
// All variables support a String function that
// is expected to return a JSON representation
// of the variable.
// Any function named Add will add the specified
// number to the variable.
// Any function named Counts returns a map of counts
// that can be used by Rates to track rates over time.
package stats

import (
	"bytes"
	"expvar"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sync2"
)

var emitStats = flag.Bool("emit_stats", false, "true iff we should emit stats to push-based monitoring/stats backends")
var statsEmitPeriod = flag.Duration("stats_emit_period", time.Duration(60*time.Second), "Interval between emitting stats to all registered backends")
var statsBackend = flag.String("stats_backend", "influxdb", "The name of the registered push-based monitoring/stats backend to use")

// NewVarHook is the type of a hook to export variables in a different way
type NewVarHook func(name string, v expvar.Var)

type varGroup struct {
	sync.Mutex
	vars       map[string]expvar.Var
	newVarHook NewVarHook
}

func (vg *varGroup) register(nvh NewVarHook) {
	vg.Lock()
	defer vg.Unlock()
	if vg.newVarHook != nil {
		panic("You've already registered a function")
	}
	if nvh == nil {
		panic("nil not allowed")
	}
	vg.newVarHook = nvh
	// Call hook on existing vars because some might have been
	// created before the call to register
	for k, v := range vg.vars {
		nvh(k, v)
	}
	vg.vars = nil
}

func (vg *varGroup) publish(name string, v expvar.Var) {
	vg.Lock()
	defer vg.Unlock()
	expvar.Publish(name, v)
	if vg.newVarHook != nil {
		vg.newVarHook(name, v)
	} else {
		vg.vars[name] = v
	}
}

var defaultVarGroup = varGroup{vars: make(map[string]expvar.Var)}

// Register allows you to register a callback function
// that will be called whenever a new stats variable gets
// created. This can be used to build alternate methods
// of exporting stats variables.
func Register(nvh NewVarHook) {
	defaultVarGroup.register(nvh)
}

// Publish is expvar.Publish+hook
func Publish(name string, v expvar.Var) {
	publish(name, v)
}

func publish(name string, v expvar.Var) {
	defaultVarGroup.publish(name, v)
}

// PushBackend is an interface for any stats/metrics backend that requires data
// to be pushed to it. It's used to support push-based metrics backends, as expvar
// by default only supports pull-based ones.
type PushBackend interface {
	// PushAll pushes all stats from expvar to the backend
	PushAll() error
}

var pushBackends = make(map[string]PushBackend)
var once sync.Once

// RegisterPushBackend allows modules to register PushBackend implementations.
// Should be called on init().
func RegisterPushBackend(name string, backend PushBackend) {
	if _, ok := pushBackends[name]; ok {
		log.Fatalf("PushBackend %s already exists; can't register the same name multiple times", name)
	}
	pushBackends[name] = backend
	if *emitStats {
		// Start a single goroutine to emit stats periodically
		once.Do(func() {
			go emitToBackend(statsEmitPeriod)
		})
	}
}

// emitToBackend does a periodic emit to the selected PushBackend. If a push fails,
// it will be logged as a warning (but things will otherwise proceed as normal).
func emitToBackend(emitPeriod *time.Duration) {
	ticker := time.NewTicker(*emitPeriod)
	defer ticker.Stop()
	for range ticker.C {
		backend, ok := pushBackends[*statsBackend]
		if !ok {
			log.Errorf("No PushBackend registered with name %s", *statsBackend)
			return
		}
		err := backend.PushAll()
		if err != nil {
			// TODO(aaijazi): This might cause log spam...
			log.Warningf("Pushing stats to backend %v failed: %v", *statsBackend, err)
		}
	}
}

// Float is expvar.Float+Get+hook
type Float struct {
	mu sync.Mutex
	f  float64
}

// NewFloat creates a new Float and exports it.
func NewFloat(name string) *Float {
	v := new(Float)
	publish(name, v)
	return v
}

// Add adds the provided value to the Float
func (v *Float) Add(delta float64) {
	v.mu.Lock()
	v.f += delta
	v.mu.Unlock()
}

// Set sets the value
func (v *Float) Set(value float64) {
	v.mu.Lock()
	v.f = value
	v.mu.Unlock()
}

// Get returns the value
func (v *Float) Get() float64 {
	v.mu.Lock()
	f := v.f
	v.mu.Unlock()
	return f
}

// String is the implementation of expvar.var
func (v *Float) String() string {
	return strconv.FormatFloat(v.Get(), 'g', -1, 64)
}

// FloatFunc converts a function that returns
// a float64 as an expvar.
type FloatFunc func() float64

// String is the implementation of expvar.var
func (f FloatFunc) String() string {
	return strconv.FormatFloat(f(), 'g', -1, 64)
}

// Int is expvar.Int+Get+hook
type Int struct {
	i sync2.AtomicInt64
}

// NewInt returns a new Int
func NewInt(name string) *Int {
	v := new(Int)
	if name != "" {
		publish(name, v)
	}
	return v
}

// Add adds the provided value to the Int
func (v *Int) Add(delta int64) {
	v.i.Add(delta)
}

// Set sets the value
func (v *Int) Set(value int64) {
	v.i.Set(value)
}

// Get returns the value
func (v *Int) Get() int64 {
	return v.i.Get()
}

// String is the implementation of expvar.var
func (v *Int) String() string {
	return strconv.FormatInt(v.i.Get(), 10)
}

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

// IntFunc converts a function that returns
// an int64 as an expvar.
type IntFunc func() int64

// String is the implementation of expvar.var
func (f IntFunc) String() string {
	return strconv.FormatInt(f(), 10)
}

// DurationFunc converts a function that returns
// an time.Duration as an expvar.
type DurationFunc func() time.Duration

// String is the implementation of expvar.var
func (f DurationFunc) String() string {
	return strconv.FormatInt(int64(f()), 10)
}

// String is expvar.String+Get+hook
type String struct {
	mu sync.Mutex
	s  string
}

// NewString returns a new String
func NewString(name string) *String {
	v := new(String)
	publish(name, v)
	return v
}

// Set sets the value
func (v *String) Set(value string) {
	v.mu.Lock()
	v.s = value
	v.mu.Unlock()
}

// Get returns the value
func (v *String) Get() string {
	v.mu.Lock()
	s := v.s
	v.mu.Unlock()
	return s
}

// String is the implementation of expvar.var
func (v *String) String() string {
	return strconv.Quote(v.Get())
}

// StringFunc converts a function that returns
// an string as an expvar.
type StringFunc func() string

// String is the implementation of expvar.var
func (f StringFunc) String() string {
	return strconv.Quote(f())
}

// JSONFunc is the public type for a single function that returns json directly.
type JSONFunc func() string

// String is the implementation of expvar.var
func (f JSONFunc) String() string {
	return f()
}

// PublishJSONFunc publishes any function that returns
// a JSON string as a variable. The string is sent to
// expvar as is.
func PublishJSONFunc(name string, f func() string) {
	publish(name, JSONFunc(f))
}

// StringMap is a map of string -> string
type StringMap struct {
	mu     sync.Mutex
	values map[string]string
}

// NewStringMap returns a new StringMap
func NewStringMap(name string) *StringMap {
	v := &StringMap{values: make(map[string]string)}
	publish(name, v)
	return v
}

// Set will set a value (existing or not)
func (v *StringMap) Set(name, value string) {
	v.mu.Lock()
	v.values[name] = value
	v.mu.Unlock()
}

// Get will return the value, or "" f not set.
func (v *StringMap) Get(name string) string {
	v.mu.Lock()
	s := v.values[name]
	v.mu.Unlock()
	return s
}

// String is the implementation of expvar.Var
func (v *StringMap) String() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return stringMapToString(v.values)
}

// StringMapFunc is the function equivalent of StringMap
type StringMapFunc func() map[string]string

// String is used by expvar.
func (f StringMapFunc) String() string {
	m := f()
	if m == nil {
		return "{}"
	}
	return stringMapToString(m)
}

func stringMapToString(m map[string]string) string {
	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")
	firstValue := true
	for k, v := range m {
		if firstValue {
			firstValue = false
		} else {
			fmt.Fprintf(b, ", ")
		}
		fmt.Fprintf(b, "\"%v\": %v", k, strconv.Quote(v))
	}
	fmt.Fprintf(b, "}")
	return b.String()
}
