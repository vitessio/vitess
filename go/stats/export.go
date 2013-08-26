// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"expvar"
	"strconv"
	"sync"
	"time"

	"github.com/youtube/vitess/go/sync2"
)

type NewVarHook func(name string, v expvar.Var)

type varGroup struct {
	sync.Mutex
	vars       map[string]expvar.Var
	newVarHook NewVarHook
}

func (vg *varGroup) register(nvh NewVarHook) {
	vg.Lock()
	defer vg.Unlock()
	vg.newVarHook = nvh
	if nvh == nil {
		return
	}
	// Call hook on existing vars because some might have been
	// created before the call to register
	for k, v := range vg.vars {
		nvh(k, v)
	}
}

func (vg *varGroup) publish(name string, v expvar.Var) {
	vg.Lock()
	defer vg.Unlock()
	expvar.Publish(name, v)
	vg.vars[name] = v
	if vg.newVarHook != nil {
		vg.newVarHook(name, v)
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
	defaultVarGroup.publish(name, v)
}

// Float is expvar.Float+Get+hook
type Float struct {
	mu sync.Mutex
	f  float64
}

func NewFloat(name string) *Float {
	v := new(Float)
	Publish(name, v)
	return v
}

func (v *Float) Add(delta float64) {
	v.mu.Lock()
	v.f += delta
	v.mu.Unlock()
}

func (v *Float) Set(value float64) {
	v.mu.Lock()
	v.f = value
	v.mu.Unlock()
}

func (v *Float) Get() float64 {
	v.mu.Lock()
	f := v.f
	v.mu.Unlock()
	return f
}

func (v *Float) String() string {
	return strconv.FormatFloat(v.Get(), 'g', -1, 64)
}

// FloatFunc converts a function that returns
// a float64 as an expvar.
type FloatFunc func() float64

func (f FloatFunc) String() string {
	return strconv.FormatFloat(f(), 'g', -1, 64)
}

// Int is expvar.Int+Get+hook
type Int struct {
	i sync2.AtomicInt64
}

func NewInt(name string) *Int {
	v := new(Int)
	Publish(name, v)
	return v
}

func (v *Int) Add(delta int64) {
	v.i.Add(delta)
}

func (v *Int) Set(value int64) {
	v.i.Set(value)
}

func (v *Int) Get() int64 {
	return v.i.Get()
}

func (v *Int) String() string {
	return strconv.FormatInt(v.i.Get(), 10)
}

// Duration exports a time.Duration
type Duration struct {
	i sync2.AtomicDuration
}

func NewDuration(name string) *Duration {
	v := new(Duration)
	Publish(name, v)
	return v
}

func (v *Duration) Add(delta time.Duration) {
	v.i.Add(delta)
}

func (v *Duration) Set(value time.Duration) {
	v.i.Set(value)
}

func (v *Duration) Get() time.Duration {
	return v.i.Get()
}

func (v *Duration) String() string {
	return strconv.FormatInt(int64(v.i.Get()), 10)
}

// IntFunc converts a function that returns
// an int64 as an expvar.
type IntFunc func() int64

func (f IntFunc) String() string {
	return strconv.FormatInt(f(), 10)
}

// DurationFunc converts a function that returns
// an time.Duration as an expvar.
type DurationFunc func() time.Duration

func (f DurationFunc) String() string {
	return strconv.FormatInt(int64(f()), 10)
}

// String is expvar.String+Get+hook
type String struct {
	mu sync.Mutex
	s  string
}

func NewString(name string) *String {
	v := new(String)
	Publish(name, v)
	return v
}

func (v *String) Set(value string) {
	v.mu.Lock()
	v.s = value
	v.mu.Unlock()
}

func (v *String) Get() string {
	v.mu.Lock()
	s := v.s
	v.mu.Unlock()
	return s
}

func (v *String) String() string {
	return strconv.Quote(v.Get())
}

// StringFunc converts a function that returns
// an string as an expvar.
type StringFunc func() string

func (f StringFunc) String() string {
	return strconv.Quote(f())
}

type jsonFunc func() string

func (f jsonFunc) String() string {
	return f()
}

// PublishJSONFunc publishes any function that returns
// a JSON string as a variable. The string is sent to
// expvar as is.
func PublishJSONFunc(name string, f func() string) {
	Publish(name, jsonFunc(f))
}
