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

	"github.com/youtube/vitess/go/sync2"
)

type NewVarHook func(name string, v expvar.Var)

var newVarHook NewVarHook

// Register allows you to register a callback function
// that will be called whenever a new stats variable gets
// created. This can be used to build alternate methods
// of exporting stats variables.
func Register(nvh NewVarHook) {
	newVarHook = nvh
}

// Publish is expvar.Publish+hook
func Publish(name string, v expvar.Var) {
	expvar.Publish(name, v)
	if newVarHook != nil {
		newVarHook(name, v)
	}
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

type strFunc func() string

func (f strFunc) String() string {
	return f()
}

// PublishFunc publishes any function that returns
// a JSON string as a variable.
func PublishFunc(name string, f func() string) {
	Publish(name, strFunc(f))
}
