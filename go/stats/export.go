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

func callHook(name string, v expvar.Var) {
	if newVarHook != nil {
		newVarHook(name, v)
	}
}

// NewFloat is same as expvar.NewFloat, except
// that it also calls the hook.
func NewFloat(name string) *expvar.Float {
	f := expvar.NewFloat(name)
	callHook(name, f)
	return f
}

// NewInt is same as expvar.NewInt, except
// that it also calls the hook.
func NewInt(name string) *expvar.Int {
	i := expvar.NewInt(name)
	callHook(name, i)
	return i
}

// NewMap is same as expvar.NewMap, except
// that it also calls the hook.
func NewMap(name string) *expvar.Map {
	m := expvar.NewMap(name)
	callHook(name, m)
	return m
}

// NewString is same as expvar.NewString, except
// that it also calls the hook.
func NewString(name string) *expvar.String {
	s := expvar.NewString(name)
	callHook(name, s)
	return s
}

// Publish is same as expvar.Publish, except
// that it also calls the hook.
func Publish(name string, v expvar.Var) {
	expvar.Publish(name, v)
	callHook(name, v)
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
