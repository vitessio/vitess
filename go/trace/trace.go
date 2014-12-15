// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package trace contains a helper interface that allows various tracing
// tools to be plugged in to components using this interface.
package trace

import (
	"golang.org/x/net/context"
)

// Span represents a unit of work within a trace. After creating a Span with
// NewSpan(), call one of the Start methods to mark the beginning of the work
// represented by this Span. Call Finish() when that work is done to record the
// Span. A Span may be reused by calling Start again.
type Span interface {
	// StartLocal marks the beginning of a span representing time spent doing
	// work locally.
	StartLocal(label string)
	// StartClient marks the beginning of a span representing time spent acting as
	// a client and waiting for a response.
	StartClient(label string)
	// StartServer marks the beginning of a span representing time spent doing
	// work in service of a remote client request.
	StartServer(label string)
	// Finish marks the span as complete.
	Finish()
	// Annotate records a key/value pair associated with a Span. It should be
	// called between Start and Finish.
	Annotate(key string, value interface{})
}

// NewSpan creates a new Span with the currently installed tracing plugin.
// If no tracing plugin is installed, it returns a fake Span that does nothing.
func NewSpan(parent Span) Span {
	return spanFactory.New(parent)
}

// FromContext returns the Span from a Context if present. The bool return
// value indicates whether a Span was present in the Context.
func FromContext(ctx context.Context) (Span, bool) {
	return spanFactory.FromContext(ctx)
}

// NewContext returns a context based on parent with a new Span value.
func NewContext(parent context.Context, span Span) context.Context {
	return spanFactory.NewContext(parent, span)
}

// NewSpanFromContext returns a new Span whose parent is the Span from the given
// Context if present, or a new Span with no parent if not.
func NewSpanFromContext(ctx context.Context) Span {
	if parent, ok := FromContext(ctx); ok {
		return NewSpan(parent)
	}
	return NewSpan(nil)
}

// spanFactory should be changed by a plugin during init() to a factory that
// creates an actual Span implementation for that plugin's tracing framework.
var spanFactory interface {
	New(parent Span) Span
	FromContext(ctx context.Context) (Span, bool)
	NewContext(parent context.Context, span Span) context.Context
} = fakeSpanFactory{}

type fakeSpanFactory struct{}

func (fakeSpanFactory) New(parent Span) Span                                         { return fakeSpan{} }
func (fakeSpanFactory) FromContext(ctx context.Context) (Span, bool)                 { return nil, false }
func (fakeSpanFactory) NewContext(parent context.Context, span Span) context.Context { return parent }

// fakeSpan implements Span with no-op methods.
type fakeSpan struct{}

func (fakeSpan) StartLocal(string)            {}
func (fakeSpan) StartClient(string)           {}
func (fakeSpan) StartServer(string)           {}
func (fakeSpan) Finish()                      {}
func (fakeSpan) Annotate(string, interface{}) {}
