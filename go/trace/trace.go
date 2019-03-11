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

// Package trace contains a helper interface that allows various tracing
// tools to be plugged in to components using this interface. If no plugin is
// registered, the default one makes all trace calls into no-ops.
package trace

import (
	"golang.org/x/net/context"
)

// Span represents a unit of work within a trace. After creating a Span with
// NewSpan(), call one of the Start methods to mark the beginning of the work
// represented by this Span. Call Finish() when that work is done to record the
// Span. A Span may be reused by calling Start again.
type Span interface {
	Finish()
	// Annotate records a key/value pair associated with a Span. It should be
	// called between Start and Finish.
	Annotate(key string, value interface{})
}

type SpanType int

const (
  Local SpanType = iota
  Client
  Server
)

// NewSpan creates a new Span with the currently installed tracing plugin.
// If no tracing plugin is installed, it returns a fake Span that does nothing.
func NewSpan(inCtx context.Context, label string, spanType SpanType) (Span, context.Context) {
  parent, _ := spanFactory.FromContext(inCtx)
  span := spanFactory.New(parent, label, spanType)
  outCtx := spanFactory.NewContext(inCtx, span)

  return span, outCtx
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

// CopySpan creates a new context from parentCtx, with only the trace span
// copied over from spanCtx, if it has any. If not, parentCtx is returned.
func CopySpan(parentCtx, spanCtx context.Context) context.Context {
	if span, ok := FromContext(spanCtx); ok {
		return NewContext(parentCtx, span)
	}
	return parentCtx
}

// SpanFactory is an interface for creating spans or extracting them from Contexts.
type SpanFactory interface {
  New(parent Span, label string, spanType SpanType) Span
  FromContext(ctx context.Context) (Span, bool)
  NewContext(parent context.Context, span Span) context.Context
}

// RegisterSpanFactory should be called by a plugin during init() to install a
// factory that creates Spans for that plugin's tracing framework. Each call to
// RegisterSpanFactory will overwrite any previous setting. If no factory is
// registered, the default fake factory will produce Spans whose methods are all
// no-ops.
func RegisterSpanFactory(sf SpanFactory) {
	spanFactory = sf
}

var spanFactory SpanFactory = fakeSpanFactory{}

type fakeSpanFactory struct{}

func (fakeSpanFactory) New(Span, string, SpanType) Span                           { return fakeSpan{} }
func (fakeSpanFactory) FromContext(context.Context) (Span, bool)                  { return nil, false }
func (fakeSpanFactory) NewContext(parent context.Context, _ Span) context.Context { return parent }

// fakeSpan implements Span with no-op methods.
type fakeSpan struct{}

func (fakeSpan) Finish()                      {}
func (fakeSpan) Annotate(string, interface{}) {}
