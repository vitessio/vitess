package context

import (
	"html/template"
	"time"

	gcontext "code.google.com/p/go.net/context"
)

// Context represents the context for SqlQuery RPC calls.
type Context interface {
	// GetRemoteAddr returns the client address
	GetRemoteAddr() string
	// GetUsername returns the username for the request
	GetUsername() string
	// HTML returns an HTML representation of this context
	HTML() template.HTML
	// String returns a string representation of this Context
	String() string

	// Deadline returns the time when work on behalf of this context should be cancelled. ok
	// will be false if there is no deadline set.
	Deadline() (deadline time.Time, ok bool)

	Done() <-chan struct{}

	Err() error

	Value(key interface{}) interface{}
}

// DummyContext is a dummy implementation of Context.
type DummyContext struct{}

func (dc *DummyContext) GetRemoteAddr() string                   { return "DummyRemoteAddr" }
func (dc *DummyContext) GetUsername() string                     { return "DummyUsername" }
func (dc *DummyContext) HTML() template.HTML                     { return template.HTML("DummyContext") }
func (dc *DummyContext) String() string                          { return "DummyContext" }
func (dc *DummyContext) Deadline() (deadline time.Time, ok bool) { return time.Time{}, false }
func (dc *DummyContext) Done() <-chan struct{}                   { return nil }
func (dc *DummyContext) Err() error                              { return nil }
func (dc *DummyContext) Value(key interface{}) interface{}       { return nil }

var _ gcontext.Context = Context(nil) // compile-time interface check.
