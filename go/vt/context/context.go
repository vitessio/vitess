package context

import (
	"html/template"
	"time"

	gcontext "code.google.com/p/go.net/context"
)

// Context represents the context for SqlQuery RPC calls.
type Context gcontext.Context

// DummyContext is a dummy implementation of Context.
type DummyContext struct {
}

func (dc *DummyContext) GetRemoteAddr() string {
	return "DummyRemoteAddr"
}
func (dc *DummyContext) GetUsername() string                     { return "DummyUsername" }
func (dc *DummyContext) HTML() template.HTML                     { return template.HTML("DummyContext") }
func (dc *DummyContext) String() string                          { return "DummyContext" }
func (dc *DummyContext) Deadline() (deadline time.Time, ok bool) { return time.Time{}, false }
func (dc *DummyContext) Done() <-chan struct{}                   { return nil }
func (dc *DummyContext) Err() error                              { return nil }
func (dc *DummyContext) Value(key interface{}) interface{}       { return nil }
