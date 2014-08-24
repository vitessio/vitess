package context

import "html/template"
import "time"

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
	// Deadline returns the deadline of this context
	Deadline() time.Time
}

// DummyContext is a dummy implementation of Context
type DummyContext struct{}

func (dc *DummyContext) GetRemoteAddr() string { return "DummyRemoteAddr" }
func (dc *DummyContext) GetUsername() string   { return "DummyUsername" }
func (dc *DummyContext) HTML() template.HTML   { return template.HTML("DummyContext") }
func (dc *DummyContext) String() string        { return "DummyContext" }
func (dc *DummyContext) Deadline() time.Time   { return time.Time{} }
