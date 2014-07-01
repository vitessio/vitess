package tabletserver

// Context represents the context for SqlQuery RPC calls.
type Context interface {
	// GetRemoteAddr returns the client address
	GetRemoteAddr() string
	// GetUsername returns the username for the request
	GetUsername() string
	// String returns a string representation of this Context
	String() string
}

// DummyContext is a dummy implementation of Context
type DummyContext struct{}

func (dc *DummyContext) GetRemoteAddr() string { return "DummyRemoteAddr" }
func (dc *DummyContext) GetUsername() string   { return "DummyUsername" }
func (dc *DummyContext) String() string        { return "DummyContext" }
