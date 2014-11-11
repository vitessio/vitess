package context

import "time"

// DummyContext is a dummy implementation of Context.
type DummyContext struct {
}

func (dc *DummyContext) Deadline() (deadline time.Time, ok bool) { return time.Time{}, false }
func (dc *DummyContext) Done() <-chan struct{}                   { return nil }
func (dc *DummyContext) Err() error                              { return nil }
func (dc *DummyContext) Value(key interface{}) interface{}       { return nil }
