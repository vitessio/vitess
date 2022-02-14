package trace

import "vitess.io/vitess/go/vt/log"

// traceLogger wraps the standard vitess log package to satisfy the datadog and
// jaeger logger interfaces.
type traceLogger struct{}

// Log is part of the ddtrace.Logger interface. Datadog only ever logs errors.
func (*traceLogger) Log(msg string) { log.Errorf(msg) }

// Error is part of the jaeger.Logger interface.
func (*traceLogger) Error(msg string) { log.Errorf(msg) }

// Infof is part of the jaeger.Logger interface.
func (*traceLogger) Infof(msg string, args ...interface{}) { log.Infof(msg, args...) }
