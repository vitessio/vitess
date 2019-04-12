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
	"flag"
	"io"
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
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

// NewSpan creates a new Span with the currently installed tracing plugin.
// If no tracing plugin is installed, it returns a fake Span that does nothing.
func NewSpan(inCtx context.Context, label string) (Span, context.Context) {
	parent, _ := spanFactory.FromContext(inCtx)
	span := spanFactory.New(parent, label)
	outCtx := spanFactory.NewContext(inCtx, span)

	return span, outCtx
}

// AnnotateSQL annotates information about a sql query in the span. This is done in a way
// so as to not leak personally identifying information (PII), or sensitive personal information (SPI)
func AnnotateSQL(span Span, sql string) {
	span.Annotate("sql-statement-type", sqlparser.StmtType(sqlparser.Preview(sql)))
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

func GetGrpcClientOptions() []grpc.DialOption {
	return spanFactory.GetGrpcClientOptions()
}

func GetGrpcServerOptions() []grpc.ServerOption {
	return spanFactory.GetGrpcServerOptions()
}

func AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	spanFactory.AddGrpcServerOptions(addInterceptors)
}

// TracingService is an interface for creating spans or extracting them from Contexts.
type TracingService interface {
	// New creates a new span from an existing one, if provided. The parent can also be nil
	New(parent Span, label string) Span

	// Extracts a span from a context, making it possible to annotate the span with additional information.
	FromContext(ctx context.Context) (Span, bool)

	// Creates a new context containing the provided span
	NewContext(parent context.Context, span Span) context.Context

	// Allows a tracing system to add interceptors to grpc server traffic
	AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor))

	// Allows a tracing system to add grpc configuration to server traffic
	GetGrpcServerOptions() []grpc.ServerOption

	// Allows a tracing system to add grpc configuration to client traffic
	GetGrpcClientOptions() []grpc.DialOption
}

type TracerFactory func(serviceName string) (TracingService, io.Closer, error)

// tracingBackendFactories should be added to by a plugin during init() to install itself
var tracingBackendFactories = make(map[string]TracerFactory)

var spanFactory TracingService = fakeSpanFactory{}

var (
	tracingServer = flag.String("tracer", "noop", "tracing service to use")
)

// StartTracing enables tracing for a named service
func StartTracing(serviceName string) io.Closer {
	factory, ok := tracingBackendFactories[*tracingServer]
	if !ok {
		return fail(serviceName)
	}

	tracer, closer, err := factory(serviceName)
	if err != nil {
		log.Error(vterrors.Wrapf(err, "failed to create a %s tracer", *tracingServer))
		return &nilCloser{}
	}

	spanFactory = tracer

	return closer
}

func fail(serviceName string) io.Closer {
	options := make([]string, len(tracingBackendFactories))
	for k := range tracingBackendFactories {
		options = append(options, k)
	}
	altStr := strings.Join(options, ", ")
	log.Errorf("no such [%s] tracing service found. alternatives are: %v", serviceName, altStr)
	return &nilCloser{}
}
