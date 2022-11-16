/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/log"
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
	Annotate(key string, value any)
}

// NewSpan creates a new Span with the currently installed tracing plugin.
// If no tracing plugin is installed, it returns a fake Span that does nothing.
func NewSpan(inCtx context.Context, label string) (Span, context.Context) {
	parent, _ := currentTracer.FromContext(inCtx)
	span := currentTracer.New(parent, label)
	outCtx := currentTracer.NewContext(inCtx, span)

	return span, outCtx
}

// NewFromString creates a new Span with the currently installed tracing plugin, extracting the span context from
// the provided string.
func NewFromString(inCtx context.Context, parent, label string) (Span, context.Context, error) {
	span, err := currentTracer.NewFromString(parent, label)
	if err != nil {
		return nil, nil, err
	}
	outCtx := currentTracer.NewContext(inCtx, span)
	return span, outCtx, nil
}

// AnnotateSQL annotates information about a sql query in the span. This is done in a way
// so as to not leak personally identifying information (PII), or sensitive personal information (SPI)
func AnnotateSQL(span Span, strippedSQL fmt.Stringer) {
	span.Annotate("sql-statement-type", strippedSQL.String())
}

// FromContext returns the Span from a Context if present. The bool return
// value indicates whether a Span was present in the Context.
func FromContext(ctx context.Context) (Span, bool) {
	return currentTracer.FromContext(ctx)
}

// NewContext returns a context based on parent with a new Span value.
func NewContext(parent context.Context, span Span) context.Context {
	return currentTracer.NewContext(parent, span)
}

// CopySpan creates a new context from parentCtx, with only the trace span
// copied over from spanCtx, if it has any. If not, parentCtx is returned.
func CopySpan(parentCtx, spanCtx context.Context) context.Context {
	if span, ok := FromContext(spanCtx); ok {
		return NewContext(parentCtx, span)
	}
	return parentCtx
}

// AddGrpcServerOptions adds GRPC interceptors that read the parent span from the grpc packets
func AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor)) {
	currentTracer.AddGrpcServerOptions(addInterceptors)
}

// AddGrpcClientOptions adds GRPC interceptors that add parent information to outgoing grpc packets
func AddGrpcClientOptions(addInterceptors func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor)) {
	currentTracer.AddGrpcClientOptions(addInterceptors)
}

// tracingService is an interface for creating spans or extracting them from Contexts.
type tracingService interface {
	// New creates a new span from an existing one, if provided. The parent can also be nil
	New(parent Span, label string) Span

	// NewFromString creates a new span and uses the provided string to reconstitute the parent span
	NewFromString(parent, label string) (Span, error)

	// FromContext extracts a span from a context, making it possible to annotate the span with additional information
	FromContext(ctx context.Context) (Span, bool)

	// NewContext creates a new context containing the provided span
	NewContext(parent context.Context, span Span) context.Context

	// AddGrpcServerOptions allows a tracing system to add interceptors to grpc server traffic
	AddGrpcServerOptions(addInterceptors func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor))

	// AddGrpcClientOptions allows a tracing system to add interceptors to grpc server traffic
	AddGrpcClientOptions(addInterceptors func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor))
}

// TracerFactory creates a tracing service for the service provided. It's important to close the provided io.Closer
// object to make sure that all spans are sent to the backend before the process exits.
type TracerFactory func(serviceName string) (tracingService, io.Closer, error)

const configKeyPrefix = "trace"

var (
	// tracingBackendFactories should be added to by a plugin during init() to install itself
	tracingBackendFactories = make(map[string]TracerFactory)

	currentTracer tracingService = noopTracingServer{}

	/* flags */

	configKey = viperutil.KeyPrefixFunc(configKeyPrefix)

	tracingServer = viperutil.Configure(
		configKey("service"),
		viperutil.Options[string]{
			Default:  "noop",
			FlagName: "tracer",
		},
	)
	enableLogging = viperutil.Configure(
		configKey("enable-logging"),
		viperutil.Options[bool]{
			FlagName: "tracing-enable-logging",
		},
	)

	pluginFlags []func(fs *pflag.FlagSet)
)

func RegisterFlags(fs *pflag.FlagSet) {
	fs.String("tracer", tracingServer.Default(), "tracing service to use")
	fs.Bool("tracing-enable-logging", false, "whether to enable logging in the tracing service")

	viperutil.BindFlags(fs, tracingServer, enableLogging)

	for _, fn := range pluginFlags {
		fn(fs)
	}
}

// StartTracing enables tracing for a named service
func StartTracing(serviceName string) io.Closer {
	tracingBackend := tracingServer.Get()
	factory, ok := tracingBackendFactories[tracingBackend]
	if !ok {
		return fail(serviceName)
	}

	tracer, closer, err := factory(serviceName)
	if err != nil {
		log.Error(vterrors.Wrapf(err, "failed to create a %s tracer", tracingBackend))
		return &nilCloser{}
	}

	currentTracer = tracer
	if tracingBackend != "noop" {
		log.Infof("successfully started tracing with [%s]", tracingBackend)
	}

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

type nilCloser struct {
}

func (c *nilCloser) Close() error { return nil }
