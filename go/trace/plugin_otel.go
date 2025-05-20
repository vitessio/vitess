/*
Copyright 2025 The Vitess Authors.

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

package trace

import (
	"context"
	"io"
	"time"

	"github.com/opentracing/opentracing-go"

	"go.opentelemetry.io/otel"
	otelBridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

/*
This file provides OpenTelemetry integration for Vitess tracing.
*/

// newOtelTracer will instantiate a tracingService implemented by OpenTelemetry,
// taking configuration primarily from the environment variables.
func newOtelTracer(serviceName string) (tracingService, io.Closer, error) {
	// Create resource with service information
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)

	if err != nil {
		return nil, &nilCloser{}, err
	}

	// Configure the OTLP exporter
	ctx := context.Background()

	// TODO: Add support for specifying other protocols via a flag
	client := otlptracehttp.NewClient()
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, &nilCloser{}, err
	}

	// Create a new OpenTelemetry TracerProvider with the OTLP exporter
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
	)

	// Set the global trace provider and propagator
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Create OpenTelemetry tracer
	otelTracer := otel.Tracer(serviceName)

	// Create a Bridge tracer that supports both OpenTracing and OpenTelemetry
	bridgeTracer, _ := otelBridge.NewTracerPair(otelTracer)

	// Set the global OpenTracing tracer
	opentracing.SetGlobalTracer(bridgeTracer)

	// Return the tracing service
	traceSvc := openTracingService{Tracer: &otelBridgeTracer{bridgeTracer: bridgeTracer}}

	// Create a closer to clean up resources
	closer := &otelCloser{provider: tp}

	return traceSvc, closer, nil
}

func init() {
	tracingBackendFactories["opentracing-otel"] = newOtelTracer
}

// otelCloser implements io.Closer for OpenTelemetry cleanup
type otelCloser struct {
	provider *sdktrace.TracerProvider
}

func (c *otelCloser) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.provider.Shutdown(ctx)
}

var _ tracer = (*otelBridgeTracer)(nil)

type otelBridgeTracer struct {
	bridgeTracer *otelBridge.BridgeTracer
}

func (obt *otelBridgeTracer) GetOpenTracingTracer() opentracing.Tracer {
	return obt.bridgeTracer
}
