package trace

import (
	"context"
	"io"

	"github.com/opentracing/opentracing-go"
	"go.opentelemetry.io/otel"
	otelBridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
)

func init() {
	tracingBackendFactories["opentelemetry"] = newOpenTelemetryFromEnv
}

type tracerProviderClose struct {
	*trace.TracerProvider
}

func (t tracerProviderClose) Close() error {
	return t.Shutdown(context.Background())
}

// newOpenTelemetryFromEnv creates an OTLP GRPC exporter configured by the
// standard OpenTelemetry SDK environment variables. It wraps it with the
// opentelemetry-opentracing bridge, and registers that as the default otel
// tracer provider.
// See upstream documentation for environment variables:
// https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
func newOpenTelemetryFromEnv(serviceName string) (tracingService, io.Closer, error) {
	exp, err := otlptracegrpc.New(context.Background())
	if err != nil {
		return nil, nil, err
	}

	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName))

	tracerProvider := trace.NewTracerProvider(trace.WithBatcher(exp), trace.WithResource(resources))
	bridgeTracer, wrapped := otelBridge.NewTracerPair(tracerProvider.Tracer("vitesss.io/vitess/go/trace"))
	otel.SetTracerProvider(wrapped)
	return openTracingService{Tracer: &otelTracer{bridgeTracer}}, tracerProviderClose{tracerProvider}, nil
}

type otelTracer struct {
	actual opentracing.Tracer
}

func (ot *otelTracer) GetOpenTracingTracer() opentracing.Tracer {
	return ot.actual
}
