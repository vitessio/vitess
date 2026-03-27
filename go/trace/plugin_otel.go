/*
Copyright 2026 The Vitess Authors.

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
	"fmt"
	"io"
	"time"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/log"
)

var (
	otelConfigKey = viperutil.KeyPrefixFunc(configKey("otel"))

	otelEndpoint = viperutil.Configure(
		otelConfigKey("endpoint"),
		viperutil.Options[string]{
			FlagName: "otel-endpoint",
		},
	)
	otelInsecure = viperutil.Configure(
		otelConfigKey("insecure"),
		viperutil.Options[bool]{
			Default:  false,
			FlagName: "otel-insecure",
		},
	)
)

func init() {
	pluginFlags = append(pluginFlags, func(fs *pflag.FlagSet) {
		fs.String("otel-endpoint", "", "OpenTelemetry collector endpoint (host:port for gRPC). If empty, the OTEL_EXPORTER_OTLP_ENDPOINT env var is used; this must be a URL including scheme, e.g. http://collector:4317 or https://collector:4317")
		fs.Bool("otel-insecure", otelInsecure.Default(), "use insecure connection to OpenTelemetry collector")

		viperutil.BindFlags(fs, otelEndpoint, otelInsecure)
	})

	tracingBackendFactories["opentelemetry"] = newOTelTracer
}

func newOTelTracer(serviceName string) (tracingService, io.Closer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var opts []otlptracegrpc.Option
	if endpoint := otelEndpoint.Get(); endpoint != "" {
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
	}
	if otelInsecure.Get() {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptrace.New(ctx, otlptracegrpc.NewClient(opts...))
	if err != nil {
		return nil, &nilCloser{}, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, &nilCloser{}, fmt.Errorf("failed to create resource: %w", err)
	}

	rate := samplingRate.Get()
	if rate < 0.0 {
		log.Warn(fmt.Sprintf("tracing sampling rate %f is below 0.0; clamping to 0.0", rate))
		rate = 0.0
	} else if rate > 1.0 {
		log.Warn(fmt.Sprintf("tracing sampling rate %f is above 1.0; clamping to 1.0", rate))
		rate = 1.0
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(rate))),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if enableLogging.Get() {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
			log.Error(fmt.Sprintf("OpenTelemetry error: %v", err))
		}))
	}

	endpoint := otelEndpoint.Get()
	if endpoint == "" {
		endpoint = "(OTEL_EXPORTER_OTLP_ENDPOINT or default localhost:4317)"
	}
	log.Info(fmt.Sprintf("OpenTelemetry tracing enabled for %s, exporting to %s", serviceName, endpoint))

	tracer := tp.Tracer("vitess.io/vitess")

	return &otelTracingService{Tracer: tracer}, &otelCloser{tp: tp}, nil
}

type otelCloser struct {
	tp *sdktrace.TracerProvider
}

func (c *otelCloser) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.tp.Shutdown(ctx)
}
