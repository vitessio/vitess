package trace

import (
	"flag"
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var (
	dataDogHost = flag.String("datadog-agent-host", "", "host to send spans to. if empty, no tracing will be done")
	dataDogPort = flag.String("datadog-agent-port", "", "port to send spans to. if empty, no tracing will be done")
)

func newDatadogTracer(serviceName string) (tracingService, io.Closer, error) {
	if *dataDogHost == "" || *dataDogPort == "" {
		return nil, nil, fmt.Errorf("need host and port to datadog agent to use datadog tracing")
	}

	t := opentracer.New(
		tracer.WithAgentAddr(*dataDogHost+":"+*dataDogPort),
		tracer.WithServiceName(serviceName),
		tracer.WithDebugMode(true),
		tracer.WithSampler(tracer.NewRateSampler(*samplingRate)),
	)
 	opentracing.SetGlobalTracer(t)

	return openTracingService{Tracer: t}, &nilCloser{}, nil
}

func init() {
	tracingBackendFactories["opentracing-datadog"] = newDatadogTracer
}
