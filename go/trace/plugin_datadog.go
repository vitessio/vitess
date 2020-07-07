package trace

import (
	"flag"
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
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
		ddtracer.WithAgentAddr(*dataDogHost+":"+*dataDogPort),
		ddtracer.WithServiceName(serviceName),
		ddtracer.WithDebugMode(true),
		ddtracer.WithSampler(ddtracer.NewRateSampler(*samplingRate)),
	)

	opentracing.SetGlobalTracer(t)

	return openTracingService{Tracer: &datadogTracer{actual: t}}, &ddCloser{}, nil
}

var _ io.Closer = (*ddCloser)(nil)

type ddCloser struct{}

func (ddCloser) Close() error {
	ddtracer.Stop()
	return nil
}

func init() {
	tracingBackendFactories["opentracing-datadog"] = newDatadogTracer
}

var _ tracer = (*datadogTracer)(nil)

type datadogTracer struct {
	actual opentracing.Tracer
}

func (dt *datadogTracer) GetOpenTracingTracer() opentracing.Tracer {
	return dt.actual
}
