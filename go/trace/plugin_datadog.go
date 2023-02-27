package trace

import (
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/spf13/pflag"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var (
	dataDogHost string
	dataDogPort string
)

func init() {
	// If compiled with plugin_datadaog, ensure that trace.RegisterFlags
	// includes datadaog tracing flags.
	pluginFlags = append(pluginFlags, func(fs *pflag.FlagSet) {
		fs.StringVar(&dataDogHost, "datadog-agent-host", "", "host to send spans to. if empty, no tracing will be done")
		fs.StringVar(&dataDogPort, "datadog-agent-port", "", "port to send spans to. if empty, no tracing will be done")
	})
}

func newDatadogTracer(serviceName string) (tracingService, io.Closer, error) {
	if dataDogHost == "" || dataDogPort == "" {
		return nil, nil, fmt.Errorf("need host and port to datadog agent to use datadog tracing")
	}

	opts := []ddtracer.StartOption{
		ddtracer.WithAgentAddr(dataDogHost + ":" + dataDogPort),
		ddtracer.WithServiceName(serviceName),
		ddtracer.WithDebugMode(true),
		ddtracer.WithSampler(ddtracer.NewRateSampler(samplingRate.Get())),
	}

	if enableLogging {
		opts = append(opts, ddtracer.WithLogger(&traceLogger{}))
	}

	t := opentracer.New(opts...)

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
