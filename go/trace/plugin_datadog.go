package trace

import (
	"fmt"
	"io"
	"net"

	"github.com/opentracing/opentracing-go"
	"github.com/spf13/pflag"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"vitess.io/vitess/go/viperutil"
)

var (
	dataDogConfigKey = viperutil.KeyPrefixFunc(configKey("datadog"))

	dataDogHost = viperutil.Configure(
		dataDogConfigKey("agent.host"),
		viperutil.Options[string]{
			FlagName: "datadog-agent-host",
		},
	)
	dataDogPort = viperutil.Configure(
		dataDogConfigKey("agent.port"),
		viperutil.Options[string]{
			FlagName: "datadog-agent-port",
		},
	)
	dataDogTraceDebugMode = viperutil.Configure(
		dataDogConfigKey("trace_debug_mode"),
		viperutil.Options[bool]{
			FlagName: "datadog-trace-debug-mode",
			Default:  false,
		},
	)
)

func init() {
	// If compiled with plugin_datadaog, ensure that trace.RegisterFlags
	// includes datadaog tracing flags.
	pluginFlags = append(pluginFlags, func(fs *pflag.FlagSet) {
		fs.String("datadog-agent-host", "", "host to send spans to. if empty, no tracing will be done")
		fs.String("datadog-agent-port", "", "port to send spans to. if empty, no tracing will be done")
		fs.Bool("datadog-trace-debug-mode", false, "enable debug mode for datadog tracing")

		viperutil.BindFlags(fs, dataDogHost, dataDogPort, dataDogTraceDebugMode)
	})
}

func newDatadogTracer(serviceName string) (tracingService, io.Closer, error) {
	host, port := dataDogHost.Get(), dataDogPort.Get()
	if host == "" || port == "" {
		return nil, nil, fmt.Errorf("need host and port to datadog agent to use datadog tracing")
	}

	opts := []ddtracer.StartOption{
		ddtracer.WithAgentAddr(net.JoinHostPort(host, port)),
		ddtracer.WithServiceName(serviceName),
		ddtracer.WithDebugMode(dataDogTraceDebugMode.Get()),
		ddtracer.WithSampler(ddtracer.NewRateSampler(samplingRate.Get())),
	}

	if enableLogging.Get() {
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
