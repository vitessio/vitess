package trace

import (
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentracer"
	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"vitess.io/vitess/go/viperutil"
)

var (
	dataDogConfigKey = viperutil.KeyPartial(configKey("datadog"))

	dataDogHost = viperutil.NewValue(
		dataDogConfigKey("agent.host"),
		viper.GetString,
		viperutil.WithFlags[string]("datadog-agent-host"),
	)
	dataDogPort = viperutil.NewValue(
		dataDogConfigKey("agent.port"),
		viper.GetString,
		viperutil.WithFlags[string]("datadog-agent-port"),
	)
)

func init() {
	// If compiled with plugin_datadaog, ensure that trace.RegisterFlags
	// includes datadaog tracing flags.
	pluginFlags = append(pluginFlags, func(fs *pflag.FlagSet) {
		fs.String("datadog-agent-host", "", "host to send spans to. if empty, no tracing will be done")
		dataDogHost.Bind(nil, fs)

		fs.String("datadog-agent-port", "", "port to send spans to. if empty, no tracing will be done")
		dataDogPort.Bind(nil, fs)
	})
}

func newDatadogTracer(serviceName string) (tracingService, io.Closer, error) {
	host, port := dataDogHost.Get(), dataDogPort.Get()
	if host == "" || port == "" {
		return nil, nil, fmt.Errorf("need host and port to datadog agent to use datadog tracing")
	}

	opts := []ddtracer.StartOption{
		ddtracer.WithAgentAddr(host + ":" + port),
		ddtracer.WithServiceName(serviceName),
		ddtracer.WithDebugMode(true),
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
