package trace

import (
	"flag"
	"fmt"
	"io"

	datadog "github.com/DataDog/dd-trace-go/opentracing"
	"github.com/opentracing/opentracing-go"
)

var (
	dataDogHost = flag.String("datadog-agent-host", "", "host to send spans to. if empty, no tracing will be done")
	dataDogPort = flag.String("datadog-agent-port", "", "port to send spans to. if empty, no tracing will be done")
)

func newDatadogTracer(serviceName string) (tracingService, io.Closer, error) {
	if *dataDogHost == "" || *dataDogPort == "" {
		return nil, nil, fmt.Errorf("need host and port to datadog agent to use datadog tracing")
	}

	config := datadog.NewConfiguration()
	config.ServiceName = serviceName
	config.AgentHostname = *dataDogHost
	config.AgentPort = *dataDogPort
	config.SampleRate = *samplingRate
	config.Debug = true

	tracer, closer, _ := datadog.NewTracer(config)

	opentracing.SetGlobalTracer(tracer)

	return openTracingService{tracer}, closer, nil
}

func init() {
	tracingBackendFactories["opentracing-datadog"] = newDatadogTracer
}
