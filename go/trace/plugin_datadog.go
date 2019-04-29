/*
Copyright 2017 Google Inc.

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

/*
This file makes it easy to build Vitess without including the Jaeger binaries.
All that is needed is to delete this file. OpenTracing binaries will still be
included but nothing Jaeger specific.
*/

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
