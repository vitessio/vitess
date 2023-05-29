/*
Copyright 2019 The Vitess Authors.

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
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/spf13/pflag"
	"github.com/uber/jaeger-client-go/config"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/log"
)

/*
This file makes it easy to build Vitess without including the Jaeger binaries.
All that is needed is to delete this file. OpenTracing binaries will still be
included but nothing Jaeger specific.
*/

var (
	jaegerConfigKey = viperutil.KeyPrefixFunc(configKey("jaeger"))
	agentHost       = viperutil.Configure(
		jaegerConfigKey("agent-host"),
		viperutil.Options[string]{
			FlagName: "jaeger-agent-host",
		},
	)
	samplingType = viperutil.Configure(
		jaegerConfigKey("sampling_type"),
		viperutil.Options[string]{
			Default:  "const",
			EnvVars:  []string{"JAEGER_SAMPLER_TYPE"},
			FlagName: "tracing-sampling-type",
		},
	)
	samplingRate = viperutil.Configure(
		jaegerConfigKey("sampling_rate"),
		viperutil.Options[float64]{
			Default:  0.1,
			EnvVars:  []string{"JAEGER_SAMPLER_PARAM"},
			FlagName: "tracing-sampling-rate",
		},
	)
)

func init() {
	// If compiled with plugin_jaeger, ensure that trace.RegisterFlags includes
	// jaeger tracing flags.
	pluginFlags = append(pluginFlags, func(fs *pflag.FlagSet) {
		fs.String("jaeger-agent-host", agentHost.Default(), "host and port to send spans to. if empty, no tracing will be done")
		fs.String("tracing-sampling-type", samplingType.Default(), "sampling strategy to use for jaeger. possible values are 'const', 'probabilistic', 'rateLimiting', or 'remote'")
		fs.Float64("tracing-sampling-rate", samplingRate.Default(), "sampling rate for the probabilistic jaeger sampler")

		viperutil.BindFlags(fs, agentHost, samplingRate, samplingType)
	})
}

// newJagerTracerFromEnv will instantiate a tracingService implemented by Jaeger,
// taking configuration from environment variables. Available properties are:
// JAEGER_SERVICE_NAME -- If this is set, the service name used in code will be ignored and this value used instead
// JAEGER_RPC_METRICS
// JAEGER_TAGS
// JAEGER_SAMPLER_TYPE
// JAEGER_SAMPLER_PARAM
// JAEGER_SAMPLER_MANAGER_HOST_PORT
// JAEGER_SAMPLER_MAX_OPERATIONS
// JAEGER_SAMPLER_REFRESH_INTERVAL
// JAEGER_REPORTER_MAX_QUEUE_SIZE
// JAEGER_REPORTER_FLUSH_INTERVAL
// JAEGER_REPORTER_LOG_SPANS
// JAEGER_ENDPOINT
// JAEGER_USER
// JAEGER_PASSWORD
// JAEGER_AGENT_HOST
// JAEGER_AGENT_PORT
func newJagerTracerFromEnv(serviceName string) (tracingService, io.Closer, error) {
	cfg, err := config.FromEnv()
	if err != nil {
		return nil, nil, err
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = serviceName
	}

	// Allow command line args to override environment variables.
	if host := agentHost.Get(); host != "" {
		cfg.Reporter.LocalAgentHostPort = host
	}
	log.Infof("Tracing to: %v as %v", cfg.Reporter.LocalAgentHostPort, cfg.ServiceName)

	cfg.Sampler.Param = samplingRate.Get()
	cfg.Sampler.Type = samplingType.Get()
	log.Infof("Tracing sampler type %v (param: %v)", cfg.Sampler.Type, cfg.Sampler.Param)

	var opts []config.Option
	if enableLogging.Get() {
		opts = append(opts, config.Logger(&traceLogger{}))
	} else if cfg.Reporter.LogSpans {
		log.Warningf("JAEGER_REPORTER_LOG_SPANS was set, but --tracing-enable-logging was not; spans will not be logged")
	}

	tracer, closer, err := cfg.NewTracer(opts...)

	if err != nil {
		return nil, &nilCloser{}, err
	}

	opentracing.SetGlobalTracer(tracer)

	return openTracingService{Tracer: &jaegerTracer{actual: tracer}}, closer, nil
}

func init() {
	tracingBackendFactories["opentracing-jaeger"] = newJagerTracerFromEnv
}

var _ tracer = (*jaegerTracer)(nil)

type jaegerTracer struct {
	actual opentracing.Tracer
}

func (jt *jaegerTracer) GetOpenTracingTracer() opentracing.Tracer {
	return jt.actual
}
