/*
Copyright 2020 The Vitess Authors.

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

package main

import (
	"flag"
	"os"

	"vitess.io/vitess/go/cmd/vtctldclient/internal/command"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
)

var globalFlags = []string{
	"alsologtostderr",
	"datadog-agent-host",
	"datadog-agent-port",
	"grpc_auth_static_client_creds",
	"grpc_compression",
	"grpc_enable_tracing",
	"grpc_initial_conn_window_size",
	"grpc_initial_window_size",
	"grpc_keepalive_time",
	"grpc_keepalive_timeout",
	"grpc_max_message_size",
	"grpc_prometheus",
	"jaeger-agent-host",
	"log_backtrace_at",
	"log_dir",
	"log_err_stacks",
	"log_rotate_max_size",
	"logtostderr",
	"pprof",
	"remote_operation_timeout",
	"stderrthreshold",
	"tracer",
	"tracing-sampling-rate",
	"v",
	"version",
	"vtctld_grpc_ca",
	"vtctld_grpc_cert",
	"vtctld_grpc_key",
	"vtctld_grpc_server_name",
}

func main() {
	defer exit.Recover()

	for _, flagName := range globalFlags {
		command.Root.PersistentFlags().AddGoFlag(flag.Lookup(flagName))
	}

	// hack to get rid of an "ERROR: logging before flag.Parse"
	flag.CommandLine = flag.NewFlagSet("", flag.ContinueOnError)
	args := os.Args[:]
	os.Args = os.Args[:1]
	flag.Parse()
	os.Args = args

	// back to your regularly scheduled cobra programming
	if err := command.Root.Execute(); err != nil {
		log.Error(err)
		exit.Return(1)
	}
}
