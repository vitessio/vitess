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
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/grpcserver"
	vtadminhttp "vitess.io/vitess/go/vt/vtadmin/http"
)

var (
	opts                 grpcserver.Options
	httpOpts             vtadminhttp.Options
	clusterConfigs       cluster.ClustersFlag
	clusterFileConfig    cluster.FileConfig
	defaultClusterConfig cluster.Config

	rootCmd = &cobra.Command{
		Use: "vtadmin",
		PreRun: func(cmd *cobra.Command, args []string) {
			tmp := os.Args
			os.Args = os.Args[0:1]
			flag.Parse()
			os.Args = tmp
			// (TODO:@amason) Check opts.EnableTracing and trace boot time.
		},
		Run: run,
	}
)

func run(cmd *cobra.Command, args []string) {
	configs := clusterFileConfig.Combine(defaultClusterConfig, clusterConfigs)
	clusters := make([]*cluster.Cluster, len(configs))

	if len(configs) == 0 {
		log.Fatal("must specify at least one cluster")
	}

	for i, cfg := range configs {
		cluster, err := cfg.Cluster()
		if err != nil {
			log.Fatal(err)
		}

		clusters[i] = cluster
	}

	s := vtadmin.NewAPI(clusters, opts, httpOpts)
	if err := s.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	rootCmd.Flags().StringVar(&opts.Addr, "addr", ":15000", "address to serve on")
	rootCmd.Flags().DurationVar(&opts.CMuxReadTimeout, "lmux-read-timeout", time.Second, "how long to spend connection muxing")
	rootCmd.Flags().DurationVar(&opts.LameDuckDuration, "lame-duck-duration", time.Second*5, "length of lame duck period at shutdown")
	rootCmd.Flags().Var(&clusterConfigs, "cluster", "per-cluster configuration. any values here take precedence over those in -cluster-defaults or -cluster-config")
	rootCmd.Flags().Var(&clusterFileConfig, "cluster-config", "path to a yaml cluster configuration. see clusters.example.yaml") // (TODO:@amason) provide example config.
	rootCmd.Flags().Var(&defaultClusterConfig, "cluster-defaults", "default options for all clusters")

	rootCmd.Flags().BoolVar(&opts.EnableTracing, "grpc-tracing", false, "whether to enable tracing on the gRPC server")
	rootCmd.Flags().BoolVar(&httpOpts.EnableTracing, "http-tracing", false, "whether to enable tracing on the HTTP server")
	rootCmd.Flags().BoolVar(&httpOpts.DisableCompression, "http-no-compress", false, "whether to disable compression of HTTP API responses")
	rootCmd.Flags().StringSliceVar(&httpOpts.CORSOrigins, "http-origin", []string{}, "repeated, comma-separated flag of allowed CORS origins. omit to disable CORS")

	// glog flags, no better way to do this
	rootCmd.Flags().AddGoFlag(flag.Lookup("v"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("logtostderr"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("alsologtostderr"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("stderrthreshold"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("log_dir"))

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}

	log.Flush()
}
