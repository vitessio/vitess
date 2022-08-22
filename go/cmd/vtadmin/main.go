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
	"context"
	"flag"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin"
	"vitess.io/vitess/go/vt/vtadmin/cache"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/grpcserver"
	vtadminhttp "vitess.io/vitess/go/vt/vtadmin/http"
	"vitess.io/vitess/go/vt/vtadmin/http/debug"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
)

var (
	opts                  grpcserver.Options
	httpOpts              vtadminhttp.Options
	clusterConfigs        cluster.ClustersFlag
	clusterFileConfig     cluster.FileConfig
	defaultClusterConfig  cluster.Config
	enableDynamicClusters bool

	rbacConfigPath string
	enableRBAC     bool
	disableRBAC    bool

	cacheRefreshKey string

	traceCloser io.Closer = &noopCloser{}

	rootCmd = &cobra.Command{
		Use: "vtadmin",
		PreRun: func(cmd *cobra.Command, args []string) {
			tmp := os.Args
			os.Args = os.Args[0:1]
			flag.Parse()
			os.Args = tmp

			if opts.EnableTracing || httpOpts.EnableTracing {
				startTracing(cmd)
			}
		},
		Run: run,
		PostRun: func(cmd *cobra.Command, args []string) {
			trace.LogErrorsWhenClosing(traceCloser)
		},
	}
)

// fatal ensures the tracer is closed and final spans are sent before issuing
// a log.Fatal call with the given args.
func fatal(args ...any) {
	trace.LogErrorsWhenClosing(traceCloser)
	log.Fatal(args...)
}

// startTracing checks the value of --tracer and then starts tracing, populating
// the private global traceCloser
func startTracing(cmd *cobra.Command) {
	tracer, err := cmd.Flags().GetString("tracer")
	if err != nil {
		log.Warningf("not starting tracer; err: %s", err)
		return
	}

	if tracer == "" || tracer == "noop" {
		log.Warningf("starting tracing with noop tracer")
	}

	traceCloser = trace.StartTracing("vtadmin")
}

func run(cmd *cobra.Command, args []string) {
	bootSpan, ctx := trace.NewSpan(context.Background(), "vtadmin.boot")
	defer bootSpan.Finish()

	configs := clusterFileConfig.Combine(defaultClusterConfig, clusterConfigs)
	clusters := make([]*cluster.Cluster, len(configs))

	if len(configs) == 0 && !enableDynamicClusters {
		bootSpan.Finish()
		fatal("must specify at least one cluster")
	}

	var rbacConfig *rbac.Config
	if disableRBAC {
		rbacConfig = rbac.DefaultConfig()
	} else if enableRBAC && rbacConfigPath != "" {
		cfg, err := rbac.LoadConfig(rbacConfigPath)
		if err != nil {
			fatal(err)
		}

		rbacConfig = cfg
	} else if enableRBAC && rbacConfigPath == "" {
		fatal("must pass --rbac-config path when enabling rbac")
	} else {
		fatal("must explicitly enable or disable RBAC by passing --no-rbac or --rbac")
	}

	for i, cfg := range configs {
		cluster, err := cfg.Cluster(ctx)
		if err != nil {
			bootSpan.Finish()
			fatal(err)
		}

		clusters[i] = cluster
	}

	if cacheRefreshKey == "" {
		log.Warningf("no cache-refresh-key set; forcing cache refreshes will not be possible")
	}
	cache.SetCacheRefreshKey(cacheRefreshKey)

	s := vtadmin.NewAPI(clusters, vtadmin.Options{
		GRPCOpts:              opts,
		HTTPOpts:              httpOpts,
		RBAC:                  rbacConfig,
		EnableDynamicClusters: enableDynamicClusters,
	})
	bootSpan.Finish()

	if err := s.ListenAndServe(); err != nil {
		fatal(err)
	}
}

func main() {
	// Common flags
	rootCmd.Flags().StringVar(&opts.Addr, "addr", ":15000", "address to serve on")
	rootCmd.Flags().DurationVar(&opts.CMuxReadTimeout, "lmux-read-timeout", time.Second, "how long to spend connection muxing")
	rootCmd.Flags().DurationVar(&opts.LameDuckDuration, "lame-duck-duration", time.Second*5, "length of lame duck period at shutdown")

	// Cluster config flags
	rootCmd.Flags().Var(&clusterConfigs, "cluster", "per-cluster configuration. any values here take precedence over those in -cluster-defaults or -cluster-config")
	rootCmd.Flags().Var(&clusterFileConfig, "cluster-config", "path to a yaml cluster configuration. see clusters.example.yaml") // (TODO:@amason) provide example config.
	rootCmd.Flags().Var(&defaultClusterConfig, "cluster-defaults", "default options for all clusters")
	rootCmd.Flags().BoolVar(&enableDynamicClusters, "enable-dynamic-clusters", false, "whether to enable dynamic clusters that are set by request header cookies or gRPC metadata")

	// Tracing flags
	trace.RegisterFlags(rootCmd.Flags()) // defined in go/vt/trace
	rootCmd.Flags().BoolVar(&opts.EnableTracing, "grpc-tracing", false, "whether to enable tracing on the gRPC server")
	rootCmd.Flags().BoolVar(&httpOpts.EnableTracing, "http-tracing", false, "whether to enable tracing on the HTTP server")

	// gRPC server flags
	rootCmd.Flags().BoolVar(&opts.AllowReflection, "grpc-allow-reflection", false, "whether to register the gRPC server for reflection; this is required to use tools like `grpc_cli`")
	rootCmd.Flags().BoolVar(&opts.EnableChannelz, "grpc-enable-channelz", false, "whether to enable the channelz service on the gRPC server")

	// HTTP server flags
	rootCmd.Flags().BoolVar(&httpOpts.DisableCompression, "http-no-compress", false, "whether to disable compression of HTTP API responses")
	rootCmd.Flags().BoolVar(&httpOpts.DisableDebug, "http-no-debug", false, "whether to disable /debug/pprof/* and /debug/env HTTP endpoints")
	rootCmd.Flags().Var(&debug.OmitEnv, "http-debug-omit-env", "name of an environment variable to omit from /debug/env, if http debug endpoints are enabled. specify multiple times to omit multiple env vars")
	rootCmd.Flags().Var(&debug.SanitizeEnv, "http-debug-sanitize-env", "name of an environment variable to sanitize in /debug/env, if http debug endpoints are enabled. specify multiple times to sanitize multiple env vars")
	rootCmd.Flags().StringVar(&opts.MetricsEndpoint, "http-metrics-endpoint", "/metrics",
		"HTTP endpoint to expose prometheus metrics on. Omit to disable scraping metrics. "+
			"Using a path used by VTAdmin's http API is unsupported and causes undefined behavior.")
	rootCmd.Flags().StringSliceVar(&httpOpts.CORSOrigins, "http-origin", []string{}, "repeated, comma-separated flag of allowed CORS origins. omit to disable CORS")
	rootCmd.Flags().StringVar(&httpOpts.ExperimentalOptions.TabletURLTmpl,
		"http-tablet-url-tmpl",
		"https://{{ .Tablet.Hostname }}:80",
		"[EXPERIMENTAL] Go template string to generate a reachable http(s) "+
			"address for a tablet. Currently used to make passthrough "+
			"requests to /debug/vars endpoints.",
	)

	// RBAC flags
	rootCmd.Flags().StringVar(&rbacConfigPath, "rbac-config", "", "path to an RBAC config file. must be set if passing --rbac")
	rootCmd.Flags().BoolVar(&enableRBAC, "rbac", false, "whether to enable RBAC. must be set if not passing --rbac")
	rootCmd.Flags().BoolVar(&disableRBAC, "no-rbac", false, "whether to disable RBAC. must be set if not passing --no-rbac")

	// Global cache flags (N.B. there are also cluster-specific cache flags)
	cacheRefreshHelp := "instructs a request to ignore any cached data (if applicable) and refresh the cache;" +
		"usable as an HTTP header named 'X-<key>' and as a gRPC metadata key '<key>'\n" +
		"Note: any whitespace characters are replaced with hyphens."
	rootCmd.Flags().StringVar(&cacheRefreshKey, "cache-refresh-key", "vt-cache-refresh", cacheRefreshHelp)

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

type noopCloser struct{}

func (nc *noopCloser) Close() error { return nil }
