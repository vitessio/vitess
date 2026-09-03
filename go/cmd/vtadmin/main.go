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
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtadmin"
	"vitess.io/vitess/go/vt/vtadmin/cache"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
	"vitess.io/vitess/go/vt/vtadmin/grpcserver"
	vtadminhttp "vitess.io/vitess/go/vt/vtadmin/http"
	"vitess.io/vitess/go/vt/vtadmin/http/debug"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
	"vitess.io/vitess/go/vt/vtadmin/vtadmin2"
	"vitess.io/vitess/go/vt/vtctl/grpcclientcommon"
	"vitess.io/vitess/go/vt/vtenv"
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

	// Server-rendered UI (vtadmin2) options.
	ui           string
	uiAddr       string
	uiReadOnly   bool
	uiDebugJSON  bool
	uiTrustProxy bool

	traceCloser io.Closer = &noopCloser{}

	rootCmd = &cobra.Command{
		Use: "vtadmin",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := validateUIOptions(ui, enableDynamicClusters); err != nil {
				return err
			}

			_flag.TrickGlog()

			if err := log.Init(cmd.Flags()); err != nil {
				return err
			}

			logutil.PurgeLogs()

			if opts.EnableTracing || httpOpts.EnableTracing {
				startTracing(cmd)
			}

			return nil
		},
		Run: run,
		PostRun: func(cmd *cobra.Command, args []string) {
			trace.LogErrorsWhenClosing(traceCloser)
		},
		Version: servenv.AppVersion.String(),
	}
)

// fatal ensures the tracer is closed and final spans are sent before issuing
// a log.Error call followed by os.Exit(1) with the given args.
func fatal(args ...any) {
	trace.LogErrorsWhenClosing(traceCloser)
	log.Error(fmt.Sprint(args...))
	os.Exit(1)
}

// startTracing checks the value of --tracer and then starts tracing, populating
// the private global traceCloser
func startTracing(cmd *cobra.Command) {
	tracer, err := cmd.Flags().GetString("tracer")
	if err != nil {
		log.Warn(fmt.Sprintf("not starting tracer; err: %s", err))
		return
	}

	if tracer == "" || tracer == "noop" {
		log.Warn("starting tracing with noop tracer")
	}

	traceCloser = trace.StartTracing("vtadmin")
}

func validateUIOptions(ui string, enableDynamicClusters bool) error {
	if ui != "react" && ui != "vtadmin2" {
		return fmt.Errorf("invalid --ui value %q: want react or vtadmin2", ui)
	}
	if ui == "vtadmin2" && enableDynamicClusters {
		return errors.New("--enable-dynamic-clusters is not supported with --ui=vtadmin2")
	}
	return nil
}

func run(cmd *cobra.Command, args []string) {
	bootSpan, ctx := trace.NewSpan(cmd.Context(), "vtadmin.boot")
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
		log.Warn("no cache-refresh-key set; forcing cache refreshes will not be possible")
	}
	cache.SetCacheRefreshKey(cacheRefreshKey)

	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: servenv.MySQLServerVersion(),
		TruncateUILen:      servenv.TruncateUILen,
		TruncateErrLen:     servenv.TruncateErrLen,
	})
	if err != nil {
		fatal(err)
	}
	s := vtadmin.NewAPI(env, clusters, vtadmin.Options{
		GRPCOpts:              opts,
		HTTPOpts:              httpOpts,
		RBAC:                  rbacConfig,
		EnableDynamicClusters: enableDynamicClusters,
	})

	// The vtadmin2 server reuses the same API implementation (and its RBAC
	// enforcement) and runs alongside the JSON API on its own address, so both
	// UIs can be served during the migration.
	if ui == "vtadmin2" {
		uiServer, err := vtadmin2.NewServer(s, vtadmin2.Options{
			Addr:            uiAddr,
			ReadOnly:        uiReadOnly,
			DocumentTitle:   "VTAdmin",
			EnableDebugJSON: uiDebugJSON,
			TrustProxyProto: uiTrustProxy,
			Authenticator:   rbacConfig.GetAuthenticator(),
		})
		if err != nil {
			fatal(err)
		}

		httpServer := vtadmin2.NewHTTPServer(uiAddr, uiServer)

		uiErr := make(chan error, 1)
		shutdownBase := context.WithoutCancel(cmd.Context())
		shutdownUI := func() {
			shutdownCtx, cancel := context.WithTimeout(shutdownBase, 5*time.Minute)
			defer cancel()
			if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("vtadmin2 UI shutdown failed", slog.Any("error", err))
			}
		}
		servenv.OnTermSync(shutdownUI)

		go func() {
			slog.Info("vtadmin2 UI listening", slog.String("addr", uiAddr))
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				uiErr <- err
			}
		}()

		defer shutdownUI()

		go func() {
			if err := <-uiErr; err != nil {
				slog.Error("vtadmin2 UI server failed", slog.Any("error", err))
				servenv.ExitChan <- os.Interrupt
			}
		}()
	}
	bootSpan.Finish()

	if err := s.ListenAndServe(); err != nil {
		fatal(err)
	}
}

func registerFlags() {
	// Common flags
	rootCmd.Flags().StringVar(&opts.Addr, "addr", ":15000", "address to serve on")
	rootCmd.Flags().DurationVar(&opts.CMuxReadTimeout, "lmux-read-timeout", time.Second, "how long to spend connection muxing")
	rootCmd.Flags().DurationVar(&opts.LameDuckDuration, "lame-duck-duration", time.Second*5, "length of lame duck period at shutdown")

	// Cluster config flags
	rootCmd.Flags().Var(&clusterConfigs, "cluster", "per-cluster configuration. any values here take precedence over those in -cluster-defaults or -cluster-config")
	rootCmd.Flags().Var(&clusterFileConfig, "cluster-config", "path to a yaml cluster configuration. see clusters.example.yaml") // (TODO:@amason) provide example config.
	rootCmd.Flags().Var(&defaultClusterConfig, "cluster-defaults", "default options for all clusters")
	rootCmd.Flags().BoolVar(&enableDynamicClusters, "enable-dynamic-clusters", false, "whether to enable dynamic clusters that are set by request header cookies or gRPC metadata")

	// Server-rendered UI flags
	rootCmd.Flags().StringVar(&ui, "ui", "react", "admin UI to serve: react (default SPA) or vtadmin2 (server-rendered UI)")
	rootCmd.Flags().StringVar(&uiAddr, "ui-addr", ":14202", "address for the vtadmin2 UI to listen on (used with --ui=vtadmin2)")
	rootCmd.Flags().BoolVar(&uiReadOnly, "ui-read-only", false, "run the vtadmin2 UI in read-only mode (used with --ui=vtadmin2)")
	rootCmd.Flags().BoolVar(&uiDebugJSON, "ui-debug-json", false, "enable ?format=json page data output in the vtadmin2 UI (used with --ui=vtadmin2)")
	rootCmd.Flags().BoolVar(&uiTrustProxy, "ui-trust-proxy-https", false, "mark UI cookies Secure when X-Forwarded-Proto: https is present from a trusted HTTPS-terminating proxy (used with --ui=vtadmin2)")

	// Tracing flags
	trace.RegisterFlags(rootCmd.Flags()) // defined in go/vt/trace
	utils.SetFlagBoolVar(rootCmd.Flags(), &opts.EnableTracing, "grpc-tracing", false, "whether to enable tracing on the gRPC server")
	rootCmd.Flags().BoolVar(&httpOpts.EnableTracing, "http-tracing", false, "whether to enable tracing on the HTTP server")

	// gRPC server flags
	rootCmd.Flags().BoolVar(&opts.AllowReflection, "grpc-allow-reflection", false, "whether to register the gRPC server for reflection; this is required to use tools like grpc_cli")
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
	rootCmd.Flags().StringVar(
		&httpOpts.ExperimentalOptions.TabletURLTmpl,
		"http-tablet-url-tmpl",
		"https://{{ .Tablet.Hostname }}:80",
		"[EXPERIMENTAL] Go template string to generate a reachable http(s) "+
			"address for a tablet. Currently used to make passthrough "+
			"requests to /debug/vars endpoints.",
	)

	// RBAC flags
	rootCmd.Flags().StringVar(&rbacConfigPath, "rbac-config", "", "path to an RBAC config file. must be set if passing --rbac")
	rootCmd.Flags().BoolVar(&enableRBAC, "rbac", false, "whether to enable RBAC. must be set if not passing --no-rbac")
	rootCmd.Flags().BoolVar(&disableRBAC, "no-rbac", false, "whether to disable RBAC. must be set if not passing --rbac")

	// Global cache flags (N.B. there are also cluster-specific cache flags)
	cacheRefreshHelp := "instructs a request to ignore any cached data (if applicable) and refresh the cache;" +
		"usable as an HTTP header named 'X-<key>' and as a gRPC metadata key '<key>'\n" +
		"Note: any whitespace characters are replaced with hyphens."
	rootCmd.Flags().StringVar(&cacheRefreshKey, "cache-refresh-key", "vt-cache-refresh", cacheRefreshHelp)

	// Structured logging flags.
	log.RegisterFlags(rootCmd.Flags())

	// glog flags, no better way to do this
	rootCmd.Flags().AddGoFlag(flag.Lookup("v"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("logtostderr"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("alsologtostderr"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("stderrthreshold"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("log_dir"))

	const deprecationMsg = "glog and its flags have been deprecated, use the default structured logging instead (\"--log-structured\")"
	rootCmd.Flags().MarkDeprecated("v", deprecationMsg)
	rootCmd.Flags().MarkDeprecated("logtostderr", deprecationMsg)
	rootCmd.Flags().MarkDeprecated("alsologtostderr", deprecationMsg)
	rootCmd.Flags().MarkDeprecated("stderrthreshold", deprecationMsg)
	rootCmd.Flags().MarkDeprecated("log_dir", deprecationMsg)

	servenv.RegisterMySQLServerFlags(rootCmd.Flags())

	// Register TLS flags for gRPC connections to vtctld
	grpcclientcommon.RegisterFlags(rootCmd.Flags())
}

func main() {
	registerFlags()

	rootCmd.SetGlobalNormalizationFunc(utils.NormalizeUnderscoresToDashes)
	if err := rootCmd.Execute(); err != nil {
		log.Error(fmt.Sprint(err))
		os.Exit(1)
	}
}

type noopCloser struct{}

func (nc *noopCloser) Close() error { return nil }
