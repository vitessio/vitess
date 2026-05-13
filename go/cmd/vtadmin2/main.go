/*
Copyright 2026 The Vitess Authors.

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
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"

	_flag "vitess.io/vitess/go/internal/flag"
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
	grpcOpts             grpcserver.Options
	httpOpts             vtadminhttp.Options
	clusterConfigs       cluster.ClustersFlag
	clusterFileConfig    cluster.FileConfig
	defaultClusterConfig cluster.Config
	uiOpts               vtadmin2.Options

	enableDynamicClusters bool
	rbacConfigPath        string
	enableRBAC            bool
	disableRBAC           bool
	cacheRefreshKey       string

	rootCmd = &cobra.Command{
		Use:     "vtadmin2",
		Short:   "Go and HTML VTAdmin web UI",
		PreRunE: preRun,
		RunE:    run,
		Version: servenv.AppVersion.String(),
	}
)

func preRun(cmd *cobra.Command, args []string) error {
	_flag.TrickGlog()
	if err := log.Init(cmd.Flags()); err != nil {
		return err
	}
	logutil.PurgeLogs()
	return validateRBACFlags()
}

func run(cmd *cobra.Command, args []string) error {
	api, err := buildAPI(cmd.Context())
	if err != nil {
		return err
	}

	server, err := vtadmin2.NewServer(api, buildVTAdmin2Options())
	if err != nil {
		return err
	}

	addr := uiOpts.Addr
	if addr == "" {
		addr = ":15001"
	}
	log.Info(fmt.Sprintf("starting vtadmin2 on %s", addr))
	return http.ListenAndServe(addr, server)
}

func buildAPI(ctx context.Context) (*vtadmin.API, error) {
	configs := clusterFileConfig.Combine(defaultClusterConfig, clusterConfigs)
	if len(configs) == 0 && !enableDynamicClusters {
		return nil, fmt.Errorf("must specify at least one cluster")
	}

	rbacConfig, err := buildRBACConfig()
	if err != nil {
		return nil, err
	}

	clusters := make([]*cluster.Cluster, 0, len(configs))
	for _, cfg := range configs {
		cluster, err := cfg.Cluster(ctx)
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, cluster)
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
		return nil, err
	}

	return vtadmin.NewAPI(env, clusters, vtadmin.Options{
		GRPCOpts:              grpcOpts,
		HTTPOpts:              httpOpts,
		RBAC:                  rbacConfig,
		EnableDynamicClusters: enableDynamicClusters,
	}), nil
}

func validateRBACFlags() error {
	if enableRBAC == disableRBAC {
		return fmt.Errorf("must explicitly enable or disable RBAC by passing --rbac or --no-rbac")
	}
	if enableRBAC && rbacConfigPath == "" {
		return fmt.Errorf("must pass --rbac-config path when enabling rbac")
	}
	return nil
}

func buildRBACConfig() (*rbac.Config, error) {
	if disableRBAC {
		return rbac.DefaultConfig(), nil
	}
	return rbac.LoadConfig(rbacConfigPath)
}

func buildVTAdmin2Options() vtadmin2.Options {
	opts := uiOpts
	if opts.DocumentTitle == "" {
		opts.DocumentTitle = "VTAdmin2"
	}
	return opts
}

func registerFlags() {
	rootCmd.Flags().StringVar(&uiOpts.Addr, "addr", ":15001", "address to serve the vtadmin2 web UI on")
	rootCmd.Flags().BoolVar(&uiOpts.ReadOnly, "read-only", false, "hide vtadmin2 mutating actions in the UI")
	rootCmd.Flags().StringVar(&uiOpts.DocumentTitle, "document-title", "VTAdmin2", "document title for vtadmin2 pages")
	rootCmd.Flags().BoolVar(&uiOpts.EnableDebugJSON, "debug-json", false, "enable debug JSON views in vtadmin2 pages")

	rootCmd.Flags().Var(&clusterConfigs, "cluster", "per-cluster configuration. any values here take precedence over those in -cluster-defaults or -cluster-config")
	rootCmd.Flags().Var(&clusterFileConfig, "cluster-config", "path to a yaml cluster configuration. see clusters.example.yaml")
	rootCmd.Flags().Var(&defaultClusterConfig, "cluster-defaults", "default options for all clusters")
	rootCmd.Flags().BoolVar(&enableDynamicClusters, "enable-dynamic-clusters", false, "whether to enable dynamic clusters that are set by request header cookies or gRPC metadata")

	rootCmd.Flags().BoolVar(&httpOpts.DisableDebug, "http-no-debug", false, "whether to omit /debug/pprof/* and /debug/env HTTP endpoints from the backing API")
	rootCmd.Flags().Var(&debug.OmitEnv, "http-debug-omit-env", "name of an environment variable to omit from /debug/env, if http debug endpoints are enabled")
	rootCmd.Flags().Var(&debug.SanitizeEnv, "http-debug-sanitize-env", "name of an environment variable to sanitize in /debug/env, if http debug endpoints are enabled")
	rootCmd.Flags().StringVar(&httpOpts.ExperimentalOptions.TabletURLTmpl, "http-tablet-url-tmpl", "https://{{ .Tablet.Hostname }}:80", "[EXPERIMENTAL] Go template string to generate a reachable tablet URL")

	rootCmd.Flags().StringVar(&rbacConfigPath, "rbac-config", "", "path to an RBAC config file. must be set if passing --rbac")
	rootCmd.Flags().BoolVar(&enableRBAC, "rbac", false, "whether to enable RBAC. must be set if not passing --no-rbac")
	rootCmd.Flags().BoolVar(&disableRBAC, "no-rbac", false, "whether to disable RBAC. must be set if not passing --rbac")
	rootCmd.Flags().StringVar(&cacheRefreshKey, "cache-refresh-key", "vt-cache-refresh", "cache refresh key used by the backing VTAdmin API")

	log.RegisterFlags(rootCmd.Flags())
	rootCmd.Flags().AddGoFlag(flag.Lookup("v"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("logtostderr"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("alsologtostderr"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("stderrthreshold"))
	rootCmd.Flags().AddGoFlag(flag.Lookup("log_dir"))
	servenv.RegisterMySQLServerFlags(rootCmd.Flags())
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
