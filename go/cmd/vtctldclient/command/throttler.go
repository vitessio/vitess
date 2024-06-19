/*
Copyright 2022 The Vitess Authors.

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

package command

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	// UpdateThrottlerConfig makes a UpdateThrottlerConfig gRPC call to a vtctld.
	UpdateThrottlerConfig = &cobra.Command{
		Use:                   "UpdateThrottlerConfig [--enable|--disable] [--threshold=<float64>] [--custom-query=<query>] [--check-as-check-self|--check-as-check-shard] [--throttle-app|unthrottle-app=<name>] [--throttle-app-ratio=<float, range [0..1]>] [--throttle-app-duration=<duration>] <keyspace>",
		Short:                 "Update the tablet throttler configuration for all tablets in the given keyspace (across all cells)",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandUpdateThrottlerConfig,
	}
	CheckThrottler = &cobra.Command{
		Use:                   "CheckThrottler [--app-name <name>] <tablet alias>",
		Short:                 "Issue a throttler check on the given tablet.",
		Example:               "CheckThrottler --app-name online-ddl zone1-0000000101",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandCheckThrottler,
	}

	GetThrottlerStatus = &cobra.Command{
		Use:                   "GetThrottlerStatus <tablet alias>",
		Short:                 "Get the throttler status for the given tablet.",
		Example:               "GetThrottlerStatus zone1-0000000101",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetThrottlerStatus,
	}
)

var (
	updateThrottlerConfigOptions vtctldatapb.UpdateThrottlerConfigRequest
	throttledAppRule             topodatapb.ThrottledAppRule
	unthrottledAppRule           topodatapb.ThrottledAppRule
	throttledAppDuration         time.Duration

	checkThrottlerOptions vtctldatapb.CheckThrottlerRequest
)

func commandUpdateThrottlerConfig(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)
	cli.FinishedParsing(cmd)

	if throttledAppRule.Name != "" && unthrottledAppRule.Name != "" {
		return fmt.Errorf("throttle-app and unthrottle-app are mutually exclusive")
	}

	if updateThrottlerConfigOptions.MetricName != "" && !cmd.Flags().Changed("threshold") {
		return fmt.Errorf("--metric-name flag requires --threshold flag. Set threshold to 0 to disable the metric threshold configuration")
	}
	if cmd.Flags().Changed("app-name") != cmd.Flags().Changed("app-metrics") {
		return fmt.Errorf("--app-name and --app-metrics must be set together")
	}
	if cmd.Flags().Changed("app-name") && updateThrottlerConfigOptions.AppName == "" {
		return fmt.Errorf("--app-name must not be empty")
	}

	updateThrottlerConfigOptions.CustomQuerySet = cmd.Flags().Changed("custom-query")
	updateThrottlerConfigOptions.Keyspace = keyspace

	if throttledAppRule.Name != "" {
		throttledAppRule.ExpiresAt = protoutil.TimeToProto(time.Now().Add(throttledAppDuration))
		updateThrottlerConfigOptions.ThrottledApp = &throttledAppRule
	} else if unthrottledAppRule.Name != "" {
		unthrottledAppRule.ExpiresAt = &vttime.Time{} // zero
		updateThrottlerConfigOptions.ThrottledApp = &unthrottledAppRule
	}

	_, err := client.UpdateThrottlerConfig(commandCtx, &updateThrottlerConfigOptions)
	if err != nil {
		return err
	}
	return nil
}

func commandCheckThrottler(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)
	if _, err := base.ScopeFromString(checkThrottlerOptions.Scope); err != nil {
		return err
	}
	resp, err := client.CheckThrottler(commandCtx, &vtctldatapb.CheckThrottlerRequest{
		TabletAlias:           alias,
		AppName:               checkThrottlerOptions.AppName,
		Scope:                 checkThrottlerOptions.Scope,
		SkipRequestHeartbeats: checkThrottlerOptions.SkipRequestHeartbeats,
		OkIfNotExists:         checkThrottlerOptions.OkIfNotExists,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandGetThrottlerStatus(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetThrottlerStatus(commandCtx, &vtctldatapb.GetThrottlerStatusRequest{
		TabletAlias: alias,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	// UpdateThrottlerConfig
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.Enable, "enable", false, "Enable the throttler")
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.Disable, "disable", false, "Disable the throttler")
	UpdateThrottlerConfig.Flags().StringVar(&updateThrottlerConfigOptions.MetricName, "metric-name", "", "name of the metric for which we apply a new threshold (requires --threshold). If empty, the default (either 'lag' or 'custom') metric is used.")
	UpdateThrottlerConfig.Flags().Float64Var(&updateThrottlerConfigOptions.Threshold, "threshold", 0, "threshold for the either default check (replication lag seconds) or custom check")
	UpdateThrottlerConfig.Flags().StringVar(&updateThrottlerConfigOptions.CustomQuery, "custom-query", "", "custom throttler check query")
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.CheckAsCheckSelf, "check-as-check-self", false, "/throttler/check requests behave as is /throttler/check-self was called")
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.CheckAsCheckShard, "check-as-check-shard", false, "use standard behavior for /throttler/check requests")

	UpdateThrottlerConfig.Flags().StringVar(&unthrottledAppRule.Name, "unthrottle-app", "", "an app name to unthrottle")
	UpdateThrottlerConfig.Flags().StringVar(&throttledAppRule.Name, "throttle-app", "", "an app name to throttle")
	UpdateThrottlerConfig.Flags().Float64Var(&throttledAppRule.Ratio, "throttle-app-ratio", throttle.DefaultThrottleRatio, "ratio to throttle app (app specififed in --throttled-app)")
	UpdateThrottlerConfig.Flags().DurationVar(&throttledAppDuration, "throttle-app-duration", throttle.DefaultAppThrottleDuration, "duration after which throttled app rule expires (app specififed in --throttled-app)")
	UpdateThrottlerConfig.Flags().BoolVar(&throttledAppRule.Exempt, "throttle-app-exempt", throttledAppRule.Exempt, "exempt this app from being at all throttled. WARNING: use with extreme care, as this is likely to push metrics beyond the throttler's threshold, and starve other apps")

	UpdateThrottlerConfig.Flags().StringVar(&updateThrottlerConfigOptions.AppName, "app-name", "", "app name for which to assign metrics (requires --app-metrics)")
	UpdateThrottlerConfig.Flags().StringSliceVar(&updateThrottlerConfigOptions.AppCheckedMetrics, "app-metrics", nil, "metrics to be used when checking the throttler for the app (requires --app-name). Empty to restore to default metrics")

	Root.AddCommand(UpdateThrottlerConfig)
	// Check Throttler
	CheckThrottler.Flags().StringVar(&checkThrottlerOptions.AppName, "app-name", throttlerapp.VitessName.String(), "app to identify as")
	CheckThrottler.Flags().StringVar(&checkThrottlerOptions.Scope, "scope", base.UndefinedScope.String(), "check scope ('shard', 'self' or leave empty for per-metric defaults)")
	CheckThrottler.Flags().BoolVar(&checkThrottlerOptions.SkipRequestHeartbeats, "skip-heartbeats", false, "skip renewing heartbeat lease")
	CheckThrottler.Flags().BoolVar(&checkThrottlerOptions.OkIfNotExists, "ok-if-not-exists", false, "return OK even if metric does not exist")
	Root.AddCommand(CheckThrottler)

	// GetThrottlerStatus
	Root.AddCommand(GetThrottlerStatus)
}
