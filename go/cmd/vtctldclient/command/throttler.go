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

	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
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
)

var (
	updateThrottlerConfigOptions vtctldatapb.UpdateThrottlerConfigRequest
	throttledAppRule             topodatapb.ThrottledAppRule
	unthrottledAppRule           topodatapb.ThrottledAppRule
	throttledAppDuration         time.Duration
)

func commandUpdateThrottlerConfig(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)
	cli.FinishedParsing(cmd)

	if throttledAppRule.Name != "" && unthrottledAppRule.Name != "" {
		return fmt.Errorf("throttle-app and unthrottle-app are mutually exclusive")
	}

	updateThrottlerConfigOptions.CustomQuerySet = cmd.Flags().Changed("custom-query")
	updateThrottlerConfigOptions.Keyspace = keyspace

	if throttledAppRule.Name != "" {
		throttledAppRule.ExpiresAt = logutil.TimeToProto(time.Now().Add(throttledAppDuration))
		updateThrottlerConfigOptions.ThrottledApp = &throttledAppRule
	} else if unthrottledAppRule.Name != "" {
		unthrottledAppRule.ExpiresAt = logutil.TimeToProto(time.Now())
		updateThrottlerConfigOptions.ThrottledApp = &unthrottledAppRule
	}

	_, err := client.UpdateThrottlerConfig(commandCtx, &updateThrottlerConfigOptions)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.Enable, "enable", false, "Enable the throttler")
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.Disable, "disable", false, "Disable the throttler")
	UpdateThrottlerConfig.Flags().Float64Var(&updateThrottlerConfigOptions.Threshold, "threshold", 0, "threshold for the either default check (replication lag seconds) or custom check")
	UpdateThrottlerConfig.Flags().StringVar(&updateThrottlerConfigOptions.CustomQuery, "custom-query", "", "custom throttler check query")
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.CheckAsCheckSelf, "check-as-check-self", false, "/throttler/check requests behave as is /throttler/check-self was called")
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.CheckAsCheckShard, "check-as-check-shard", false, "use standard behavior for /throttler/check requests")

	UpdateThrottlerConfig.Flags().StringVar(&unthrottledAppRule.Name, "unthrottle-app", "", "an app name to unthrottle")
	UpdateThrottlerConfig.Flags().StringVar(&throttledAppRule.Name, "throttle-app", "", "an app name to throttle")
	UpdateThrottlerConfig.Flags().Float64Var(&throttledAppRule.Ratio, "throttle-app-ratio", throttle.DefaultThrottleRatio, "ratio to throttle app (app specififed in --throttled-app)")
	UpdateThrottlerConfig.Flags().DurationVar(&throttledAppDuration, "throttle-app-duration", throttle.DefaultAppThrottleDuration, "duration after which throttled app rule expires (app specififed in --throttled-app)")
	UpdateThrottlerConfig.Flags().BoolVar(&throttledAppRule.Exempt, "throttle-app-exempt", throttledAppRule.Exempt, "exempt this app from being at all throttled. WARNING: use with extreme care, as this is likely to push metrics beyond the throttler's threshold, and starve other apps")

	Root.AddCommand(UpdateThrottlerConfig)
}
