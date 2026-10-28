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
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"

	"vitess.io/vitess/go/vt/proto/querythrottler"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	// UpdateThrottlerConfig makes a UpdateThrottlerConfig gRPC call to a vtctld.
	UpdateThrottlerConfig = &cobra.Command{
		Use:                   "UpdateThrottlerConfig [--enable|--disable] [--metric-name=<name>] [--threshold=<float64>] [--custom-query=<query>] [--throttle-app|unthrottle-app=<name>] [--throttle-app-ratio=<float, range [0..1]>] [--throttle-app-duration=<duration>] [--throttle-app-exempt=<bool>] [--app-name=<name> --app-metrics=<metrics>] <keyspace>",
		Short:                 "Update the tablet throttler configuration for all tablets in the given keyspace (across all cells)",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		PreRunE:               validateUpdateThrottlerConfig,
		RunE:                  commandUpdateThrottlerConfig,
	}
	CheckThrottler = &cobra.Command{
		Use:                   "CheckThrottler [--app-name <name>] [--throttler-type <type>] <tablet alias>",
		Short:                 "Issue a throttler check on the given tablet.",
		Example:               "CheckThrottler --app-name online-ddl zone1-0000000101",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandCheckThrottler,
	}

	GetThrottlerStatus = &cobra.Command{
		Use:                   "GetThrottlerStatus [--throttler-type <type>] <tablet alias>",
		Short:                 "Get the throttler status for the given tablet.",
		Example:               "GetThrottlerStatus --throttler-type DEDICATED_QUERY_THROTTLER zone1-0000000101",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetThrottlerStatus,
	}

	// UpdateQueryThrottlerConfig makes an UpdateQueryThrottlerConfig gRPC call to a vtctld.
	UpdateQueryThrottlerConfig = &cobra.Command{
		Use:   "UpdateQueryThrottlerConfig {--config <config> | --config-file <file>} <keyspace>",
		Short: "Update the query throttler configuration for the given keyspace.",
		Long: `Update the query throttler configuration for the given keyspace. The configuration controls how incoming queries are throttled based on tablet throttler metrics.

The command replaces the keyspace's whole configuration, so {"enabled":false} also removes every rule. To change one rule, pass the full configuration with that change. GetKeyspace shows the current configuration, and GetSrvKeyspaces shows each cell's copy.

Tablets act on a throttler check they refresh every --query-throttler-cache-update-interval (default 10s). Throttling starts and stops up to that long after a metric crosses its threshold, and a tablet that has just enabled the configuration throttles nothing until its first check completes.

Statements whose rejection would cost more than running them are never throttled, and dry_run does not count them: statements inside a transaction (explicit transactions, every statement in a session with autocommit=0, and DML that VTGate runs in a transaction of its own under autocommit, such as an UPDATE or DELETE that spans several shards or a write to a table with an owned lookup vindex), sequence fetches, statements that take or release an advisory lock, and SELECT statements that read no table.

On a primary, "lag" (or "shard/lag") is the highest replication lag among the replicas it probes, which --throttle-tablet-types selects (default: replica), so a single lagging replica throttles every query that a PRIMARY lag rule matches. On a replica, it is that replica's own lag.`,
		Example: `# Throttle 50% of INSERTs on primaries when replication lag is above 5s, and all of them above 10s.
UpdateQueryThrottlerConfig --config '{"enabled":true,"strategy":"TABLET_THROTTLER","dry_run":false,"tablet_strategy_config":{"tablet_rules":{"PRIMARY":{"statement_rules":{"INSERT":{"metric_rules":{"lag":{"thresholds":[{"above":5,"throttle":50},{"above":10,"throttle":100}]}}}}}}}}' my_keyspace

# Disable the query throttler.
UpdateQueryThrottlerConfig --config '{"enabled":false}' my_keyspace`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		PreRunE:               validateUpdateQueryThrottlerConfig,
		RunE:                  commandUpdateQueryThrottlerConfig,
	}
)

var (
	updateThrottlerConfigOptions vtctldatapb.UpdateThrottlerConfigRequest
	throttledAppRule             topodatapb.ThrottledAppRule
	unthrottledAppRule           topodatapb.ThrottledAppRule
	throttledAppDuration         time.Duration

	checkThrottlerOptions vtctldatapb.CheckThrottlerRequest
	requestHeartbeats     bool
	checkThrottlerType    string

	getThrottlerStatusType string

	updateQueryThrottlerConfigOptions = struct {
		Config     string
		ConfigFile string
	}{}
)

func validateUpdateThrottlerConfig(cmd *cobra.Command, args []string) error {
	if updateThrottlerConfigOptions.MetricName != "" && !cmd.Flags().Changed("threshold") {
		return errors.New("--metric-name flag requires --threshold flag. Set threshold to 0 to disable the metric threshold configuration")
	}
	if cmd.Flags().Changed("app-name") && updateThrottlerConfigOptions.AppName == "" {
		return errors.New("--app-name must not be empty")
	}

	return nil
}

func commandUpdateThrottlerConfig(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)
	cli.FinishedParsing(cmd)

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

// parseThrottlerType converts a --throttler-type value to the throttler it selects.
func parseThrottlerType(name string) (tabletmanagerdatapb.ThrottlerType, error) {
	v, ok := tabletmanagerdatapb.ThrottlerType_value[strings.ToUpper(name)]
	if !ok {
		return 0, fmt.Errorf("unknown --throttler-type %q; must be one of DEFAULT_LAG_THROTTLER, DEDICATED_QUERY_THROTTLER", name)
	}
	return tabletmanagerdatapb.ThrottlerType(v), nil
}

// checkAnsweringThrottlerType returns an error when a response comes from another throttler than the one
// --throttler-type asked for. A vtctld that predates the field drops it from the request, and a tablet that
// predates it answers for the lag throttler; either way, the response carries no type, which reads as
// DEFAULT_LAG_THROTTLER.
func checkAnsweringThrottlerType(requested, answered tabletmanagerdatapb.ThrottlerType) error {
	if answered == requested {
		return nil
	}
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "--throttler-type %s was requested, but %s answered; the vtctld or the tablet probably runs a version that does not support --throttler-type", requested, answered)
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
	throttlerType, err := parseThrottlerType(checkThrottlerType)
	if err != nil {
		return err
	}
	resp, err := client.CheckThrottler(commandCtx, &vtctldatapb.CheckThrottlerRequest{
		TabletAlias:           alias,
		AppName:               checkThrottlerOptions.AppName,
		Scope:                 checkThrottlerOptions.Scope,
		SkipRequestHeartbeats: !requestHeartbeats,
		OkIfNotExists:         checkThrottlerOptions.OkIfNotExists,
		ThrottlerType:         throttlerType,
	})
	if err != nil {
		return err
	}
	if err := checkAnsweringThrottlerType(throttlerType, resp.GetCheck().GetThrottlerType()); err != nil {
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

	throttlerType, err := parseThrottlerType(getThrottlerStatusType)
	if err != nil {
		return err
	}
	resp, err := client.GetThrottlerStatus(commandCtx, &vtctldatapb.GetThrottlerStatusRequest{
		TabletAlias:   alias,
		ThrottlerType: throttlerType,
	})
	if err != nil {
		return err
	}
	if err := checkAnsweringThrottlerType(throttlerType, resp.GetStatus().GetThrottlerType()); err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func validateUpdateQueryThrottlerConfig(cmd *cobra.Command, args []string) error {
	opts := updateQueryThrottlerConfigOptions
	if (opts.Config != "" && opts.ConfigFile != "") || (opts.Config == "" && opts.ConfigFile == "") {
		return errors.New("must pass exactly one of --config or --config-file")
	}
	return nil
}

// validateQueryThrottlerConfigContent performs client-side validation of the
// config content for fast feedback before the gRPC round-trip.
func validateQueryThrottlerConfigContent(cfg *querythrottler.Config) error {
	if !cfg.GetEnabled() {
		return nil
	}

	if cfg.GetStrategy() != querythrottler.ThrottlingStrategy_TABLET_THROTTLER {
		return fmt.Errorf("strategy must be TABLET_THROTTLER when enabled is true, got %s", cfg.GetStrategy())
	}

	tsc := cfg.GetTabletStrategyConfig()
	if tsc == nil {
		return errors.New("tablet_strategy_config is required when strategy is TABLET_THROTTLER")
	}

	if len(tsc.GetTabletRules()) == 0 {
		return errors.New("tablet_rules cannot be empty when strategy is TABLET_THROTTLER")
	}

	// This mirrors grpcvtctldserver.validateQueryThrottlerConfig, which is where each rule
	// is explained. Duplicated here only for fast feedback; keep the two in sync.
	seenBareMetrics := make(map[string]string)
	for tabletType, stmtRuleSet := range tsc.GetTabletRules() {
		if !base.IsQueryThrottlerTabletType(tabletType) {
			return fmt.Errorf("unsupported tablet type %q; must be one of PRIMARY, REPLICA, RDONLY, EXPERIMENTAL", tabletType)
		}
		if len(stmtRuleSet.GetStatementRules()) == 0 {
			return fmt.Errorf("statement_rules cannot be empty (tablet_type=%s)", tabletType)
		}
		for stmtType, metricRuleSet := range stmtRuleSet.GetStatementRules() {
			if !base.IsQueryThrottlerStatementType(stmtType) {
				return fmt.Errorf("unsupported statement type %q (tablet_type=%s)", stmtType, tabletType)
			}
			if len(metricRuleSet.GetMetricRules()) == 0 {
				return fmt.Errorf("metric_rules cannot be empty (tablet_type=%s, statement=%s)", tabletType, stmtType)
			}
			for metricName, rule := range metricRuleSet.GetMetricRules() {
				_, mName, err := base.DisaggregateMetricName(metricName)
				if err != nil {
					return fmt.Errorf("unknown metric name %q (tablet_type=%s, statement=%s)", metricName, tabletType, stmtType)
				}
				if mName == base.CustomMetricName {
					return fmt.Errorf("custom metric is not supported by the query throttler (tablet_type=%s, statement=%s, metric=%s)", tabletType, stmtType, metricName)
				}
				bare := mName.String()
				if prevKey, ok := seenBareMetrics[bare]; ok && prevKey != metricName {
					return fmt.Errorf("metric %q is configured under multiple scopes; only one scope of a metric may be configured across the query throttler config", bare)
				}
				seenBareMetrics[bare] = metricName
				if len(rule.GetThresholds()) == 0 {
					return fmt.Errorf("thresholds cannot be empty (tablet_type=%s, statement=%s, metric=%s)", tabletType, stmtType, metricName)
				}
				seenAbove := make(map[float64]int)
				for i, t := range rule.GetThresholds() {
					if math.IsNaN(t.GetAbove()) || math.IsInf(t.GetAbove(), 0) {
						return fmt.Errorf("threshold[%d] 'above' must be a finite number, got %v (tablet_type=%s, statement=%s, metric=%s)",
							i, t.GetAbove(), tabletType, stmtType, metricName)
					}
					if t.GetAbove() < 0 {
						return fmt.Errorf("threshold[%d] 'above' must be >= 0, got %v (tablet_type=%s, statement=%s, metric=%s)",
							i, t.GetAbove(), tabletType, stmtType, metricName)
					}
					if j, ok := seenAbove[t.GetAbove()]; ok {
						return fmt.Errorf("threshold[%d] and threshold[%d] have the same 'above' %v (tablet_type=%s, statement=%s, metric=%s)",
							j, i, t.GetAbove(), tabletType, stmtType, metricName)
					}
					seenAbove[t.GetAbove()] = i

					if t.GetThrottle() < 0 || t.GetThrottle() > 100 {
						return fmt.Errorf("threshold[%d] 'throttle' must be between 0 and 100, got %d (tablet_type=%s, statement=%s, metric=%s)",
							i, t.GetThrottle(), tabletType, stmtType, metricName)
					}
				}
			}
		}
	}

	return nil
}

// parseQueryThrottlerConfig decodes a query throttler config from JSON. Unlike
// json2.UnmarshalPB, it rejects unknown fields, so a misspelled key such as "dry-run"
// fails instead of being silently dropped from the config that gets pushed.
func parseQueryThrottlerConfig(data []byte) (*querythrottler.Config, error) {
	config := &querythrottler.Config{}
	if err := protojson.Unmarshal(data, config); err != nil {
		return nil, err
	}
	return config, nil
}

func commandUpdateQueryThrottlerConfig(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)
	opts := updateQueryThrottlerConfigOptions
	cli.FinishedParsing(cmd)

	var configBytes []byte
	if opts.ConfigFile != "" {
		data, err := os.ReadFile(opts.ConfigFile)
		if err != nil {
			return err
		}
		configBytes = data
	} else {
		configBytes = []byte(opts.Config)
	}

	config, err := parseQueryThrottlerConfig(configBytes)
	if err != nil {
		return err
	}

	if err := validateQueryThrottlerConfigContent(config); err != nil {
		return err
	}

	_, err = client.UpdateQueryThrottlerConfig(commandCtx, &vtctldatapb.UpdateQueryThrottlerConfigRequest{
		Keyspace:             keyspace,
		QueryThrottlerConfig: config,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Successfully updated QueryThrottlerConfig for keyspace %s\n", keyspace)
	return nil
}

func init() {
	// UpdateThrottlerConfig
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.Enable, "enable", false, "Enable the throttler")
	UpdateThrottlerConfig.Flags().BoolVar(&updateThrottlerConfigOptions.Disable, "disable", false, "Disable the throttler")
	UpdateThrottlerConfig.Flags().StringVar(&updateThrottlerConfigOptions.MetricName, "metric-name", "", "name of the metric for which we apply a new threshold (requires --threshold). If empty, the default (either 'lag' or 'custom') metric is used.")
	UpdateThrottlerConfig.Flags().Float64Var(&updateThrottlerConfigOptions.Threshold, "threshold", 0, "threshold for the either default check (replication lag seconds) or custom check")
	UpdateThrottlerConfig.Flags().StringVar(&updateThrottlerConfigOptions.CustomQuery, "custom-query", "", "custom throttler check query")

	UpdateThrottlerConfig.Flags().StringVar(&unthrottledAppRule.Name, "unthrottle-app", "", "an app name to unthrottle")
	UpdateThrottlerConfig.Flags().StringVar(&throttledAppRule.Name, "throttle-app", "", "an app name to throttle")
	UpdateThrottlerConfig.Flags().Float64Var(&throttledAppRule.Ratio, "throttle-app-ratio", throttle.DefaultThrottleRatio, "ratio to throttle app (app specififed in --throttled-app)")
	UpdateThrottlerConfig.Flags().DurationVar(&throttledAppDuration, "throttle-app-duration", throttle.DefaultAppThrottleDuration, "duration after which throttled app rule expires (app specififed in --throttled-app)")
	UpdateThrottlerConfig.Flags().BoolVar(&throttledAppRule.Exempt, "throttle-app-exempt", throttledAppRule.Exempt, "exempt this app from being at all throttled. WARNING: use with extreme care, as this is likely to push metrics beyond the throttler's threshold, and starve other apps")
	UpdateThrottlerConfig.Flags().StringVar(&updateThrottlerConfigOptions.AppName, "app-name", "", "app name for which to assign metrics (requires --app-metrics)")
	UpdateThrottlerConfig.Flags().StringSliceVar(&updateThrottlerConfigOptions.AppCheckedMetrics, "app-metrics", nil, "metrics to be used when checking the throttler for the app (requires --app-name). Empty to restore to default metrics. Example: --app-metrics=lag,custom,shard/loadavg")
	UpdateThrottlerConfig.MarkFlagsMutuallyExclusive("unthrottle-app", "throttle-app")
	UpdateThrottlerConfig.MarkFlagsRequiredTogether("app-name", "app-metrics")

	Root.AddCommand(UpdateThrottlerConfig)
	// Check Throttler
	CheckThrottler.Flags().StringVar(&checkThrottlerOptions.AppName, "app-name", throttlerapp.VitessName.String(), "app name to check")
	CheckThrottler.Flags().StringVar(&checkThrottlerOptions.Scope, "scope", base.UndefinedScope.String(), "check scope ('shard', 'self' or leave empty for per-metric defaults)")
	CheckThrottler.Flags().BoolVar(&requestHeartbeats, "request-heartbeats", false, "request heartbeat lease")
	CheckThrottler.Flags().BoolVar(&checkThrottlerOptions.OkIfNotExists, "ok-if-not-exists", false, "return OK even if metric does not exist")
	CheckThrottler.Flags().StringVar(&checkThrottlerType, "throttler-type", tabletmanagerdatapb.ThrottlerType_DEFAULT_LAG_THROTTLER.String(), "throttler to check: DEFAULT_LAG_THROTTLER or DEDICATED_QUERY_THROTTLER (the query throttler's tablet throttler)")
	Root.AddCommand(CheckThrottler)

	// GetThrottlerStatus
	GetThrottlerStatus.Flags().StringVar(&getThrottlerStatusType, "throttler-type", tabletmanagerdatapb.ThrottlerType_DEFAULT_LAG_THROTTLER.String(), "throttler to get the status of: DEFAULT_LAG_THROTTLER or DEDICATED_QUERY_THROTTLER (the query throttler's tablet throttler)")
	Root.AddCommand(GetThrottlerStatus)

	// UpdateQueryThrottlerConfig
	UpdateQueryThrottlerConfig.Flags().StringVarP(&updateQueryThrottlerConfigOptions.Config, "config", "c", "", "QueryThrottlerConfig as a JSON string")
	UpdateQueryThrottlerConfig.Flags().StringVarP(&updateQueryThrottlerConfigOptions.ConfigFile, "config-file", "f", "", "Path to a file containing QueryThrottlerConfig as JSON")
	Root.AddCommand(UpdateQueryThrottlerConfig)
}
