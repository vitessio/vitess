/*
Copyright 2023 The Vitess Authors.

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
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	AllMigrationsIndicator = "all"
)

var (
	OnlineDDL = &cobra.Command{
		Use:                   "OnlineDDL <cmd> <keyspace> [args]",
		Short:                 "Operates on online DDL (schema migrations).",
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(2),
	}
	OnlineDDLCancel = &cobra.Command{
		Use:                   "cancel <keyspace> <uuid|all>",
		Short:                 "Cancel one or all migrations, terminating any running ones as needed.",
		Example:               "OnlineDDL cancel test_keyspace 82fa54ac_e83e_11ea_96b7_f875a4d24e90",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandOnlineDDLCancel,
	}
	OnlineDDLCleanup = &cobra.Command{
		Use:                   "cleanup <keyspace> <uuid>",
		Short:                 "Mark a given schema migration ready for artifact cleanup.",
		Example:               "OnlineDDL cleanup test_keyspace 82fa54ac_e83e_11ea_96b7_f875a4d24e90",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandOnlineDDLCleanup,
	}
	OnlineDDLComplete = &cobra.Command{
		Use:                   "complete <keyspace> <uuid|all>",
		Short:                 "Complete one or all migrations executed with --postpone-completion",
		Example:               "OnlineDDL complete test_keyspace 82fa54ac_e83e_11ea_96b7_f875a4d24e90",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandOnlineDDLComplete,
	}
	OnlineDDLLaunch = &cobra.Command{
		Use:                   "launch <keyspace> <uuid|all>",
		Short:                 "Launch one or all migrations executed with --postpone-launch",
		Example:               "OnlineDDL launch test_keyspace 82fa54ac_e83e_11ea_96b7_f875a4d24e90",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandOnlineDDLLaunch,
	}
	OnlineDDLRetry = &cobra.Command{
		Use:                   "retry <keyspace> <uuid>",
		Short:                 "Mark a given schema migration for retry.",
		Example:               "vtctl OnlineDDL retry test_keyspace 82fa54ac_e83e_11ea_96b7_f875a4d24e90",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandOnlineDDLRetry,
	}
	OnlineDDLThrottle = &cobra.Command{
		Use:                   "throttle <keyspace> <uuid|all>",
		Short:                 "Throttles one or all migrations",
		Example:               "OnlineDDL throttle all",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandOnlineDDLThrottle,
	}
	OnlineDDLUnthrottle = &cobra.Command{
		Use:                   "unthrottle <keyspace> <uuid|all>",
		Short:                 "Unthrottles one or all migrations",
		Example:               "OnlineDDL unthrottle all",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandOnlineDDLUnthrottle,
	}
	OnlineDDLForceCutOver = &cobra.Command{
		Use:                   "force-cutover <keyspace> <uuid|all>",
		Short:                 "Mark a given schema migration, or all pending migrations, for forced cut over.",
		Example:               "OnlineDDL force-cutover test_keyspace 82fa54ac_e83e_11ea_96b7_f875a4d24e90",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandOnlineDDLForceCutOver,
	}
	OnlineDDLShow = &cobra.Command{
		Use:   "show",
		Short: "Display information about online DDL operations.",
		Example: `OnlineDDL show test_keyspace 82fa54ac_e83e_11ea_96b7_f875a4d24e90
OnlineDDL show test_keyspace all
OnlineDDL show --order descending test_keyspace all
OnlineDDL show --limit 10 test_keyspace all
OnlineDDL show --skip 5 --limit 10 test_keyspace all
OnlineDDL show test_keyspace running
OnlineDDL show test_keyspace complete
OnlineDDL show test_keyspace failed`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.RangeArgs(1, 2),
		RunE:                  commandOnlineDDLShow,
	}
)

// analyzeOnlineDDLCommandWithUuidOrAllArgument is a general helper function for OnlineDDL commands that
// accept either a valid UUID or the "all" argument.
func analyzeOnlineDDLCommandWithUuidOrAllArgument(cmd *cobra.Command) (keyspace, uuid string, err error) {
	keyspace = cmd.Flags().Arg(0)
	uuid = cmd.Flags().Arg(1)

	switch {
	case strings.ToLower(uuid) == AllMigrationsIndicator:
	case schema.IsOnlineDDLUUID(uuid):
	default:
		return "", "", fmt.Errorf("argument must be 'all' or a valid UUID. Got '%s'", uuid)
	}
	return keyspace, uuid, nil
}

func commandOnlineDDLCancel(cmd *cobra.Command, args []string) error {
	keyspace, uuid, err := analyzeOnlineDDLCommandWithUuidOrAllArgument(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	resp, err := client.CancelSchemaMigration(commandCtx, &vtctldatapb.CancelSchemaMigrationRequest{
		Keyspace: keyspace,
		Uuid:     uuid,
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

func commandOnlineDDLCleanup(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)
	uuid := cmd.Flags().Arg(1)
	if !schema.IsOnlineDDLUUID(uuid) {
		return fmt.Errorf("%s is not a valid UUID", uuid)
	}

	cli.FinishedParsing(cmd)

	resp, err := client.CleanupSchemaMigration(commandCtx, &vtctldatapb.CleanupSchemaMigrationRequest{
		Keyspace: keyspace,
		Uuid:     uuid,
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

func commandOnlineDDLForceCutOver(cmd *cobra.Command, args []string) error {
	keyspace, uuid, err := analyzeOnlineDDLCommandWithUuidOrAllArgument(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	resp, err := client.ForceCutOverSchemaMigration(commandCtx, &vtctldatapb.ForceCutOverSchemaMigrationRequest{
		Keyspace: keyspace,
		Uuid:     uuid,
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

func commandOnlineDDLComplete(cmd *cobra.Command, args []string) error {
	keyspace, uuid, err := analyzeOnlineDDLCommandWithUuidOrAllArgument(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	resp, err := client.CompleteSchemaMigration(commandCtx, &vtctldatapb.CompleteSchemaMigrationRequest{
		Keyspace: keyspace,
		Uuid:     uuid,
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

func commandOnlineDDLLaunch(cmd *cobra.Command, args []string) error {
	keyspace, uuid, err := analyzeOnlineDDLCommandWithUuidOrAllArgument(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	resp, err := client.LaunchSchemaMigration(commandCtx, &vtctldatapb.LaunchSchemaMigrationRequest{
		Keyspace: keyspace,
		Uuid:     uuid,
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

func commandOnlineDDLRetry(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)
	uuid := cmd.Flags().Arg(1)
	if !schema.IsOnlineDDLUUID(uuid) {
		return fmt.Errorf("%s is not a valid UUID", uuid)
	}

	cli.FinishedParsing(cmd)

	resp, err := client.RetrySchemaMigration(commandCtx, &vtctldatapb.RetrySchemaMigrationRequest{
		Keyspace: keyspace,
		Uuid:     uuid,
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

// throttleCommandHelper is a helper function that implements the logic for both
// commandOnlineDDLThrottle and commandOnlineDDLUnthrottle ; the only difference between the two
// is the ThrottledApp *rule* sent in UpdateThrottlerConfigRequest.
// input: `throttleType`: true stands for "throttle", `false` stands for "unthrottle"
func throttleCommandHelper(cmd *cobra.Command, throttleType bool) error {
	keyspace, uuid, err := analyzeOnlineDDLCommandWithUuidOrAllArgument(cmd)
	if err != nil {
		return err
	}
	cli.FinishedParsing(cmd)

	var rule topodatapb.ThrottledAppRule
	if throttleType {
		rule.Ratio = throttle.DefaultThrottleRatio
		rule.ExpiresAt = protoutil.TimeToProto(time.Now().Add(throttle.DefaultAppThrottleDuration))
	} else {
		rule.Ratio = 0
		rule.ExpiresAt = protoutil.TimeToProto(time.Now())
	}

	if strings.ToLower(uuid) == AllMigrationsIndicator {
		rule.Name = throttlerapp.OnlineDDLName.String()
	} else {
		rule.Name = uuid
	}

	updateThrottlerConfigOptions := vtctldatapb.UpdateThrottlerConfigRequest{
		Keyspace:     keyspace,
		ThrottledApp: &rule,
	}
	resp, err := client.UpdateThrottlerConfig(commandCtx, &updateThrottlerConfigOptions)
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

// commandOnlineDDLThrottle throttles one or multiple migrations.
// As opposed to *most* OnlineDDL functions, this functionality does not end up calling a gRPC on tablets.
// Instead, it updates Keyspace and SrvKeyspace entries, on which the tablets listen.
func commandOnlineDDLThrottle(cmd *cobra.Command, args []string) error {
	return throttleCommandHelper(cmd, true)
}

// commandOnlineDDLUnthrottle unthrottles one or multiple migrations.
// As opposed to *most* OnlineDDL functions, this functionality does not end up calling a gRPC on tablets.
// Instead, it updates Keyspace and SrvKeyspace entries, on which the tablets listen.
func commandOnlineDDLUnthrottle(cmd *cobra.Command, args []string) error {
	return throttleCommandHelper(cmd, false)
}

var onlineDDLShowArgs = struct {
	JSON     bool
	OrderStr string
	Limit    uint64
	Skip     uint64
}{
	OrderStr: "ascending",
}

func commandOnlineDDLShow(cmd *cobra.Command, args []string) error {
	var order vtctldatapb.QueryOrdering
	switch strings.ToLower(onlineDDLShowArgs.OrderStr) {
	case "":
		order = vtctldatapb.QueryOrdering_NONE
	case "asc", "ascending":
		order = vtctldatapb.QueryOrdering_ASCENDING
	case "desc", "descending":
		order = vtctldatapb.QueryOrdering_DESCENDING
	default:
		return fmt.Errorf("invalid ordering %s (choices are 'asc', 'ascending', 'desc', 'descending')", onlineDDLShowArgs.OrderStr)
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetSchemaMigrationsRequest{
		Keyspace: cmd.Flags().Arg(0),
		Order:    order,
		Limit:    onlineDDLShowArgs.Limit,
		Skip:     onlineDDLShowArgs.Skip,
	}

	switch arg := cmd.Flags().Arg(1); arg {
	case "", "all":
	case "recent":
		req.Recent = protoutil.DurationToProto(7 * 24 * time.Hour)
	default:
		if status, err := schematools.ParseSchemaMigrationStatus(arg); err == nil {
			// Argument is a status name.
			req.Status = status
		} else if schema.IsOnlineDDLUUID(arg) {
			req.Uuid = arg
		} else {
			req.MigrationContext = arg
		}
	}

	resp, err := client.GetSchemaMigrations(commandCtx, req)
	if err != nil {
		return err
	}

	switch {
	case onlineDDLShowArgs.JSON:
		data, err := cli.MarshalJSON(resp)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", data)
	default:
		res, err := sqltypes.MarshalResult(schematools.MarshallableSchemaMigrations(resp.Migrations))
		if err != nil {
			return err
		}

		cli.WriteQueryResultTable(os.Stdout, res)
	}
	return nil
}

func init() {
	OnlineDDL.AddCommand(OnlineDDLCancel)
	OnlineDDL.AddCommand(OnlineDDLCleanup)
	OnlineDDL.AddCommand(OnlineDDLComplete)
	OnlineDDL.AddCommand(OnlineDDLLaunch)
	OnlineDDL.AddCommand(OnlineDDLRetry)
	OnlineDDL.AddCommand(OnlineDDLThrottle)
	OnlineDDL.AddCommand(OnlineDDLUnthrottle)
	OnlineDDL.AddCommand(OnlineDDLForceCutOver)

	OnlineDDLShow.Flags().BoolVar(&onlineDDLShowArgs.JSON, "json", false, "Output JSON instead of human-readable table.")
	OnlineDDLShow.Flags().StringVar(&onlineDDLShowArgs.OrderStr, "order", "asc", "Sort the results by `id` property of the Schema migration.")
	OnlineDDLShow.Flags().Uint64Var(&onlineDDLShowArgs.Limit, "limit", 0, "Limit number of rows returned in output.")
	OnlineDDLShow.Flags().Uint64Var(&onlineDDLShowArgs.Skip, "skip", 0, "Skip specified number of rows returned in output.")

	OnlineDDL.AddCommand(OnlineDDLShow)
	Root.AddCommand(OnlineDDL)
}
