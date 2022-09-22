/*
Copyright 2021 The Vitess Authors.

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
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo/topoproto"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// ChangeTabletType makes a ChangeTabletType gRPC call to a vtctld.
	ChangeTabletType = &cobra.Command{
		Use:   "ChangeTabletType [--dry-run] <alias> <tablet-type>",
		Short: "Changes the db type for the specified tablet, if possible.",
		Long: `Changes the db type for the specified tablet, if possible.

This command is used primarily to arrange replicas, and it will not convert a primary.
NOTE: This command automatically updates the serving graph.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandChangeTabletType,
	}
	// DeleteTablets makes a DeleteTablets gRPC call to a vtctld.
	DeleteTablets = &cobra.Command{
		Use:                   "DeleteTablets <alias> [ <alias> ... ]",
		Short:                 "Deletes tablet(s) from the topology.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(1),
		RunE:                  commandDeleteTablets,
	}
	// ExecuteHook makes an ExecuteHook gRPC call to a vtctld.
	ExecuteHook = &cobra.Command{
		Use:   "ExecuteHook <alias> <hook_name> [<param1=value1> ...]",
		Short: "Runs the specified hook on the given tablet.",
		Long: `Runs the specified hook on the given tablet.

A hook is an executable script that resides in the ${VTROOT}/vthook directory.
For ExecuteHook, this is on the tablet requested, not on the vtctld or the host
running the vtctldclient.

Any key-value pairs passed after the hook name will be passed as parameters to
the hook on the tablet.

Note: hook names may not contain slash (/) characters.
`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(2),
		RunE:                  commandExecuteHook,
	}
	// GetFullStatus makes a FullStatus gRPC call to a vttablet.
	GetFullStatus = &cobra.Command{
		Use:                   "GetFullStatus <alias>",
		Short:                 "Outputs a JSON structure that contains full status of MySQL including the replication information, semi-sync information, GTID information among others.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetFullStatus,
	}
	// GetPermissions makes a GetPermissions gRPC call to a vtctld.
	GetPermissions = &cobra.Command{
		Use:                   "GetPermissions <tablet_alias>",
		Short:                 "Displays the permissions for a tablet.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetPermissions,
	}
	// GetTablet makes a GetTablet gRPC call to a vtctld.
	GetTablet = &cobra.Command{
		Use:                   "GetTablet <alias>",
		Short:                 "Outputs a JSON structure that contains information about the tablet.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetTablet,
	}
	// GetTablets makes a GetTablets gRPC call to a vtctld.
	GetTablets = &cobra.Command{
		Use:   "GetTablets [--strict] [{--cell $c1 [--cell $c2 ...], --keyspace $ks [--shard $shard], --tablet-alias $alias}]",
		Short: "Looks up tablets according to filter criteria.",
		Long: `Looks up tablets according to the filter criteria.

If --tablet-alias is passed, none of the other filters (keyspace, shard, cell) may
be passed, and tablets are looked up by tablet alias only.

If --keyspace is passed, then all tablets in the keyspace are retrieved. The
--shard flag may also be passed to further narrow the set of tablets to that
<keyspace/shard>. Passing --shard without also passing --keyspace will fail.

Passing --cell limits the set of tablets to those in the specified cells. The
--cell flag accepts a CSV argument (e.g. --cell "c1,c2") and may be repeated
(e.g. --cell "c1" --cell "c2").

Valid output formats are "awk" and "json".`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandGetTablets,
	}
	// GetTabletVersion makes a GetVersion RPC to a vtctld.
	GetTabletVersion = &cobra.Command{
		Use:                   "GetTabletVersion <alias>",
		Short:                 "Print the version of a tablet from its debug vars.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"GetVersion"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetTabletVersion,
	}
	// PingTablet makes a PingTablet gRPC call to a vtctld.
	PingTablet = &cobra.Command{
		Use:                   "PingTablet <alias>",
		Short:                 "Checks that the specified tablet is awake and responding to RPCs. This command can be blocked by other in-flight operations.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandPingTablet,
	}
	// RefreshState makes a RefreshState gRPC call to a vtctld.
	RefreshState = &cobra.Command{
		Use:                   "RefreshState <alias>",
		Short:                 "Reloads the tablet record on the specified tablet.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandRefreshState,
	}
	// RefreshStateByShard makes a RefreshStateByShard gRPC call to a vtcld.
	RefreshStateByShard = &cobra.Command{
		Use:                   "RefreshStateByShard [--cell <cell1> ...] <keyspace/shard>",
		Short:                 "Reloads the tablet record all tablets in the shard, optionally limited to the specified cells.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandRefreshStateByShard,
	}
	// RunHealthCheck makes a RunHealthCheck gRPC call to a vtctld.
	RunHealthCheck = &cobra.Command{
		Use:                   "RunHealthCheck <tablet_alias>",
		Short:                 "Runs a healthcheck on the remote tablet.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"RunHealthcheck"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandRunHealthCheck,
	}
	// SetWritable makes a SetWritable gRPC call to a vtctld.
	SetWritable = &cobra.Command{
		Use:                   "SetWritable <alias> <true/false>",
		Short:                 "Sets the specified tablet as writable or read-only.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandSetWritable,
	}
	// SleepTablet makes a SleepTablet gRPC call to a vtctld.
	SleepTablet = &cobra.Command{
		Use:   "SleepTablet <alias> <duration>",
		Short: "Blocks the action queue on the specified tablet for the specified amount of time. This is typically used for testing.",
		Long: `SleepTablet <alias> <duration>

Blocks the action queue on the specified tablet for the specified duration.
This command is typically only used for testing.
		
The duration is the amount of time that the action queue should be blocked.
The value is a string that contains a possibly signed sequence of decimal numbers,
each with optional fraction and a unit suffix, such as “300ms” or “1h45m”.
See the definition of the Go language’s ParseDuration[1] function for more details.
Note that, in the SleepTablet implementation, the value should be positively-signed.

[1]: https://pkg.go.dev/time#ParseDuration
`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandSleepTablet,
	}
	// StartReplication makes a StartReplication gRPC call to a vtctld.
	StartReplication = &cobra.Command{
		Use:                   "StartReplication <alias>",
		Short:                 "Starts replication on the specified tablet.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandStartReplication,
	}
	// StopReplication makes a StopReplication gRPC call to a vtctld.
	StopReplication = &cobra.Command{
		Use:                   "StopReplication <alias>",
		Short:                 "Stops replication on the specified tablet.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandStopReplication,
	}
)

var changeTabletTypeOptions = struct {
	DryRun bool
}{}

func commandChangeTabletType(cmd *cobra.Command, args []string) error {
	aliasStr := cmd.Flags().Arg(0)
	typeStr := cmd.Flags().Arg(1)

	alias, err := topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		return err
	}

	newType, err := topoproto.ParseTabletType(typeStr)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.ChangeTabletType(commandCtx, &vtctldatapb.ChangeTabletTypeRequest{
		TabletAlias: alias,
		DbType:      newType,
		DryRun:      changeTabletTypeOptions.DryRun,
	})
	if err != nil {
		return err
	}

	if resp.WasDryRun {
		fmt.Println("--- DRY RUN ---")
	}

	fmt.Printf("- %v\n", cli.MarshalTabletAWK(resp.BeforeTablet))
	fmt.Printf("+ %v\n", cli.MarshalTabletAWK(resp.AfterTablet))

	return nil
}

var deleteTabletsOptions = struct {
	AllowPrimary bool
}{}

func commandDeleteTablets(cmd *cobra.Command, args []string) error {
	aliases, err := cli.TabletAliasesFromPosArgs(cmd.Flags().Args())
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.DeleteTablets(commandCtx, &vtctldatapb.DeleteTabletsRequest{
		TabletAliases: aliases,
		AllowPrimary:  deleteTabletsOptions.AllowPrimary,
	})

	if err != nil {
		return fmt.Errorf("%w: while deleting %d tablets; please inspect the topo", err, len(aliases))
	}

	fmt.Printf("Successfully deleted %d tablets\n", len(aliases))

	return nil
}

func commandExecuteHook(cmd *cobra.Command, args []string) error {
	tabletAlias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	hookName := cmd.Flags().Arg(1)
	if strings.Contains(hookName, "/") {
		return fmt.Errorf("cannot execute hook named %s, ExecuteHook does not support hook names with slashes ('/')", hookName)
	}

	cli.FinishedParsing(cmd)

	hookParams := cmd.Flags().Args()[2:]
	resp, err := client.ExecuteHook(commandCtx, &vtctldatapb.ExecuteHookRequest{
		TabletAlias: tabletAlias,
		TabletHookRequest: &tabletmanagerdatapb.ExecuteHookRequest{
			Name:       hookName,
			Parameters: hookParams,
		},
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

func commandGetFullStatus(cmd *cobra.Command, args []string) error {
	aliasStr := cmd.Flags().Arg(0)
	alias, err := topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetFullStatus(commandCtx, &vtctldatapb.GetFullStatusRequest{TabletAlias: alias})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Status)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandGetPermissions(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetPermissions(commandCtx, &vtctldatapb.GetPermissionsRequest{
		TabletAlias: alias,
	})
	if err != nil {
		return err
	}
	p, err := cli.MarshalJSON(resp.Permissions)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", p)

	return nil
}

func commandGetTablet(cmd *cobra.Command, args []string) error {
	aliasStr := cmd.Flags().Arg(0)
	alias, err := topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetTablet(commandCtx, &vtctldatapb.GetTabletRequest{TabletAlias: alias})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Tablet)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var getTabletsOptions = struct {
	Cells    []string
	Keyspace string
	Shard    string

	TabletAliasStrings []string

	Format string
	Strict bool
}{}

func commandGetTablets(cmd *cobra.Command, args []string) error {
	format := strings.ToLower(getTabletsOptions.Format)

	switch format {
	case "awk", "json":
	default:
		return fmt.Errorf("invalid output format, got %s", getTabletsOptions.Format)
	}

	var aliases []*topodatapb.TabletAlias

	if len(getTabletsOptions.TabletAliasStrings) > 0 {
		switch {
		case getTabletsOptions.Keyspace != "":
			return fmt.Errorf("--keyspace (= %s) cannot be passed when using --tablet-alias (= %v)", getTabletsOptions.Keyspace, getTabletsOptions.TabletAliasStrings)
		case getTabletsOptions.Shard != "":
			return fmt.Errorf("--shard (= %s) cannot be passed when using --tablet-alias (= %v)", getTabletsOptions.Shard, getTabletsOptions.TabletAliasStrings)
		case len(getTabletsOptions.Cells) > 0:
			return fmt.Errorf("--cell (= %v) cannot be passed when using --tablet-alias (= %v)", getTabletsOptions.Cells, getTabletsOptions.TabletAliasStrings)
		}

		var err error
		aliases, err = cli.TabletAliasesFromPosArgs(getTabletsOptions.TabletAliasStrings)
		if err != nil {
			return err
		}
	}

	if getTabletsOptions.Keyspace == "" && getTabletsOptions.Shard != "" {
		return fmt.Errorf("--shard (= %s) cannot be passed without also passing --keyspace", getTabletsOptions.Shard)
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetTablets(commandCtx, &vtctldatapb.GetTabletsRequest{
		TabletAliases: aliases,
		Cells:         getTabletsOptions.Cells,
		Keyspace:      getTabletsOptions.Keyspace,
		Shard:         getTabletsOptions.Shard,
		Strict:        getTabletsOptions.Strict,
	})
	if err != nil {
		return err
	}

	switch format {
	case "awk":
		for _, t := range resp.Tablets {
			fmt.Println(cli.MarshalTabletAWK(t))
		}
	case "json":
		data, err := cli.MarshalJSON(resp.Tablets)
		if err != nil {
			return err
		}

		fmt.Printf("%s\n", data)
	}

	return nil
}

func commandGetTabletVersion(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetVersion(commandCtx, &vtctldatapb.GetVersionRequest{
		TabletAlias: alias,
	})
	if err != nil {
		return err
	}

	fmt.Println(resp.Version)
	return nil
}

func commandPingTablet(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.PingTablet(commandCtx, &vtctldatapb.PingTabletRequest{
		TabletAlias: alias,
	})
	return err
}

func commandRefreshState(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.RefreshState(commandCtx, &vtctldatapb.RefreshStateRequest{
		TabletAlias: alias,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Refreshed state on %s\n", topoproto.TabletAliasString(alias))
	return nil
}

var refreshStateByShardOptions = struct {
	Cells []string
}{}

func commandRefreshStateByShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.RefreshStateByShard(commandCtx, &vtctldatapb.RefreshStateByShardRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Cells:    refreshStateByShardOptions.Cells,
	})
	if err != nil {
		return err
	}

	msg := &strings.Builder{}
	msg.WriteString(fmt.Sprintf("Refreshed state on %s/%s", keyspace, shard))
	if len(refreshStateByShardOptions.Cells) > 0 {
		msg.WriteString(fmt.Sprintf(" in cells %s", strings.Join(refreshStateByShardOptions.Cells, ", ")))
	}
	msg.WriteByte('\n')
	if resp.IsPartialRefresh {
		msg.WriteString("State refresh was partial; some tablets in the shard may not have succeeded.\n")
	}

	fmt.Print(msg.String())
	return nil
}

func commandRunHealthCheck(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.RunHealthCheck(commandCtx, &vtctldatapb.RunHealthCheckRequest{
		TabletAlias: alias,
	})
	return err
}

func commandSetWritable(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	isWritable, err := strconv.ParseBool(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.SetWritable(commandCtx, &vtctldatapb.SetWritableRequest{
		TabletAlias: alias,
		Writable:    isWritable,
	})
	return err
}

func commandSleepTablet(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	duration, err := time.ParseDuration(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.SleepTablet(commandCtx, &vtctldatapb.SleepTabletRequest{
		TabletAlias: alias,
		Duration:    protoutil.DurationToProto(duration),
	})
	return err
}

func commandStartReplication(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.StartReplication(commandCtx, &vtctldatapb.StartReplicationRequest{
		TabletAlias: alias,
	})
	return err
}

func commandStopReplication(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.StopReplication(commandCtx, &vtctldatapb.StopReplicationRequest{
		TabletAlias: alias,
	})
	return err
}

func init() {
	ChangeTabletType.Flags().BoolVarP(&changeTabletTypeOptions.DryRun, "dry-run", "d", false, "Shows the proposed change without actually executing it.")
	Root.AddCommand(ChangeTabletType)

	DeleteTablets.Flags().BoolVarP(&deleteTabletsOptions.AllowPrimary, "allow-primary", "p", false, "Allow the primary tablet of a shard to be deleted. Use with caution.")
	Root.AddCommand(DeleteTablets)

	Root.AddCommand(ExecuteHook)
	Root.AddCommand(GetFullStatus)
	Root.AddCommand(GetPermissions)
	Root.AddCommand(GetTablet)

	GetTablets.Flags().StringSliceVarP(&getTabletsOptions.TabletAliasStrings, "tablet-alias", "t", nil, "List of tablet aliases to filter by.")
	GetTablets.Flags().StringSliceVarP(&getTabletsOptions.Cells, "cell", "c", nil, "List of cells to filter tablets by.")
	GetTablets.Flags().StringVarP(&getTabletsOptions.Keyspace, "keyspace", "k", "", "Keyspace to filter tablets by.")
	GetTablets.Flags().StringVarP(&getTabletsOptions.Shard, "shard", "s", "", "Shard to filter tablets by.")
	GetTablets.Flags().StringVar(&getTabletsOptions.Format, "format", "awk", "Output format to use; valid choices are (json, awk).")
	GetTablets.Flags().BoolVar(&getTabletsOptions.Strict, "strict", false, "Require all cells to return successful tablet data. Without --strict, tablet listings may be partial.")
	Root.AddCommand(GetTablets)

	Root.AddCommand(GetTabletVersion)
	Root.AddCommand(PingTablet)
	Root.AddCommand(RefreshState)

	RefreshStateByShard.Flags().StringSliceVarP(&refreshStateByShardOptions.Cells, "cells", "c", nil, "If specified, only call RefreshState on tablets in the specified cells. If empty, all cells are considered.")
	Root.AddCommand(RefreshStateByShard)

	Root.AddCommand(RunHealthCheck)
	Root.AddCommand(SetWritable)
	Root.AddCommand(SleepTablet)
	Root.AddCommand(StartReplication)
	Root.AddCommand(StopReplication)
}
