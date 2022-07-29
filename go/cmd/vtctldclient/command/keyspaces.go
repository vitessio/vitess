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
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

var (
	// CreateKeyspace makes a CreateKeyspace gRPC call to a vtctld.
	CreateKeyspace = &cobra.Command{
		Use:   "CreateKeyspace <keyspace> [--force|-f] [--type KEYSPACE_TYPE] [--base-keyspace KEYSPACE --snapshot-timestamp TIME] [--served-from DB_TYPE:KEYSPACE ...]  [--durability-policy <policy_name>]",
		Short: "Creates the specified keyspace in the topology.",
		Long: `Creates the specified keyspace in the topology.
	
For a SNAPSHOT keyspace, the request must specify the name of a base keyspace,
as well as a snapshot time.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandCreateKeyspace,
	}
	// DeleteKeyspace makes a DeleteKeyspace gRPC call to a vtctld.
	DeleteKeyspace = &cobra.Command{
		Use:   "DeleteKeyspace [--recursive|-r] [--force|-f] <keyspace>",
		Short: "Deletes the specified keyspace from the topology.",
		Long: `Deletes the specified keyspace from the topology.

In recursive mode, it also recursively deletes all shards in the keyspace.
Otherwise, the keyspace must be empty (have no shards), or returns an error.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandDeleteKeyspace,
	}
	// FindAllShardsInKeyspace makes a FindAllShardsInKeyspace gRPC call to a vtctld.
	FindAllShardsInKeyspace = &cobra.Command{
		Use:                   "FindAllShardsInKeyspace <keyspace>",
		Short:                 "Returns a map of shard names to shard references for a given keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"findallshardsinkeyspace"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandFindAllShardsInKeyspace,
	}
	// GetKeyspace makes a GetKeyspace gRPC call to a vtctld.
	GetKeyspace = &cobra.Command{
		Use:                   "GetKeyspace <keyspace>",
		Short:                 "Returns information about the given keyspace from the topology.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"getkeyspace"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetKeyspace,
	}
	// GetKeyspaces makes a GetKeyspaces gRPC call to a vtctld.
	GetKeyspaces = &cobra.Command{
		Use:                   "GetKeyspaces",
		Short:                 "Returns information about every keyspace in the topology.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"getkeyspaces"},
		Args:                  cobra.NoArgs,
		RunE:                  commandGetKeyspaces,
	}
	// RemoveKeyspaceCell makes a RemoveKeyspaceCell gRPC call to a vtctld.
	RemoveKeyspaceCell = &cobra.Command{
		Use:                   "RemoveKeyspaceCell [--force|-f] [--recursive|-r] <keyspace> <cell>",
		Short:                 "Removes the specified cell from the Cells list for all shards in the specified keyspace (by calling RemoveShardCell on every shard). It also removes the SrvKeyspace for that keyspace in that cell.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandRemoveKeyspaceCell,
	}
	// SetKeyspaceDurabilityPolicy makes a SetKeyspaceDurabilityPolicy gRPC call to a vtcltd.
	SetKeyspaceDurabilityPolicy = &cobra.Command{
		Use:   "SetKeyspaceDurabilityPolicy [--durability-policy=policy_name] <keyspace name>",
		Short: "Sets the durability-policy used by the specified keyspace.",
		Long: `Sets the durability-policy used by the specified keyspace. 
Durability policy governs the durability of the keyspace by describing which tablets should be sending semi-sync acknowledgements to the primary.
Possible values include 'semi_sync', 'none' and others as dictated by registered plugins.

To set the durability policy of customer keyspace to semi_sync, you would use the following command:
SetKeyspaceDurabilityPolicy --durability-policy='semi_sync' customer`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandSetKeyspaceDurabilityPolicy,
	}
	// SetKeyspaceServedFrom makes a SetKeyspaceServedFrom gRPC call to a vtcltd.
	SetKeyspaceServedFrom = &cobra.Command{
		Use:                   "SetKeyspaceServedFrom [--source <keyspace>] [--remove] [--cells=<cells>] <keyspace> <tablet_type>",
		Short:                 "Updates the ServedFromMap for a keyspace manually. This command is intended for emergency fixes. This command does not rebuild the serving graph.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandSetKeyspaceServedFrom,
		Deprecated:            "and will soon be removed! Please use VReplication instead: https://vitess.io/docs/reference/vreplication",
	}
	// SetKeyspaceShardingInfo makes a SetKeyspaceShardingInfo gRPC call to a vtcltd.
	SetKeyspaceShardingInfo = &cobra.Command{
		Use:                   "SetKeyspaceShardingInfo [--force] <keyspace> [<column name> [<column type>]]",
		Short:                 "Updates the sharding information for a keyspace.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.RangeArgs(1, 3),
		RunE:                  commandSetKeyspaceShardingInfo,
		Deprecated:            "and will soon be removed! Please use VReplication instead: https://vitess.io/docs/reference/vreplication",
	}
	// ValidateSchemaKeyspace makes a ValidateSchemaKeyspace gRPC call to a vtctld.
	ValidateSchemaKeyspace = &cobra.Command{
		Use:                   "ValidateSchemaKeyspace [--exclude-tables=<exclude_tables>] [--include-views] [--skip-no-primary] [--include-vschema] <keyspace>",
		Short:                 "Validates that the schema on the primary tablet for shard 0 matches the schema on all other tablets in the keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"validateschemakeyspace"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandValidateSchemaKeyspace,
	}
	// ValidateVersionKeyspace makes a ValidateVersionKeyspace gRPC call to a vtctld.
	ValidateVersionKeyspace = &cobra.Command{
		Use:                   "ValidateVersionKeyspace <keyspace>",
		Short:                 "Validates that the version on the primary tablet of shard 0 matches all of the other tablets in the keyspace.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"validateversionkeyspace"},
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandValidateVersionKeyspace,
	}
)

var createKeyspaceOptions = struct {
	Force             bool
	AllowEmptyVSchema bool

	ShardingColumnName string
	ShardingColumnType cli.KeyspaceIDTypeFlag

	ServedFromsMap cli.StringMapValue

	KeyspaceType      cli.KeyspaceTypeFlag
	BaseKeyspace      string
	SnapshotTimestamp string
	DurabilityPolicy  string
}{
	KeyspaceType: cli.KeyspaceTypeFlag(topodatapb.KeyspaceType_NORMAL),
}

func commandCreateKeyspace(cmd *cobra.Command, args []string) error {
	name := cmd.Flags().Arg(0)

	switch topodatapb.KeyspaceType(createKeyspaceOptions.KeyspaceType) {
	case topodatapb.KeyspaceType_NORMAL, topodatapb.KeyspaceType_SNAPSHOT:
	default:
		return fmt.Errorf("invalid keyspace type passed to --type: %v", createKeyspaceOptions.KeyspaceType)
	}

	var snapshotTime *vttime.Time
	if topodatapb.KeyspaceType(createKeyspaceOptions.KeyspaceType) == topodatapb.KeyspaceType_SNAPSHOT {
		if createKeyspaceOptions.DurabilityPolicy != "none" {
			return errors.New("--durability-policy cannot be specified while creating a snapshot keyspace")
		}

		if createKeyspaceOptions.BaseKeyspace == "" {
			return errors.New("--base-keyspace is required for a snapshot keyspace")
		}

		if createKeyspaceOptions.SnapshotTimestamp == "" {
			return errors.New("--snapshot-timestamp is required for a snapshot keyspace")
		}

		t, err := time.Parse(time.RFC3339, createKeyspaceOptions.SnapshotTimestamp)
		if err != nil {
			return fmt.Errorf("cannot parse --snapshot-timestamp as RFC3339: %w", err)
		}

		if now := time.Now(); t.After(now) {
			return fmt.Errorf("--snapshot-time cannot be in the future; snapshot = %v, now = %v", t, now)
		}

		snapshotTime = logutil.TimeToProto(t)
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.CreateKeyspaceRequest{
		Name:               name,
		Force:              createKeyspaceOptions.Force,
		AllowEmptyVSchema:  createKeyspaceOptions.AllowEmptyVSchema,
		ShardingColumnName: createKeyspaceOptions.ShardingColumnName,
		ShardingColumnType: topodatapb.KeyspaceIdType(createKeyspaceOptions.ShardingColumnType),
		Type:               topodatapb.KeyspaceType(createKeyspaceOptions.KeyspaceType),
		BaseKeyspace:       createKeyspaceOptions.BaseKeyspace,
		SnapshotTime:       snapshotTime,
		DurabilityPolicy:   createKeyspaceOptions.DurabilityPolicy,
	}

	for n, v := range createKeyspaceOptions.ServedFromsMap.StringMapValue {
		tt, err := topo.ParseServingTabletType(n)
		if err != nil {
			return err
		}

		req.ServedFroms = append(req.ServedFroms, &topodatapb.Keyspace_ServedFrom{
			TabletType: tt,
			Keyspace:   v,
		})
	}

	resp, err := client.CreateKeyspace(commandCtx, req)
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Keyspace)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully created keyspace %s. Result:\n%s\n", name, data)

	return nil
}

var deleteKeyspaceOptions = struct {
	Recursive bool
	Force     bool
}{}

func commandDeleteKeyspace(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)
	_, err := client.DeleteKeyspace(commandCtx, &vtctldatapb.DeleteKeyspaceRequest{
		Keyspace:  ks,
		Recursive: deleteKeyspaceOptions.Recursive,
		Force:     deleteKeyspaceOptions.Force,
	})

	if err != nil {
		return fmt.Errorf("DeleteKeyspace(%v) error: %w; please check the topo", ks, err)
	}

	fmt.Printf("Successfully deleted keyspace %v.\n", ks)

	return nil
}

func commandFindAllShardsInKeyspace(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)
	resp, err := client.FindAllShardsInKeyspace(commandCtx, &vtctldatapb.FindAllShardsInKeyspaceRequest{
		Keyspace: ks,
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

func commandGetKeyspace(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)
	resp, err := client.GetKeyspace(commandCtx, &vtctldatapb.GetKeyspaceRequest{
		Keyspace: ks,
	})

	if err != nil {
		return err
	}

	fmt.Printf("%+v\n", resp.Keyspace)

	return nil
}

func commandGetKeyspaces(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetKeyspaces(commandCtx, &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Keyspaces)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var removeKeyspaceCellOptions = struct {
	Force     bool
	Recursive bool
}{}

func commandRemoveKeyspaceCell(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)
	cell := cmd.Flags().Arg(1)

	_, err := client.RemoveKeyspaceCell(commandCtx, &vtctldatapb.RemoveKeyspaceCellRequest{
		Keyspace:  keyspace,
		Cell:      cell,
		Force:     removeKeyspaceCellOptions.Force,
		Recursive: removeKeyspaceCellOptions.Recursive,
	})

	if err != nil {
		return err
	}

	fmt.Printf("Successfully removed keyspace %s from cell %s\n", keyspace, cell)

	return nil
}

var setKeyspaceDurabilityPolicyOptions = struct {
	DurabilityPolicy string
}{}

func commandSetKeyspaceDurabilityPolicy(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)
	cli.FinishedParsing(cmd)

	resp, err := client.SetKeyspaceDurabilityPolicy(commandCtx, &vtctldatapb.SetKeyspaceDurabilityPolicyRequest{
		Keyspace:         keyspace,
		DurabilityPolicy: setKeyspaceDurabilityPolicyOptions.DurabilityPolicy,
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

var setKeyspaceServedFromOptions = struct {
	Cells          []string
	SourceKeyspace string
	Remove         bool
}{}

func commandSetKeyspaceServedFrom(cmd *cobra.Command, args []string) error {
	keyspace := cmd.Flags().Arg(0)
	tabletType, err := topoproto.ParseTabletType(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.SetKeyspaceServedFrom(commandCtx, &vtctldatapb.SetKeyspaceServedFromRequest{
		Keyspace:       keyspace,
		TabletType:     tabletType,
		Cells:          setKeyspaceServedFromOptions.Cells,
		SourceKeyspace: setKeyspaceServedFromOptions.SourceKeyspace,
		Remove:         setKeyspaceServedFromOptions.Remove,
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

var setKeyspaceShardingInfoOptions = struct {
	Force bool
}{}

func commandSetKeyspaceShardingInfo(cmd *cobra.Command, args []string) error {
	var (
		keyspace   = cmd.Flags().Arg(0)
		columnName string
		columnType = topodatapb.KeyspaceIdType_UNSET
	)

	switch len(cmd.Flags().Args()) {
	case 1:
		// Nothing else to do; we set keyspace already above.
	case 2:
		columnName = cmd.Flags().Arg(1)
	case 3:
		var err error
		columnType, err = key.ParseKeyspaceIDType(cmd.Flags().Arg(2))
		if err != nil {
			return err
		}
	default:
		// This should be impossible due to cobra.RangeArgs, but we handle it
		// explicitly anyway.
		return fmt.Errorf("SetKeyspaceShardingInfo expects between 1 and 3 positional args; have %d", len(cmd.Flags().Args()))
	}

	isColumnNameSet := columnName != ""
	isColumnTypeSet := columnType != topodatapb.KeyspaceIdType_UNSET

	if (isColumnNameSet && !isColumnTypeSet) || (!isColumnNameSet && isColumnTypeSet) {
		return fmt.Errorf("both <column_name:%v> and <column_type:%v> must be set, or both must be unset", columnName, key.KeyspaceIDTypeString(columnType))
	}

	cli.FinishedParsing(cmd)

	resp, err := client.SetKeyspaceShardingInfo(commandCtx, &vtctldatapb.SetKeyspaceShardingInfoRequest{
		Keyspace:   keyspace,
		ColumnName: columnName,
		ColumnType: columnType,
		Force:      setKeyspaceShardingInfoOptions.Force,
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

var validateSchemaKeyspaceOptions = struct {
	ExcludeTables  []string
	IncludeViews   bool
	SkipNoPrimary  bool
	IncludeVSchema bool
}{}

func commandValidateSchemaKeyspace(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)
	resp, err := client.ValidateSchemaKeyspace(commandCtx, &vtctldatapb.ValidateSchemaKeyspaceRequest{
		Keyspace:       ks,
		ExcludeTables:  validateSchemaKeyspaceOptions.ExcludeTables,
		IncludeVschema: validateSchemaKeyspaceOptions.IncludeVSchema,
		SkipNoPrimary:  validateSchemaKeyspaceOptions.SkipNoPrimary,
		IncludeViews:   validateSchemaKeyspaceOptions.IncludeViews,
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

func commandValidateVersionKeyspace(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)
	resp, err := client.ValidateVersionKeyspace(commandCtx, &vtctldatapb.ValidateVersionKeyspaceRequest{
		Keyspace: ks,
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
	CreateKeyspace.Flags().BoolVarP(&createKeyspaceOptions.Force, "force", "f", false, "Proceeds even if the keyspace already exists. Does not overwrite the existing keyspace record.")
	CreateKeyspace.Flags().BoolVarP(&createKeyspaceOptions.AllowEmptyVSchema, "allow-empty-vschema", "e", false, "Allows a new keyspace to have no vschema.")
	CreateKeyspace.Flags().StringVar(&createKeyspaceOptions.ShardingColumnName, "sharding-column-name", "", "The column name to use for sharding operations.")
	CreateKeyspace.Flags().MarkDeprecated("sharding-column-name", "Specifying a sharding-column-name on the Keyspace is deprecated and will be removed in a future release.")
	CreateKeyspace.Flags().Var(&createKeyspaceOptions.ShardingColumnType, "sharding-column-type", "The type of the column to use for sharding operations.")
	CreateKeyspace.Flags().MarkDeprecated("sharding-column-type", "Specifying a sharding-column-type on the Keyspace is deprecated and will be removed in a future release.")
	CreateKeyspace.Flags().Var(&createKeyspaceOptions.ServedFromsMap, "served-from", "Specifies a set of db_type:keyspace pairs used to serve traffic for the keyspace.")
	CreateKeyspace.Flags().Var(&createKeyspaceOptions.KeyspaceType, "type", "The type of the keyspace.")
	CreateKeyspace.Flags().StringVar(&createKeyspaceOptions.BaseKeyspace, "base-keyspace", "", "The base keyspace for a snapshot keyspace.")
	CreateKeyspace.Flags().StringVar(&createKeyspaceOptions.SnapshotTimestamp, "snapshot-timestamp", "", "The snapshot time for a snapshot keyspace, as a timestamp in RFC3339 format.")
	CreateKeyspace.Flags().StringVar(&createKeyspaceOptions.DurabilityPolicy, "durability-policy", "none", "Type of durability to enforce for this keyspace. Default is none. Possible values include 'semi_sync' and others as dictated by registered plugins.")
	Root.AddCommand(CreateKeyspace)

	DeleteKeyspace.Flags().BoolVarP(&deleteKeyspaceOptions.Recursive, "recursive", "r", false, "Recursively delete all shards in the keyspace, and all tablets in those shards.")
	DeleteKeyspace.Flags().BoolVarP(&deleteKeyspaceOptions.Force, "force", "f", false, "Delete the keyspace even if it cannot be locked; this should only be used for cleanup operations.")
	Root.AddCommand(DeleteKeyspace)

	Root.AddCommand(FindAllShardsInKeyspace)
	Root.AddCommand(GetKeyspace)
	Root.AddCommand(GetKeyspaces)

	RemoveKeyspaceCell.Flags().BoolVarP(&removeKeyspaceCellOptions.Force, "force", "f", false, "Proceed even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data.")
	RemoveKeyspaceCell.Flags().BoolVarP(&removeKeyspaceCellOptions.Recursive, "recursive", "r", false, "Also delete all tablets in that cell beloning to the specified keyspace.")
	Root.AddCommand(RemoveKeyspaceCell)

	SetKeyspaceServedFrom.Flags().StringSliceVarP(&setKeyspaceServedFromOptions.Cells, "cells", "c", nil, "Cells to affect (comma-separated).")
	SetKeyspaceServedFrom.Flags().BoolVarP(&setKeyspaceServedFromOptions.Remove, "remove", "r", false, "If set, remove the ServedFrom record.")
	SetKeyspaceServedFrom.Flags().StringVar(&setKeyspaceServedFromOptions.SourceKeyspace, "source", "", "Specifies the source keyspace name.")
	Root.AddCommand(SetKeyspaceServedFrom)

	SetKeyspaceDurabilityPolicy.Flags().StringVar(&setKeyspaceDurabilityPolicyOptions.DurabilityPolicy, "durability-policy", "none", "Type of durability to enforce for this keyspace. Default is none. Other values include 'semi_sync' and others as dictated by registered plugins.")
	Root.AddCommand(SetKeyspaceDurabilityPolicy)

	SetKeyspaceShardingInfo.Flags().BoolVarP(&setKeyspaceShardingInfoOptions.Force, "force", "f", false, "Updates fields even if they are already set. Use caution before passing force to this command.")
	Root.AddCommand(SetKeyspaceShardingInfo)

	ValidateSchemaKeyspace.Flags().BoolVar(&validateSchemaKeyspaceOptions.IncludeViews, "include-views", false, "Includes views in compared schemas.")
	ValidateSchemaKeyspace.Flags().BoolVar(&validateSchemaKeyspaceOptions.IncludeVSchema, "include-vschema", false, "Includes VSchema validation in validation results.")
	ValidateSchemaKeyspace.Flags().BoolVar(&validateSchemaKeyspaceOptions.SkipNoPrimary, "skip-no-primary", false, "Skips validation on whether or not a primary exists in shards.")
	ValidateSchemaKeyspace.Flags().StringSliceVar(&validateSchemaKeyspaceOptions.ExcludeTables, "exclude-tables", []string{}, "Tables to exclude during schema comparison.")
	Root.AddCommand(ValidateSchemaKeyspace)

	Root.AddCommand(ValidateVersionKeyspace)
}
