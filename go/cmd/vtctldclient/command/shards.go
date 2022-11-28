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

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// CreateShard makes a CreateShard gRPC request to a vtctld.
	CreateShard = &cobra.Command{
		Use:                   "CreateShard [--force|-f] [--include-parent|-p] <keyspace/shard>",
		Short:                 "Creates the specified shard in the topology.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandCreateShard,
	}
	// DeleteShards makes a DeleteShards gRPC request to a vtctld.
	DeleteShards = &cobra.Command{
		Use:   "DeleteShards [--recursive|-r] [--even-if-serving] [--force|-f] <keyspace/shard> [<keyspace/shard> ...]",
		Short: "Deletes the specified shards from the topology.",
		Long: `Deletes the specified shards from the topology.

In recursive mode, it also deletes all tablets belonging to the shard.
Otherwise, the shard must be empty (have no tablets) or returns an error for
that shard.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(1),
		RunE:                  commandDeleteShards,
	}
	// GenerateShardRanges outputs a set of shard ranges assuming a (mostly)
	// equal distribution of N shards.
	GenerateShardRanges = &cobra.Command{
		Use:                   "GenerateShardRanges <num_shards>",
		Short:                 "Print a set of shard ranges assuming a keyspace with N shards.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			n, err := strconv.ParseInt(cmd.Flags().Arg(0), 10, 64)
			if err != nil {
				return err
			}

			cli.FinishedParsing(cmd)

			shards, err := key.GenerateShardRanges(int(n))
			if err != nil {
				return err
			}

			data, err := cli.MarshalJSON(shards)
			if err != nil {
				return err
			}

			fmt.Printf("%s\n", data)
			return nil
		},
		Annotations: map[string]string{
			skipClientCreationKey: "true",
		},
	}
	// GetShard makes a GetShard gRPC request to a vtctld.
	GetShard = &cobra.Command{
		Use:                   "GetShard <keyspace/shard>",
		Short:                 "Returns information about a shard in the topology.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetShard,
	}
	// RemoveShardCell makes a RemoveShardCell gRPC request to a vtctld.
	RemoveShardCell = &cobra.Command{
		Use:                   "RemoveShardCell [--force|-f] [--recursive|-r] <keyspace/shard> <cell>",
		Short:                 "Remove the specified cell from the specified shard's Cells list.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandRemoveShardCell,
	}
	// SetShardIsPrimaryServing makes a SetShardIsPrimaryServing gRPC call to a
	// vtctld.
	SetShardIsPrimaryServing = &cobra.Command{
		Use:                   "SetShardIsPrimaryServing <keyspace/shard> <true/false>",
		Short:                 "Add or remove a shard from serving. This is meant as an emergency function. It does not rebuild any serving graphs; i.e. it does not run `RebuildKeyspaceGraph`.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandSetShardIsPrimaryServing,
	}
	// SetShardTabletControl makes a SetShardTabletControl gRPC call to a vtctld.
	SetShardTabletControl = &cobra.Command{
		Use:   "SetShardTabletControl [--cells=c1,c2...] [--denied-tables=t1,t2,...] [--remove] [--disable-query-service[=0|false]] <keyspace/shard> <tablet_type>",
		Short: "Sets the TabletControl record for a shard and tablet type. Only use this for an emergency fix or after a finished MoveTables.",
		Long: `Sets the TabletControl record for a shard and tablet type.

Only use this for an emergency fix or after a finished MoveTables.

Always specify the denied-tables flag for MoveTables, but never for Reshard operations.

To set the DisableQueryService flag, keep denied-tables empty, and set --disable-query-service
to true or false. This is useful to fix Reshard operations gone wrong.

To change the list of denied tables, specify the --denied-tables parameter with
the new list. This is useful to fix tables that are being blocked after a
MoveTables operation.

To remove the ShardTabletControl record entirely, use the --remove flag. This is
useful after a MoveTables has finished to remove serving restrictions.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandSetShardTabletControl,
	}
	// ShardReplicationAdd makse a ShardReplicationAdd gRPC request to a vtctld.
	ShardReplicationAdd = &cobra.Command{
		Use:                   "ShardReplicationAdd <keyspace/shard> <tablet alias>",
		Short:                 "Adds an entry to the replication graph in the given cell.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandShardReplicationAdd,
		Hidden:                true,
	}
	// ShardReplicationFix makes a ShardReplicationFix gRPC request to a vtctld.
	ShardReplicationFix = &cobra.Command{
		Use:                   "ShardReplicationFix <cell> <keyspace/shard>",
		Short:                 "Walks through a ShardReplication object and fixes the first error encountered.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandShardReplicationFix,
	}
	// ShardReplicationPositions makes a ShardReplicationPositions gRPC request
	// to a vtctld.
	ShardReplicationPositions = &cobra.Command{
		Use: "ShardReplicationPositions <keyspace/shard>",
		Long: `Shows the replication status of each tablet in the shard graph.
Output is sorted by tablet type, then replication position.
Use ctrl-C to interrupt the command and see partial results if needed.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandShardReplicationPositions,
	}
	// ShardReplicationRemove makse a ShardReplicationRemove gRPC request to a vtctld.
	ShardReplicationRemove = &cobra.Command{
		Use:                   "ShardReplicationRemove <keyspace/shard> <tablet alias>",
		Short:                 "Removes an entry from the replication graph in the given cell.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandShardReplicationRemove,
		Hidden:                true,
	}
	// SourceShardAdd makes a SourceShardAdd gRPC request to a vtctld.
	SourceShardAdd = &cobra.Command{
		Use:                   "SourceShardAdd [--key-range <keyrange>] [--tables <table1,table2,...> [--tables <table3,...>]...] <keyspace/shard> <uid> <source keyspace/shard>",
		Short:                 "Adds the SourceShard record with the provided index for emergencies only. It does not call RefreshState for the shard primary.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(3),
		RunE:                  commandSourceShardAdd,
	}
	// SourceShardDelete makes a SourceShardDelete gRPC request to a vtctld.
	SourceShardDelete = &cobra.Command{
		Use:                   "SourceShardDelete <keyspace/shard> <uid>",
		Short:                 "Deletes the SourceShard record with the provided index. This should only be used for emergency cleanup. It does not call RefreshState for the shard primary.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		RunE:                  commandSourceShardDelete,
	}

	// ValidateVersionShard makes a ValidateVersionShard gRPC request to a vtctld.
	ValidateVersionShard = &cobra.Command{
		Use:                   "ValidateVersionShard <keyspace/shard>",
		Short:                 "Validates that the version on the primary matches all of the replicas.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandValidateVersionShard,
	}
)

var createShardOptions = struct {
	Force         bool
	IncludeParent bool
}{}

func commandCreateShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.CreateShard(commandCtx, &vtctldatapb.CreateShardRequest{
		Keyspace:      keyspace,
		ShardName:     shard,
		Force:         createShardOptions.Force,
		IncludeParent: createShardOptions.IncludeParent,
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

var deleteShardsOptions = struct {
	Recursive     bool
	EvenIfServing bool
	Force         bool
}{}

func commandDeleteShards(cmd *cobra.Command, args []string) error {
	shards, err := cli.ParseKeyspaceShards(cmd.Flags().Args())
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.DeleteShards(commandCtx, &vtctldatapb.DeleteShardsRequest{
		Shards:        shards,
		EvenIfServing: deleteShardsOptions.EvenIfServing,
		Recursive:     deleteShardsOptions.Recursive,
		Force:         deleteShardsOptions.Force,
	})

	if err != nil {
		return fmt.Errorf("%w: while deleting %d shards; please inspect the topo", err, len(shards))
	}

	fmt.Printf("Successfully deleted %d shards\n", len(shards))

	return nil
}

func commandGetShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetShard(commandCtx, &vtctldatapb.GetShardRequest{
		Keyspace:  keyspace,
		ShardName: shard,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Shard)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var removeShardCellOptions = struct {
	Force     bool
	Recursive bool
}{}

func commandRemoveShardCell(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	cell := cmd.Flags().Arg(1)

	_, err = client.RemoveShardCell(commandCtx, &vtctldatapb.RemoveShardCellRequest{
		Keyspace:  keyspace,
		ShardName: shard,
		Cell:      cell,
		Force:     removeShardCellOptions.Force,
		Recursive: removeShardCellOptions.Recursive,
	})

	if err != nil {
		return err
	}

	fmt.Printf("Successfully removed cell %v from shard %s/%s\n", cell, keyspace, shard)

	return nil
}

func commandSetShardIsPrimaryServing(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return fmt.Errorf("cannot parse keyspace/shard: %w", err)
	}

	isServing, err := strconv.ParseBool(cmd.Flags().Arg(1))
	if err != nil {
		return fmt.Errorf("cannot parse is_serving as bool: %w", err)
	}

	cli.FinishedParsing(cmd)

	resp, err := client.SetShardIsPrimaryServing(commandCtx, &vtctldatapb.SetShardIsPrimaryServingRequest{
		Keyspace:  keyspace,
		Shard:     shard,
		IsServing: isServing,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Shard)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

var setShardTabletControlOptions = struct {
	Cells               []string
	DeniedTables        []string
	Remove              bool
	DisableQueryService bool
}{}

func commandSetShardTabletControl(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return fmt.Errorf("cannot parse keyspace/shard: %w", err)
	}

	tabletType, err := topoproto.ParseTabletType(cmd.Flags().Arg(1))
	if err != nil {
		return fmt.Errorf("cannot parse tablet type: %w", err)
	}

	cli.FinishedParsing(cmd)

	resp, err := client.SetShardTabletControl(commandCtx, &vtctldatapb.SetShardTabletControlRequest{
		Keyspace:            keyspace,
		Shard:               shard,
		TabletType:          tabletType,
		Cells:               setShardTabletControlOptions.Cells,
		DeniedTables:        setShardTabletControlOptions.DeniedTables,
		Remove:              setShardTabletControlOptions.Remove,
		DisableQueryService: setShardTabletControlOptions.DisableQueryService,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Shard)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

func commandShardReplicationAdd(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	tabletAlias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.ShardReplicationAdd(commandCtx, &vtctldatapb.ShardReplicationAddRequest{
		Keyspace:    keyspace,
		Shard:       shard,
		TabletAlias: tabletAlias,
	})
	return err
}

func commandShardReplicationFix(cmd *cobra.Command, args []string) error {
	cell := cmd.Flags().Arg(0)
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.ShardReplicationFix(commandCtx, &vtctldatapb.ShardReplicationFixRequest{
		Keyspace: keyspace,
		Shard:    shard,
		Cell:     cell,
	})
	if err != nil {
		return err
	}

	switch resp.Error {
	case nil:
		fmt.Println("All nodes in the replication graph are valid.")
	default:
		fmt.Printf("%s has been fixed for %s.\n", topoproto.ShardReplicationErrorTypeString(resp.Error.Type), topoproto.TabletAliasString(resp.Error.TabletAlias))
	}

	return nil
}

func commandShardReplicationPositions(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.ShardReplicationPositions(commandCtx, &vtctldatapb.ShardReplicationPositionsRequest{
		Keyspace: keyspace,
		Shard:    shard,
	})
	if err != nil {
		return err
	}

	for _, rt := range cli.SortedReplicatingTablets(resp.TabletMap, resp.ReplicationStatuses) {
		var line string

		switch rt.Status {
		case nil:
			line = cli.MarshalTabletAWK(rt.Tablet) + "<err> <err> <err>"
		default:
			line = cli.MarshalTabletAWK(rt.Tablet) + fmt.Sprintf(" %v %v", rt.Status.Position, rt.Status.ReplicationLagSeconds)
		}

		fmt.Println(line)
	}

	return nil
}

func commandShardReplicationRemove(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	tabletAlias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.ShardReplicationRemove(commandCtx, &vtctldatapb.ShardReplicationRemoveRequest{
		Keyspace:    keyspace,
		Shard:       shard,
		TabletAlias: tabletAlias,
	})
	return err
}

var sourceShardAddOptions = struct {
	KeyRangeStr string
	Tables      []string
}{}

func commandSourceShardAdd(cmd *cobra.Command, args []string) error {
	ks, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	uid, err := strconv.ParseUint(cmd.Flags().Arg(1), 10, 32)
	if err != nil {
		return fmt.Errorf("Failed to parse SourceShard uid: %w", err) // nolint
	}

	sks, sshard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(2))
	if err != nil {
		return err
	}

	var kr *topodatapb.KeyRange
	if sourceShardAddOptions.KeyRangeStr != "" {
		_, kr, err = topo.ValidateShardName(sourceShardAddOptions.KeyRangeStr)
		if err != nil {
			return fmt.Errorf("Invalid keyrange: %w", err)
		}
	}

	cli.FinishedParsing(cmd)

	resp, err := client.SourceShardAdd(commandCtx, &vtctldatapb.SourceShardAddRequest{
		Keyspace:       ks,
		Shard:          shard,
		Uid:            uint32(uid),
		SourceKeyspace: sks,
		SourceShard:    sshard,
		KeyRange:       kr,
		Tables:         sourceShardAddOptions.Tables,
	})
	if err != nil {
		return err
	}

	switch resp.Shard {
	case nil:
		fmt.Printf("SourceShard with uid %v already exists for %s/%s, not adding it.\n", uid, ks, shard)
	default:
		data, err := cli.MarshalJSON(resp.Shard)
		if err != nil {
			return err
		}

		fmt.Printf("Updated shard record:\n%s\n", data)
	}

	return nil
}

func commandSourceShardDelete(cmd *cobra.Command, args []string) error {
	ks, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	uid, err := strconv.ParseUint(cmd.Flags().Arg(1), 10, 32)
	if err != nil {
		return fmt.Errorf("Failed to parse SourceShard uid: %w", err) // nolint
	}

	cli.FinishedParsing(cmd)

	resp, err := client.SourceShardDelete(commandCtx, &vtctldatapb.SourceShardDeleteRequest{
		Keyspace: ks,
		Shard:    shard,
		Uid:      uint32(uid),
	})
	if err != nil {
		return err
	}

	switch resp.Shard {
	case nil:
		fmt.Printf("No SourceShard with uid %v.\n", uid)
	default:
		data, err := cli.MarshalJSON(resp.Shard)
		if err != nil {
			return err
		}

		fmt.Printf("Updated shard record:\n%s\n", data)
	}
	return nil
}

func commandValidateVersionShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.ValidateVersionShard(commandCtx, &vtctldatapb.ValidateVersionShardRequest{
		Keyspace: keyspace,
		Shard:    shard,
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
	CreateShard.Flags().BoolVarP(&createShardOptions.Force, "force", "f", false, "Overwrite an existing shard record, if one exists.")
	CreateShard.Flags().BoolVarP(&createShardOptions.IncludeParent, "include-parent", "p", false, "Creates the parent keyspace record if does not already exist.")
	Root.AddCommand(CreateShard)

	DeleteShards.Flags().BoolVarP(&deleteShardsOptions.Recursive, "recursive", "r", false, "Also delete all tablets belonging to the shard. This is required to delete a non-empty shard.")
	DeleteShards.Flags().BoolVar(&deleteShardsOptions.EvenIfServing, "even-if-serving", false, "Remove the shard even if it is serving. Use with caution.")
	DeleteShards.Flags().BoolVarP(&deleteShardsOptions.Force, "force", "f", false, "Remove the shard even if it cannot be locked; this should only be used for cleanup operations.")
	Root.AddCommand(DeleteShards)

	Root.AddCommand(GetShard)
	Root.AddCommand(GenerateShardRanges)

	RemoveShardCell.Flags().BoolVarP(&removeShardCellOptions.Force, "force", "f", false, "Proceed even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data.")
	RemoveShardCell.Flags().BoolVarP(&removeShardCellOptions.Recursive, "recursive", "r", false, "Also delete all tablets in that cell beloning to the specified shard.")
	Root.AddCommand(RemoveShardCell)

	Root.AddCommand(SetShardIsPrimaryServing)

	SetShardTabletControl.Flags().StringSliceVarP(&setShardTabletControlOptions.Cells, "cells", "c", nil, "Specifies a comma-separated list of cells to update.")
	SetShardTabletControl.Flags().StringSliceVar(&setShardTabletControlOptions.DeniedTables, "denied-tables", nil, "Specifies a comma-separated list of tables to add to the denylist (for MoveTables). Each table name is either an exact match, or a regular expression of the form '/regexp/'.")
	SetShardTabletControl.Flags().BoolVarP(&setShardTabletControlOptions.Remove, "remove", "r", false, "Removes the specified cells for MoveTables operations.")
	SetShardTabletControl.Flags().BoolVar(&setShardTabletControlOptions.DisableQueryService, "disable-query-service", false, "Sets the DisableQueryService flag in the specified cells. This flag requires --denied-tables and --remove to be unset; if either is set, this flag is ignored.")
	Root.AddCommand(SetShardTabletControl)

	Root.AddCommand(ShardReplicationAdd)
	Root.AddCommand(ShardReplicationFix)
	Root.AddCommand(ShardReplicationPositions)
	Root.AddCommand(ShardReplicationRemove)
	Root.AddCommand(ValidateVersionShard)

	SourceShardAdd.Flags().StringVar(&sourceShardAddOptions.KeyRangeStr, "key-range", "", "Key range to use for the SourceShard.")
	SourceShardAdd.Flags().StringSliceVar(&sourceShardAddOptions.Tables, "tables", nil, "Comma-separated lists of tables to replicate (for MoveTables). Each table name is either an exact match, or a regular expression of the form \"/regexp/\".")
	Root.AddCommand(SourceShardAdd)

	Root.AddCommand(SourceShardDelete)
}
