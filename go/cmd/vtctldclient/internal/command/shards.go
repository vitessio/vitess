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

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// CreateShard makes a CreateShard gRPC request to a vtctld.
	CreateShard = &cobra.Command{
		Use:  "CreateShard <keyspace/shard>",
		Args: cobra.ExactArgs(1),
		RunE: commandCreateShard,
	}
	// DeleteShards makes a DeleteShards gRPC request to a vtctld.
	DeleteShards = &cobra.Command{
		Use:  "DeleteShards <keyspace/shard> [<keyspace/shard> ...]",
		Args: cobra.MinimumNArgs(1),
		RunE: commandDeleteShards,
	}
	// GetShard makes a GetShard gRPC request to a vtctld.
	GetShard = &cobra.Command{
		Use:  "GetShard <keyspace/shard>",
		Args: cobra.ExactArgs(1),
		RunE: commandGetShard,
	}
	// RemoveShardCell makes a RemoveShardCell gRPC request to a vtctld.
	RemoveShardCell = &cobra.Command{
		Use:  "RemoveShardCell <keyspace/shard> <cell>",
		Args: cobra.ExactArgs(2),
		RunE: commandRemoveShardCell,
	}
	// ShardReplicationPositions makes a ShardReplicationPositions gRPC request
	// to a vtctld.
	ShardReplicationPositions = &cobra.Command{
		Use: "ShardReplicationPositions <keyspace/shard>",
		Long: `Shows the replication status of each tablet in the shard graph.
Output is sorted by tablet type, then replication position.
Use ctrl-C to interrupt the command and see partial results if needed.`,
		Args: cobra.ExactArgs(1),
		RunE: commandShardReplicationPositions,
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
			line = cli.MarshalTabletAWK(rt.Tablet) + fmt.Sprintf(" %v %v", rt.Status.Position, rt.Status.SecondsBehindMaster)
		}

		fmt.Println(line)
	}

	return nil
}

func init() {
	CreateShard.Flags().BoolVarP(&createShardOptions.Force, "force", "f", false, "")
	CreateShard.Flags().BoolVarP(&createShardOptions.IncludeParent, "include-parent", "p", false, "")
	Root.AddCommand(CreateShard)

	DeleteShards.Flags().BoolVarP(&deleteShardsOptions.Recursive, "recursive", "r", false, "Also delete all tablets belonging to the shard. This is required to delete a non-empty shard.")
	DeleteShards.Flags().BoolVarP(&deleteShardsOptions.EvenIfServing, "even-if-serving", "f", false, "Remove the shard even if it is serving. Use with caution.")
	Root.AddCommand(DeleteShards)

	Root.AddCommand(GetShard)

	RemoveShardCell.Flags().BoolVarP(&removeShardCellOptions.Force, "force", "f", false, "Proceed even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data.")
	RemoveShardCell.Flags().BoolVarP(&removeShardCellOptions.Recursive, "recursive", "r", false, "Also delete all tablets in that cell beloning to the specified shard.")
	Root.AddCommand(RemoveShardCell)

	Root.AddCommand(ShardReplicationPositions)
}
