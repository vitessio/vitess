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

package command

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// MigrateReplicationMode makes a MigrateReplicationMode gRPC call to a vtctld.
var MigrateReplicationMode = &cobra.Command{
	Use:   "MigrateReplicationMode --durability-policy <policy> [--dry-run] [--wait-timeout <duration>] <keyspace>[/<shard>]",
	Short: "Converts the shards of a keyspace between asynchronous (semi-sync) replication and MySQL Group Replication, online.",
	Long: `Converts the shards of a keyspace between asynchronous (semi-sync) replication and MySQL Group Replication, online.

With a group replication durability policy (group_replication, group_replication_cross_cell), every shard
is converted under its shard lock: the group is bootstrapped on the primary, the voting replicas join one
at a time, and the other tablets keep replicating asynchronously from the primary. The keyspace durability
policy is updated once every shard of the keyspace runs Group Replication.

With any other durability policy, the keyspace durability policy is updated first, and then every shard
leaves its group: the secondaries first, each repointed to the primary, and the primary last.

Every step checks the current state first, so the command can be run again after a failure. With
--dry-run, the command prints the plan without changing anything.`,
	Example: `MigrateReplicationMode --durability-policy group_replication_cross_cell --dry-run commerce
MigrateReplicationMode --durability-policy group_replication commerce/-80
MigrateReplicationMode --durability-policy semi_sync commerce`,
	DisableFlagsInUseLine: true,
	Args:                  cobra.ExactArgs(1),
	RunE:                  commandMigrateReplicationMode,
}

var migrateReplicationModeOptions = struct {
	DurabilityPolicy string
	DryRun           bool
	WaitTimeout      time.Duration
}{}

func commandMigrateReplicationMode(cmd *cobra.Command, args []string) error {
	keyspace, shard := cmd.Flags().Arg(0), ""
	if strings.Contains(keyspace, "/") {
		var err error
		keyspace, shard, err = topoproto.ParseKeyspaceShard(keyspace)
		if err != nil {
			return err
		}
	}

	cli.FinishedParsing(cmd)

	resp, err := client.MigrateReplicationMode(commandCtx, &vtctldatapb.MigrateReplicationModeRequest{
		Keyspace:         keyspace,
		Shard:            shard,
		DurabilityPolicy: migrateReplicationModeOptions.DurabilityPolicy,
		DryRun:           migrateReplicationModeOptions.DryRun,
		WaitTimeout:      protoutil.DurationToProto(migrateReplicationModeOptions.WaitTimeout),
	})
	if resp != nil {
		for _, event := range resp.Events {
			fmt.Println(logutil.EventString(event))
		}
		resp.Events = nil
		data, jsonErr := cli.MarshalJSON(resp)
		if jsonErr != nil {
			return jsonErr
		}
		fmt.Printf("%s\n", data)
	}
	return err
}

func init() {
	MigrateReplicationMode.Flags().StringVar(&migrateReplicationModeOptions.DurabilityPolicy, "durability-policy", "", "The target durability policy. A group replication policy converts the shards to MySQL Group Replication; any other policy converts them back.")
	MigrateReplicationMode.Flags().BoolVar(&migrateReplicationModeOptions.DryRun, "dry-run", false, "Print the plan without changing anything.")
	MigrateReplicationMode.Flags().DurationVar(&migrateReplicationModeOptions.WaitTimeout, "wait-timeout", 5*time.Minute, "Maximum time to wait for each step, for example for a member to become ONLINE.")
	_ = MigrateReplicationMode.MarkFlagRequired("durability-policy")
	Root.AddCommand(MigrateReplicationMode)
}
