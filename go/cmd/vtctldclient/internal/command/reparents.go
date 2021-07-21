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
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// EmergencyReparentShard makes an EmergencyReparent gRPC call to a vtctld.
	EmergencyReparentShard = &cobra.Command{
		Use:  "EmergencyReparentShard <keyspace/shard>",
		Args: cobra.ExactArgs(1),
		Long: "Reparents the shard to the new primary. Assumes the old primary is dead and not responding",
		RunE: commandEmergencyReparentShard,
	}
	// InitShardPrimary makes an InitShardPrimary gRPC call to a vtctld.
	InitShardPrimary = &cobra.Command{
		Use:  "InitShardPrimary <keyspace/shard> <primary alias>",
		Args: cobra.ExactArgs(2),
		RunE: commandInitShardPrimary,
	}
	// PlannedReparentShard makes a PlannedReparentShard gRPC call to a vtctld.
	PlannedReparentShard = &cobra.Command{
		Use:  "PlannedReparentShard <keyspace/shard>",
		Args: cobra.ExactArgs(1),
		Long: "string",
		RunE: commandPlannedReparentShard,
	}
	// ReparentTablet makes a ReparentTablet gRPC call to a vtctld.
	ReparentTablet = &cobra.Command{
		Use: "ReparentTablet <alias>",
		Long: "Reparent a tablet to the current primary in the shard. This only works if the current replica position " +
			"matches the last known reparent action.",
		Args: cobra.ExactArgs(1),
		RunE: commandReparentTablet,
	}
	// TabletExternallyReparented makes a TabletExternallyReparented gRPC call
	// to a vtctld.
	TabletExternallyReparented = &cobra.Command{
		Use:  "TabletExternallyReparented <alias>",
		Args: cobra.ExactArgs(1),
		RunE: commandTabletExternallyReparented,
	}
)

var emergencyReparentShardOptions = struct {
	Force                     bool
	WaitReplicasTimeout       time.Duration
	NewPrimaryAliasStr        string
	IgnoreReplicaAliasStrList []string
}{}

func commandEmergencyReparentShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	var (
		newPrimaryAlias      *topodatapb.TabletAlias
		ignoreReplicaAliases = make([]*topodatapb.TabletAlias, len(emergencyReparentShardOptions.IgnoreReplicaAliasStrList))
	)

	if emergencyReparentShardOptions.NewPrimaryAliasStr != "" {
		newPrimaryAlias, err = topoproto.ParseTabletAlias(emergencyReparentShardOptions.NewPrimaryAliasStr)
		if err != nil {
			return err
		}
	}

	for i, aliasStr := range emergencyReparentShardOptions.IgnoreReplicaAliasStrList {
		alias, err := topoproto.ParseTabletAlias(aliasStr)
		if err != nil {
			return err
		}

		ignoreReplicaAliases[i] = alias
	}

	cli.FinishedParsing(cmd)

	resp, err := client.EmergencyReparentShard(commandCtx, &vtctldatapb.EmergencyReparentShardRequest{
		Keyspace:            keyspace,
		Shard:               shard,
		NewPrimary:          newPrimaryAlias,
		IgnoreReplicas:      ignoreReplicaAliases,
		WaitReplicasTimeout: protoutil.DurationToProto(emergencyReparentShardOptions.WaitReplicasTimeout),
	})
	if err != nil {
		return err
	}

	for _, event := range resp.Events {
		fmt.Println(logutil.EventString(event))
	}

	return nil
}

var initShardPrimaryOptions = struct {
	WaitReplicasTimeout time.Duration
	Force               bool
}{}

func commandInitShardPrimary(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	tabletAlias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(1))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.InitShardPrimary(commandCtx, &vtctldatapb.InitShardPrimaryRequest{
		Keyspace:                keyspace,
		Shard:                   shard,
		PrimaryElectTabletAlias: tabletAlias,
		WaitReplicasTimeout:     protoutil.DurationToProto(initShardPrimaryOptions.WaitReplicasTimeout),
		Force:                   initShardPrimaryOptions.Force,
	})
	if err != nil {
		return err
	}

	for _, event := range resp.Events {
		log.Infof("%v", event)
	}

	return err
}

var plannedReparentShardOptions = struct {
	NewPrimaryAliasStr   string
	AvoidPrimaryAliasStr string
	WaitReplicasTimeout  time.Duration
}{}

func commandPlannedReparentShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	var (
		newPrimaryAlias   *topodatapb.TabletAlias
		avoidPrimaryAlias *topodatapb.TabletAlias
	)

	if plannedReparentShardOptions.NewPrimaryAliasStr != "" {
		newPrimaryAlias, err = topoproto.ParseTabletAlias(plannedReparentShardOptions.NewPrimaryAliasStr)
		if err != nil {
			return err
		}
	}

	if plannedReparentShardOptions.AvoidPrimaryAliasStr != "" {
		avoidPrimaryAlias, err = topoproto.ParseTabletAlias(plannedReparentShardOptions.AvoidPrimaryAliasStr)
		if err != nil {
			return err
		}
	}

	cli.FinishedParsing(cmd)

	resp, err := client.PlannedReparentShard(commandCtx, &vtctldatapb.PlannedReparentShardRequest{
		Keyspace:            keyspace,
		Shard:               shard,
		NewPrimary:          newPrimaryAlias,
		AvoidPrimary:        avoidPrimaryAlias,
		WaitReplicasTimeout: protoutil.DurationToProto(plannedReparentShardOptions.WaitReplicasTimeout),
	})
	if err != nil {
		return err
	}

	for _, event := range resp.Events {
		fmt.Println(logutil.EventString(event))
	}

	return nil
}

func commandReparentTablet(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	resp, err := client.ReparentTablet(commandCtx, &vtctldatapb.ReparentTabletRequest{
		Tablet: alias,
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

func commandTabletExternallyReparented(cmd *cobra.Command, args []string) error {
	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	resp, err := client.TabletExternallyReparented(commandCtx, &vtctldatapb.TabletExternallyReparentedRequest{
		Tablet: alias,
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
	EmergencyReparentShard.Flags().DurationVar(&emergencyReparentShardOptions.WaitReplicasTimeout, "wait-replicas-timeout", *topo.RemoteOperationTimeout, "Time to wait for replicas to catch up in reparenting.")
	EmergencyReparentShard.Flags().StringVar(&emergencyReparentShardOptions.NewPrimaryAliasStr, "new-primary", "", "Alias of a tablet that should be the new primary. If not specified, the vtctld will select the best candidate to promote.")
	EmergencyReparentShard.Flags().StringSliceVarP(&emergencyReparentShardOptions.IgnoreReplicaAliasStrList, "ignore-replicas", "i", nil, "Comma-separated, repeated list of replica tablet aliases to ignore during the emergency reparent.")
	Root.AddCommand(EmergencyReparentShard)

	InitShardPrimary.Flags().DurationVar(&initShardPrimaryOptions.WaitReplicasTimeout, "wait-replicas-timeout", 30*time.Second, "time to wait for replicas to catch up in reparenting")
	InitShardPrimary.Flags().BoolVar(&initShardPrimaryOptions.Force, "force", false, "will force the reparent even if the provided tablet is not a master or the shard master")
	Root.AddCommand(InitShardPrimary)

	PlannedReparentShard.Flags().DurationVar(&plannedReparentShardOptions.WaitReplicasTimeout, "wait-replicas-timeout", *topo.RemoteOperationTimeout, "Time to wait for replicas to catch up on replication both before and after reparenting.")
	PlannedReparentShard.Flags().StringVar(&plannedReparentShardOptions.NewPrimaryAliasStr, "new-primary", "", "Alias of a tablet that should be the new primary.")
	PlannedReparentShard.Flags().StringVar(&plannedReparentShardOptions.AvoidPrimaryAliasStr, "avoid-primary", "", "Alias of a tablet that should not be the primary; i.e. \"reparent to any other tablet if this one is the primary\".")
	Root.AddCommand(PlannedReparentShard)

	Root.AddCommand(ReparentTablet)
	Root.AddCommand(TabletExternallyReparented)
}
