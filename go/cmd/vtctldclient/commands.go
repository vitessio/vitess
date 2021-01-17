/*
Copyright 2020 The Vitess Authors.

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

package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	findAllShardsInKeyspaceCmd = &cobra.Command{
		Use:     "FindAllShardsInKeyspace keyspace",
		Aliases: []string{"findallshardsinkeyspace"},
		Args:    cobra.ExactArgs(1),
		RunE:    commandFindAllShardsInKeyspace,
	}
	getCellInfoNamesCmd = &cobra.Command{
		Use:  "GetCellInfoNames",
		Args: cobra.NoArgs,
		RunE: commandGetCellInfoNames,
	}
	getCellInfoCmd = &cobra.Command{
		Use:  "GetCellInfo cell",
		Args: cobra.ExactArgs(1),
		RunE: commandGetCellInfo,
	}
	getCellsAliasesCmd = &cobra.Command{
		Use:  "GetCellsAliases",
		Args: cobra.NoArgs,
		RunE: commandGetCellsAliases,
	}
	getKeyspaceCmd = &cobra.Command{
		Use:     "GetKeyspace keyspace",
		Aliases: []string{"getkeyspace"},
		Args:    cobra.ExactArgs(1),
		RunE:    commandGetKeyspace,
	}
	getKeyspacesCmd = &cobra.Command{
		Use:     "GetKeyspaces",
		Aliases: []string{"getkeyspaces"},
		Args:    cobra.NoArgs,
		RunE:    commandGetKeyspaces,
	}
	initShardPrimaryCmd = &cobra.Command{
		Use:  "InitShardPrimary",
		Args: cobra.ExactArgs(2),
		RunE: commandInitShardPrimary,
	}
)

func commandFindAllShardsInKeyspace(cmd *cobra.Command, args []string) error {
	ks := cmd.Flags().Arg(0)
	resp, err := client.FindAllShardsInKeyspace(commandCtx, &vtctldatapb.FindAllShardsInKeyspaceRequest{
		Keyspace: ks,
	})

	if err != nil {
		return err
	}

	data, err := MarshalJSON(resp)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

func commandGetCellInfoNames(cmd *cobra.Command, args []string) error {
	resp, err := client.GetCellInfoNames(commandCtx, &vtctldatapb.GetCellInfoNamesRequest{})
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", strings.Join(resp.Names, "\n"))

	return nil
}

func commandGetCellInfo(cmd *cobra.Command, args []string) error {
	cell := cmd.Flags().Arg(0)
	resp, err := client.GetCellInfo(commandCtx, &vtctldatapb.GetCellInfoRequest{Cell: cell})

	if err != nil {
		return err
	}

	data, err := MarshalJSON(resp.CellInfo)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandGetCellsAliases(cmd *cobra.Command, args []string) error {
	resp, err := client.GetCellsAliases(commandCtx, &vtctldatapb.GetCellsAliasesRequest{})
	if err != nil {
		return err
	}

	data, err := MarshalJSON(resp.Aliases)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandGetKeyspace(cmd *cobra.Command, args []string) error {
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
	resp, err := client.GetKeyspaces(commandCtx, &vtctldatapb.GetKeyspacesRequest{})
	if err != nil {
		return err
	}

	data, err := MarshalJSON(resp.Keyspaces)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var initShardPrimaryArgs = struct {
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

	resp, err := client.InitShardPrimary(commandCtx, &vtctldatapb.InitShardPrimaryRequest{
		Keyspace:                keyspace,
		Shard:                   shard,
		PrimaryElectTabletAlias: tabletAlias,
		WaitReplicasTimeout:     ptypes.DurationProto(initShardPrimaryArgs.WaitReplicasTimeout),
		Force:                   initShardPrimaryArgs.Force,
	})

	for _, event := range resp.Events {
		log.Infof("%v", event)
	}

	return err
}

func init() {
	rootCmd.AddCommand(findAllShardsInKeyspaceCmd)
	rootCmd.AddCommand(getCellInfoNamesCmd)
	rootCmd.AddCommand(getCellInfoCmd)
	rootCmd.AddCommand(getCellsAliasesCmd)
	rootCmd.AddCommand(getKeyspaceCmd)
	rootCmd.AddCommand(getKeyspacesCmd)

	initShardPrimaryCmd.Flags().DurationVar(&initShardPrimaryArgs.WaitReplicasTimeout, "wait-replicas-timeout", 30*time.Second, "time to wait for replicas to catch up in reparenting")
	initShardPrimaryCmd.Flags().BoolVar(&initShardPrimaryArgs.Force, "force", false, "will force the reparent even if the provided tablet is not a master or the shard master")
	rootCmd.AddCommand(initShardPrimaryCmd)
}
