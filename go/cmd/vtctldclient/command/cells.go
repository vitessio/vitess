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
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	_ "vitess.io/vitess/go/vt/topo/consultopo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// AddCellInfo makes an AddCellInfo gRPC call to a vtctld.
	AddCellInfo = &cobra.Command{
		Use:   "AddCellInfo --root <root> [--server-address <addr>] <cell>",
		Short: "Registers a local topology service in a new cell by creating the CellInfo.",
		Long: fmt.Sprintf(`Registers a local topology service in a new cell by creating the CellInfo
with the provided parameters.

The address will be used to connect to the topology service, and Vitess data will
be stored starting at the provided root.

If the --boostrap flag is specified then you must specify a value of '%s' for --server
and also provide at least one topology server endpoint using --topology-servers so that
we do not attempt to connect to a remote vtctld server and vtctldclient can instead
connect directly to the topology server in order to create the cell that you can then
start vtctld in.`, useBundledVtctld),
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		PreRunE:               validateAddCellInfoFlags,
		RunE:                  commandAddCellInfo,
	}
	// AddCellsAlias makes an AddCellsAlias gRPC call to a vtctld.
	AddCellsAlias = &cobra.Command{
		Use:   "AddCellsAlias --cells <cell1,cell2,...> [--cells <cell3> ...] <alias>",
		Short: "Defines a group of cells that can be referenced by a single name (the alias).",
		Long: `Defines a group of cells that can be referenced by a single name (the alias).

When routing query traffic, replica/rdonly traffic can be routed across cells
within the group (alias). Only primary traffic can be routed across cells not in
the same group (alias).`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandAddCellsAlias,
	}
	// DeleteCellInfo makes a DeleteCellInfo gRPC call to a vtctld.
	DeleteCellInfo = &cobra.Command{
		Use:                   "DeleteCellInfo [--force] <cell>",
		Short:                 "Deletes the CellInfo for the provided cell.",
		Long:                  "Deletes the CellInfo for the provided cell. The cell cannot be referenced by any Shard record.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandDeleteCellInfo,
	}
	// DeleteCellsAlias makes a DeleteCellsAlias gRPC call to a vtctld.
	DeleteCellsAlias = &cobra.Command{
		Use:                   "DeleteCellsAlias <alias>",
		Short:                 "Deletes the CellsAlias for the provided alias.",
		Long:                  "Deletes the CellsAlias for the provided alias.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandDeleteCellsAlias,
	}
	// GetCellInfoNames makes a GetCellInfoNames gRPC call to a vtctld.
	GetCellInfoNames = &cobra.Command{
		Use:                   "GetCellInfoNames",
		Short:                 "Lists the names of all cells in the cluster.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandGetCellInfoNames,
	}
	// GetCellInfo makes a GetCellInfo gRPC call to a vtctld.
	GetCellInfo = &cobra.Command{
		Use:                   "GetCellInfo <cell>",
		Short:                 "Gets the CellInfo object for the given cell.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetCellInfo,
	}
	// GetCellsAliases makes a GetCellsAliases gRPC call to a vtctld.
	GetCellsAliases = &cobra.Command{
		Use:                   "GetCellsAliases",
		Short:                 "Gets all CellsAlias objects in the cluster.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandGetCellsAliases,
	}
	// UpdateCellInfo makes an UpdateCellInfo gRPC call to a vtctld.
	UpdateCellInfo = &cobra.Command{
		Use:   "UpdateCellInfo [--root <root>] [--server-address <addr>] <cell>",
		Short: "Updates the content of a CellInfo with the provided parameters, creating the CellInfo if it does not exist.",
		Long: `Updates the content of a CellInfo with the provided parameters, creating the CellInfo if it does not exist.

If a value is empty, it is ignored.`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandUpdateCellInfo,
	}
	// UpdateCellsAlias makes an UpdateCellsAlias gRPC call to a vtctld.
	UpdateCellsAlias = &cobra.Command{
		Use:                   "UpdateCellsAlias [--cells <cell1,cell2,...> [--cells <cell4> ...]] <alias>",
		Short:                 "Updates the content of a CellsAlias with the provided parameters, creating the CellsAlias if it does not exist.",
		Long:                  "Updates the content of a CellsAlias with the provided parameters, creating the CellsAlias if it does not exist.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandUpdateCellsAlias,
	}
)

var addCellInfoOptions = struct {
	cellInfo    topodatapb.CellInfo
	bootstrap   bool
	topoImpl    string
	topoRoot    string
	topoServers []string
}{}

func validateAddCellInfoFlags(cmd *cobra.Command, args []string) error {
	if addCellInfoOptions.bootstrap {
		// You must at least specify one topology server to bootstrap the cluster.
		if len(addCellInfoOptions.topoServers) == 0 {
			return fmt.Errorf("you must specify at least one topology server to bootstrap the cluster")
		}
		for _, topoServer := range addCellInfoOptions.topoServers {
			if _, _, err := net.SplitHostPort(topoServer); err != nil {
				return fmt.Errorf("invalid topology server address (%s): %v", topoServer, err)
			}
		}
	}

	return nil
}

func commandAddCellInfo(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	var (
		cell   = cmd.Flags().Arg(0)
		ctx    = commandCtx
		cancel context.CancelFunc
	)

	if addCellInfoOptions.bootstrap {
		ts, err := topo.OpenServer(addCellInfoOptions.topoImpl, strings.Join(addCellInfoOptions.topoServers, ","), addCellInfoOptions.topoRoot)
		if err != nil {
			return fmt.Errorf("failed to connect to the topology server: %v", err)
		}
		defer ts.Close()

		// Use internal vtcltd server implementation.
		// Register a nil grpc handler -- we will not use tmclient at all.
		tmclient.RegisterTabletManagerClientFactory("grpc", func() tmclient.TabletManagerClient {
			return nil
		})
		vtctld := grpcvtctldserver.NewVtctldServer(ts)
		localvtctldclient.SetServer(vtctld)
		VtctldClientProtocol = "local"
		client, err = vtctldclient.New(VtctldClientProtocol, "")
		if err != nil {
			return fmt.Errorf("failed to setup internal vtctld server: %v", err)
		}

		ctx, cancel = context.WithTimeout(commandCtx, topo.RemoteOperationTimeout)
		defer cancel()
	}

	_, err := client.AddCellInfo(ctx, &vtctldatapb.AddCellInfoRequest{
		Name:     cell,
		CellInfo: &addCellInfoOptions.cellInfo,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Created cell: %s\n", cell)

	return nil
}

var addCellsAliasOptions topodatapb.CellsAlias

func commandAddCellsAlias(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	alias := cmd.Flags().Arg(0)
	_, err := client.AddCellsAlias(commandCtx, &vtctldatapb.AddCellsAliasRequest{
		Name:  alias,
		Cells: addCellsAliasOptions.Cells,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Created cells alias: %s (cells = %v)\n", alias, addCellsAliasOptions.Cells)
	return nil
}

var deleteCellInfoOptions = struct {
	Force bool
}{}

func commandDeleteCellInfo(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	cell := cmd.Flags().Arg(0)
	_, err := client.DeleteCellInfo(commandCtx, &vtctldatapb.DeleteCellInfoRequest{
		Name:  cell,
		Force: deleteCellInfoOptions.Force,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Deleted cell %s\n", cell)
	return nil
}

func commandDeleteCellsAlias(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	alias := cmd.Flags().Arg(0)
	_, err := client.DeleteCellsAlias(commandCtx, &vtctldatapb.DeleteCellsAliasRequest{
		Name: alias,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Delete cells alias %s\n", alias)
	return nil
}

func commandGetCellInfoNames(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetCellInfoNames(commandCtx, &vtctldatapb.GetCellInfoNamesRequest{})
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", strings.Join(resp.Names, "\n"))

	return nil
}

func commandGetCellInfo(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	cell := cmd.Flags().Arg(0)

	resp, err := client.GetCellInfo(commandCtx, &vtctldatapb.GetCellInfoRequest{Cell: cell})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.CellInfo)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandGetCellsAliases(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetCellsAliases(commandCtx, &vtctldatapb.GetCellsAliasesRequest{})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Aliases)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var updateCellInfoOptions topodatapb.CellInfo

func commandUpdateCellInfo(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	cell := cmd.Flags().Arg(0)
	resp, err := client.UpdateCellInfo(commandCtx, &vtctldatapb.UpdateCellInfoRequest{
		Name:     cell,
		CellInfo: &updateCellInfoOptions,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.CellInfo)
	if err != nil {
		return err
	}

	fmt.Printf("Updated cell %s. New CellInfo:\n%s\n", resp.Name, data)
	return nil
}

var updateCellsAliasOptions topodatapb.CellsAlias

func commandUpdateCellsAlias(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	alias := cmd.Flags().Arg(0)
	resp, err := client.UpdateCellsAlias(commandCtx, &vtctldatapb.UpdateCellsAliasRequest{
		Name:       alias,
		CellsAlias: &updateCellsAliasOptions,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.CellsAlias)
	if err != nil {
		return err
	}

	fmt.Printf("Updated cells alias %s. New CellsAlias:\n%s\n", resp.Name, data)
	return nil
}

func init() {
	AddCellInfo.Flags().StringVarP(&addCellInfoOptions.cellInfo.ServerAddress, "server-address", "a", "", "The address the topology server will connect to for this cell.")
	AddCellInfo.Flags().StringVarP(&addCellInfoOptions.cellInfo.Root, "root", "r", "", "The root path the topology server will use for this cell.")
	AddCellInfo.Flags().BoolVar(&addCellInfoOptions.bootstrap, "bootstrap", false, fmt.Sprintf("Should we create the cell directly in the topology server(s) to bootstrap the cluster so that we can then start a vtctld process in this cell. Note: you will also need to specify a value of '%s' for --server when using this flag.", useBundledVtctld))
	AddCellInfo.Flags().StringVar(&addCellInfoOptions.topoImpl, "topology-implementation", "etcd2", "The topology server implementation used.")
	AddCellInfo.Flags().StringSliceVar(&addCellInfoOptions.topoServers, "topology-servers", nil, "The endpoints (host and port) to use when connecting directly to the topology server(s) when boostrapping a cluster.")
	AddCellInfo.Flags().StringVar(&addCellInfoOptions.topoRoot, "topology-global-root", "/vitess/global", "The topology server root path to use.")
	AddCellInfo.MarkFlagRequired("root")
	Root.AddCommand(AddCellInfo)

	AddCellsAlias.Flags().StringSliceVarP(&addCellsAliasOptions.Cells, "cells", "c", nil, "The list of cell names that are members of this alias.")
	Root.AddCommand(AddCellsAlias)

	DeleteCellInfo.Flags().BoolVarP(&deleteCellInfoOptions.Force, "force", "f", false, "Proceeds even if the cell's topology server cannot be reached. The assumption is that you shut down the entire cell, and just need to update the global topo data.")
	Root.AddCommand(DeleteCellInfo)
	Root.AddCommand(DeleteCellsAlias)

	Root.AddCommand(GetCellInfoNames)
	Root.AddCommand(GetCellInfo)
	Root.AddCommand(GetCellsAliases)

	UpdateCellInfo.Flags().StringVarP(&updateCellInfoOptions.ServerAddress, "server-address", "a", "", "The address the topology server will connect to for this cell.")
	UpdateCellInfo.Flags().StringVarP(&updateCellInfoOptions.Root, "root", "r", "", "The root path the topology server will use for this cell.")
	Root.AddCommand(UpdateCellInfo)

	UpdateCellsAlias.Flags().StringSliceVarP(&updateCellsAliasOptions.Cells, "cells", "c", nil, "The list of cell names that are members of this alias.")
	Root.AddCommand(UpdateCellsAlias)
}
