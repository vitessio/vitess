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

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	// DeleteSrvVSchema makes a DeleteSrvVSchema gRPC call to a vtctld.
	DeleteSrvVSchema = &cobra.Command{
		Use:                   "DeleteSrvVSchema <cell>",
		Short:                 "Deletes the SrvVSchema object in the given cell.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandDeleteSrvVSchema,
	}
	// GetSrvKeyspaceNames makes a GetSrvKeyspaceNames gRPC call to a vtctld.
	GetSrvKeyspaceNames = &cobra.Command{
		Use:                   "GetSrvKeyspaceNames [<cell> ...]",
		Short:                 "Outputs a JSON mapping of cell=>keyspace names served in that cell. Omit to query all cells.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ArbitraryArgs,
		RunE:                  commandGetSrvKeyspaceNames,
	}
	// GetSrvKeyspaces makes a GetSrvKeyspaces gRPC call to a vtctld.
	GetSrvKeyspaces = &cobra.Command{
		Use:                   "GetSrvKeyspaces <keyspace> [<cell> ...]",
		Short:                 "Returns the SrvKeyspaces for the given keyspace in one or more cells.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(1),
		RunE:                  commandGetSrvKeyspaces,
	}
	// GetSrvVSchema makes a GetSrvVSchema gRPC call to a vtctld.
	GetSrvVSchema = &cobra.Command{
		Use:                   "GetSrvVSchema cell",
		Short:                 "Returns the SrvVSchema for the given cell.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetSrvVSchema,
	}
	// GetSrvVSchemas makes a GetSrvVSchemas gRPC call to a vtctld.
	GetSrvVSchemas = &cobra.Command{
		Use:                   "GetSrvVSchemas [<cell> ...]",
		Short:                 "Returns the SrvVSchema for all cells, optionally filtered by the given cells.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ArbitraryArgs,
		RunE:                  commandGetSrvVSchemas,
	}
	// RebuildKeyspaceGraph makes one or more RebuildKeyspaceGraph gRPC calls to a vtctld.
	RebuildKeyspaceGraph = &cobra.Command{
		Use:                   "RebuildKeyspaceGraph [--cells=c1,c2,...] [--allow-partial] ks1 [ks2 ...]",
		Short:                 "Rebuilds the serving data for the keyspace(s). This command may trigger an update to all connected clients.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.MinimumNArgs(1),
		RunE:                  commandRebuildKeyspaceGraph,
	}
	// RebuildVSchemaGraph makes a RebuildVSchemaGraph gRPC call to a vtctld.
	RebuildVSchemaGraph = &cobra.Command{
		Use:                   "RebuildVSchemaGraph [--cells=c1,c2,...]",
		Short:                 "Rebuilds the cell-specific SrvVSchema from the global VSchema objects in the provided cells (or all cells if none provided).",
		DisableFlagsInUseLine: true,
		Args:                  cobra.NoArgs,
		RunE:                  commandRebuildVSchemaGraph,
	}
)

func commandDeleteSrvVSchema(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	cell := cmd.Flags().Arg(0)
	_, err := client.DeleteSrvVSchema(commandCtx, &vtctldatapb.DeleteSrvVSchemaRequest{
		Cell: cell,
	})

	return err
}

func commandGetSrvKeyspaceNames(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	cells := cmd.Flags().Args()
	resp, err := client.GetSrvKeyspaceNames(commandCtx, &vtctldatapb.GetSrvKeyspaceNamesRequest{
		Cells: cells,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Names)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)
	return nil
}

func commandGetSrvKeyspaces(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)
	cells := cmd.Flags().Args()[1:]

	resp, err := client.GetSrvKeyspaces(commandCtx, &vtctldatapb.GetSrvKeyspacesRequest{
		Keyspace: keyspace,
		Cells:    cells,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.SrvKeyspaces)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandGetSrvVSchema(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	cell := cmd.Flags().Arg(0)

	resp, err := client.GetSrvVSchema(commandCtx, &vtctldatapb.GetSrvVSchemaRequest{
		Cell: cell,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.SrvVSchema)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func commandGetSrvVSchemas(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	cells := cmd.Flags().Args()[0:]

	resp, err := client.GetSrvVSchemas(commandCtx, &vtctldatapb.GetSrvVSchemasRequest{
		Cells: cells,
	})
	if err != nil {
		return err
	}

	// By default, an empty array will serialize as `null`, but `[]` is a little nicer.
	data := []byte("[]")

	if len(resp.SrvVSchemas) > 0 {
		data, err = cli.MarshalJSON(resp.SrvVSchemas)
		if err != nil {
			return err
		}
	}

	fmt.Printf("%s\n", data)

	return nil
}

var rebuildKeyspaceGraphOptions = struct {
	Cells        []string
	AllowPartial bool
}{}

func commandRebuildKeyspaceGraph(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspaces := cmd.Flags().Args()
	for _, ks := range keyspaces {
		_, err := client.RebuildKeyspaceGraph(commandCtx, &vtctldatapb.RebuildKeyspaceGraphRequest{
			Keyspace:     ks,
			Cells:        rebuildKeyspaceGraphOptions.Cells,
			AllowPartial: rebuildKeyspaceGraphOptions.AllowPartial,
		})
		if err != nil {
			return fmt.Errorf("RebuildKeyspaceGraph(%v) failed: %v", ks, err)
		}
	}

	return nil
}

var rebuildVSchemaGraphOptions = struct {
	Cells []string
}{}

func commandRebuildVSchemaGraph(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	_, err := client.RebuildVSchemaGraph(commandCtx, &vtctldatapb.RebuildVSchemaGraphRequest{
		Cells: rebuildVSchemaGraphOptions.Cells,
	})
	if err != nil {
		return err
	}

	fmt.Println("RebuildVSchemaGraph: ok")

	return nil
}

func init() {
	Root.AddCommand(DeleteSrvVSchema)

	Root.AddCommand(GetSrvKeyspaceNames)
	Root.AddCommand(GetSrvKeyspaces)
	Root.AddCommand(GetSrvVSchema)
	Root.AddCommand(GetSrvVSchemas)

	RebuildKeyspaceGraph.Flags().StringSliceVarP(&rebuildKeyspaceGraphOptions.Cells, "cells", "c", nil, "Specifies a comma-separated list of cells to update.")
	RebuildKeyspaceGraph.Flags().BoolVar(&rebuildKeyspaceGraphOptions.AllowPartial, "allow-partial", false, "Specifies whether a SNAPSHOT keyspace is allowed to serve with an incomplete set of shards. Ignored for all other types of keyspaces.")
	Root.AddCommand(RebuildKeyspaceGraph)

	RebuildVSchemaGraph.Flags().StringSliceVarP(&rebuildVSchemaGraphOptions.Cells, "cells", "c", nil, "Specifies a comma-separated list of cells to look for tablets.")
	Root.AddCommand(RebuildVSchemaGraph)
}
