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
	// GetSrvKeyspaces makes a GetSrvKeyspaces gRPC call to a vtctld.
	GetSrvKeyspaces = &cobra.Command{
		Use:                   "GetSrvKeyspaces <keyspace> [<cell> ...]",
		Short:                 "Returns the SrvKeyspaces for the given keyspace in one or more cells.",
		Args:                  cobra.MinimumNArgs(1),
		RunE:                  commandGetSrvKeyspaces,
		DisableFlagsInUseLine: true,
	}
	// GetSrvVSchema makes a GetSrvVSchema gRPC call to a vtctld.
	GetSrvVSchema = &cobra.Command{
		Use:                   "GetSrvVSchema cell",
		Short:                 "Returns the SrvVSchema for the given cell.",
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetSrvVSchema,
		DisableFlagsInUseLine: true,
	}
	// GetSrvVSchemas makes a GetSrvVSchemas gRPC call to a vtctld.
	GetSrvVSchemas = &cobra.Command{
		Use:                   "GetSrvVSchemas [<cell> ...]",
		Short:                 "Returns the SrvVSchema for all cells, optionally filtered by the given cells.",
		Args:                  cobra.ArbitraryArgs,
		RunE:                  commandGetSrvVSchemas,
		DisableFlagsInUseLine: true,
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
	Root.AddCommand(GetSrvKeyspaces)
	Root.AddCommand(GetSrvVSchema)
	Root.AddCommand(GetSrvVSchemas)

	RebuildVSchemaGraph.Flags().StringSliceVarP(&rebuildVSchemaGraphOptions.Cells, "cells", "c", nil, "Specifies a comma-separated list of cells to look for tablets")
	Root.AddCommand(RebuildVSchemaGraph)
}
