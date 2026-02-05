/*
Copyright 2022 The Vitess Authors.

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
	"os"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	// GetTopologyPath makes a GetTopologyPath gRPC call to a vtctld.
	GetTopologyPath = &cobra.Command{
		Use:                   "GetTopologyPath <path>",
		Short:                 "Gets the value associated with the particular path (key) in the topology server.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetTopologyPath,
	}

	// SetVtorcEmergencyReparent enables/disables the use of EmergencyReparentShard in VTOrc recoveries for a given keyspace or keyspace/shard.
	SetVtorcEmergencyReparent = &cobra.Command{
		Use:                   "SetVtorcEmergencyReparent [--enable|-e] [--disable|-d] <keyspace> <shard>",
		Short:                 "Enable/disables the use of EmergencyReparentShard in VTOrc recoveries for a given keyspace or keyspace/shard.",
		DisableFlagsInUseLine: true,
		Aliases:               []string{"setvtorcemergencyreparent"},
		Args:                  cobra.RangeArgs(1, 2),
		RunE:                  commandSetVtorcEmergencyReparent,
	}

	// WriteTopologyPath writes the contents of a local file to a path
	// in the topology server.
	WriteTopologyPath = &cobra.Command{
		Use:                   "WriteTopologyPath --server=local <path> <file>",
		Short:                 "Copies a local file to the topology server at the given path.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if VtctldClientProtocol != "local" {
				return fmt.Errorf("The WriteTopologyPath command can only be used with --server=%s", useInternalVtctld)
			}
			return nil
		},
		RunE: commandWriteTopologyPath,
	}
)

var getTopologyPathOptions = struct {
	// The version of the key/path to get. If not specified, the latest/current
	// version is returned.
	version int64
	// If true, only the data is output and it is in JSON format rather than prototext.
	dataAsJSON bool
}{}

func commandGetTopologyPath(cmd *cobra.Command, args []string) error {
	path := cmd.Flags().Arg(0)

	cli.FinishedParsing(cmd)

	resp, err := client.GetTopologyPath(commandCtx, &vtctldatapb.GetTopologyPathRequest{
		Path:    path,
		Version: getTopologyPathOptions.version,
		AsJson:  getTopologyPathOptions.dataAsJSON,
	})
	if err != nil {
		return err
	}

	if getTopologyPathOptions.dataAsJSON {
		if resp.GetCell() == nil || resp.GetCell().GetData() == "" {
			return fmt.Errorf("no data found for path %s", path)
		}
		fmt.Println(resp.GetCell().GetData())
		return nil
	}

	data, err := cli.MarshalJSONPretty(resp.GetCell())
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var setVtorcEmergencyReparentOptions = struct {
	Disable bool
	Enable  bool
}{}

func commandSetVtorcEmergencyReparent(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	ks := cmd.Flags().Arg(0)
	shard := cmd.Flags().Arg(1)
	keyspaceShard := topoproto.KeyspaceShardString(ks, shard)
	if !setVtorcEmergencyReparentOptions.Disable && !setVtorcEmergencyReparentOptions.Enable {
		return fmt.Errorf("SetVtorcEmergencyReparent(%v) error: must set --enable or --disable flag", keyspaceShard)
	}
	if setVtorcEmergencyReparentOptions.Disable && setVtorcEmergencyReparentOptions.Enable {
		return fmt.Errorf("SetVtorcEmergencyReparent(%v) error: --enable and --disable flags are mutually exclusive", keyspaceShard)
	}

	_, err := client.SetVtorcEmergencyReparent(commandCtx, &vtctldatapb.SetVtorcEmergencyReparentRequest{
		Keyspace: ks,
		Shard:    shard,
		Disable:  setVtorcEmergencyReparentOptions.Disable,
	})

	if err != nil {
		return fmt.Errorf("SetVtorcEmergencyReparent(%v) error: %w; please check the topo", keyspaceShard, err)
	}

	fmt.Printf("Successfully updated keyspace/shard %v.\n", keyspaceShard)

	return nil
}

var writeTopologyPathOptions = struct {
	// The cell to use for the copy. Defaults to the global cell.
	cell string
}{}

func commandWriteTopologyPath(cmd *cobra.Command, args []string) error {
	path := cmd.Flags().Arg(0)
	file := cmd.Flags().Arg(1)
	ts, err := topo.OpenServer(topoOptions.implementation, strings.Join(topoOptions.globalServerAddresses, ","), topoOptions.globalRoot)
	if err != nil {
		return fmt.Errorf("failed to connect to the topology server: %v", err)
	}
	cli.FinishedParsing(cmd)

	conn, err := ts.ConnForCell(cmd.Context(), writeTopologyPathOptions.cell)
	if err != nil {
		return fmt.Errorf("failed to connect to the topology server: %v", err)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %v", file, err)
	}
	_, err = conn.Update(cmd.Context(), path, data, nil)
	if err != nil {
		return fmt.Errorf("failed to write to topology server path %s: %v", path, err)
	}

	return nil
}

func init() {
	GetTopologyPath.Flags().Int64Var(&getTopologyPathOptions.version, "version", getTopologyPathOptions.version, "The version of the path's key to get. If not specified, the latest version is returned.")
	GetTopologyPath.Flags().BoolVar(&getTopologyPathOptions.dataAsJSON, "data-as-json", getTopologyPathOptions.dataAsJSON, "If true, only the data is output and it is in JSON format rather than prototext.")
	Root.AddCommand(GetTopologyPath)

	Root.AddCommand(SetVtorcEmergencyReparent)
	SetVtorcEmergencyReparent.Flags().BoolVarP(&setVtorcEmergencyReparentOptions.Disable, "disable", "d", false, "Disable the use of EmergencyReparentShard in recoveries.")
	SetVtorcEmergencyReparent.Flags().BoolVarP(&setVtorcEmergencyReparentOptions.Enable, "enable", "e", false, "Enable the use of EmergencyReparentShard in recoveries.")

	WriteTopologyPath.Flags().StringVar(&writeTopologyPathOptions.cell, "cell", topo.GlobalCell, "Topology server cell to copy the file to.")
	Root.AddCommand(WriteTopologyPath)
}
