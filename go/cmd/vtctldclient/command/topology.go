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
	// GetTopology fetches the topology for a given cluster
	GetTopology = &cobra.Command{
		Use:   "GetTopology <cluster-id>",
		Short: "Gets topology map for specified cluster",
		Long:  `Gets the topology map from the specified cluster's vtctld toposerver instance`,
		Args:  cobra.ExactArgs(1),
		RunE:  commandGetTopology,
	}
)

func commandGetTopology(cmd *cobra.Command, args []string) error {
	clusterID := cmd.Flags().Arg(0)

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.GetTopologyRequest{
		ClusterId: clusterID,
	}

	resp, err := client.GetTopology(commandCtx, req)
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Cells)
	if err != nil {
		return err
	}

	fmt.Printf("Successfully fetched topology for cluster %s. Result:\n%s\n", clusterID, data)

	return nil
}

func init() {
	Root.AddCommand(GetTopology)
}
