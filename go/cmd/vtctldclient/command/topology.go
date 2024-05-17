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
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
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

	// The version of the key/path to get. If not specified, the latest/current
	// version is returned.
	version int64 = 0
	// If true, only the data is output and it is in JSON format rather than prototext.
	dataAsJSON bool = false
)

func commandGetTopologyPath(cmd *cobra.Command, args []string) error {
	path := cmd.Flags().Arg(0)

	cli.FinishedParsing(cmd)

	resp, err := client.GetTopologyPath(commandCtx, &vtctldatapb.GetTopologyPathRequest{
		Path:    path,
		Version: version,
		AsJson:  dataAsJSON,
	})
	if err != nil {
		return err
	}

	if dataAsJSON {
		if resp.GetCell() == nil || resp.GetCell().GetData() == "" {
			return fmt.Errorf("no data found for path %s", path)
		}
		m := make(map[string]any)
		if err := json.Unmarshal([]byte(resp.GetCell().GetData()), &m); err != nil {
			return errors.Wrap(err, "failed to unmarshal node data as JSON")
		}
		skm, err := cli.ConvertToSnakeCase(m)
		if err != nil {
			return errors.Wrap(err, "failed to convert names to snake case")
		}
		js, err := json.MarshalIndent(skm, "", "  ")
		if err != nil {
			return errors.Wrap(err, "failed to marshal JSON data")
		}
		fmt.Println(string(js))
		return nil
	}

	data, err := cli.MarshalJSONPretty(resp.GetCell())
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	GetTopologyPath.Flags().Int64Var(&version, "version", version, "The version of the path's key to get. If not specified, the latest version is returned.")
	GetTopologyPath.Flags().BoolVar(&dataAsJSON, "data-as-json", dataAsJSON, "If true, only the data is output and it is in JSON format rather than prototext.")
	Root.AddCommand(GetTopologyPath)
}
