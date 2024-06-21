/*
Copyright 2024 The Vitess Authors.

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

package common

import (
	"bytes"
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func GetMirrorTrafficCommand(opts *SubCommandsOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "mirrortraffic",
		Short:                 fmt.Sprintf("Mirror traffic for a %s MoveTables workflow.", opts.SubCommand),
		Example:               fmt.Sprintf(`vtctldclient --server localhost:15999 %s --workflow %s --target-keyspace customer mirrortraffic --percent 50.0`, opts.SubCommand, opts.Workflow),
		DisableFlagsInUseLine: true,
		Aliases:               []string{"MirrorTraffic"},
		Args:                  cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			if !cmd.Flags().Lookup("tablet-types").Changed {
				// We mirror traffic for all tablet types if none are provided.
				MirrorTrafficOptions.TabletTypes = []topodatapb.TabletType{
					topodatapb.TabletType_PRIMARY,
					topodatapb.TabletType_REPLICA,
					topodatapb.TabletType_RDONLY,
				}
			}
		},
		RunE: commandMirrorTraffic,
	}
	return cmd
}

func commandMirrorTraffic(cmd *cobra.Command, args []string) error {
	format, err := GetOutputFormat(cmd)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	req := &vtctldatapb.WorkflowMirrorTrafficRequest{
		Keyspace:    BaseOptions.TargetKeyspace,
		Workflow:    BaseOptions.Workflow,
		TabletTypes: MirrorTrafficOptions.TabletTypes,
		Percent:     MirrorTrafficOptions.Percent,
	}
	resp, err := GetClient().WorkflowMirrorTraffic(GetCommandCtx(), req)
	if err != nil {
		return err
	}

	var output []byte
	if format == "json" {
		output, err = cli.MarshalJSONPretty(resp)
		if err != nil {
			return err
		}
	} else {
		tout := bytes.Buffer{}
		tout.WriteString(resp.Summary + "\n\n")
		tout.WriteString(fmt.Sprintf("Start State: %s\n", resp.StartState))
		tout.WriteString(fmt.Sprintf("Current State: %s\n", resp.CurrentState))
		output = tout.Bytes()
	}
	fmt.Printf("%s\n", output)

	return nil
}
