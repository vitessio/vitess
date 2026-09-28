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

package command

import (
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// GetMirrorRules makes a GetMirrorRules gRPC call to a vtctld.
var GetMirrorRules = &cobra.Command{
	Use:                   "GetMirrorRules",
	Short:                 "Displays the VSchema mirror rules.",
	DisableFlagsInUseLine: true,
	Args:                  cobra.NoArgs,
	RunE:                  commandGetMirrorRules,
}

func commandGetMirrorRules(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	resp, err := client.GetMirrorRules(commandCtx, &vtctldatapb.GetMirrorRulesRequest{})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.MirrorRules)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	Root.AddCommand(GetMirrorRules)
}
