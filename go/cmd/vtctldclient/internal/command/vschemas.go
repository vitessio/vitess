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
	// GetVSchema makes a GetVSchema gRPC call to a vtctld.
	GetVSchema = &cobra.Command{
		Use:  "GetVSchema keyspace",
		Args: cobra.ExactArgs(1),
		RunE: commandGetVSchema,
	}
)

func commandGetVSchema(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)

	resp, err := client.GetVSchema(commandCtx, &vtctldatapb.GetVSchemaRequest{
		Keyspace: keyspace,
	})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.VSchema)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	Root.AddCommand(GetVSchema)
}
