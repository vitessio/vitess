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
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// GetBackups makes a GetBackups gRPC call to a vtctld.
var GetBackups = &cobra.Command{
	Use:  "GetBackups keyspace shard",
	Args: cobra.ExactArgs(2),
	RunE: commandGetBackups,
}

func commandGetBackups(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	keyspace := cmd.Flags().Arg(0)
	shard := cmd.Flags().Arg(1)

	resp, err := client.GetBackups(commandCtx, &vtctldatapb.GetBackupsRequest{
		Keyspace: keyspace,
		Shard:    shard,
	})
	if err != nil {
		return err
	}

	names := make([]string, len(resp.Backups))
	for i, b := range resp.Backups {
		names[i] = b.Name
	}

	fmt.Printf("%s\n", strings.Join(names, "\n"))

	return nil
}

func init() {
	Root.AddCommand(GetBackups)
}
