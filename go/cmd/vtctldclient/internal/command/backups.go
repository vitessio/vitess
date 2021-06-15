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
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// GetBackups makes a GetBackups gRPC call to a vtctld.
var GetBackups = &cobra.Command{
	Use:  "GetBackups <keyspace/shard>",
	Args: cobra.ExactArgs(1),
	RunE: commandGetBackups,
}

var getBackupsOptions = struct {
	Limit         uint32
	Detailed      bool
	DetailedLimit uint32
	OutputJSON    bool
}{}

func commandGetBackups(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetBackups(commandCtx, &vtctldatapb.GetBackupsRequest{
		Keyspace:      keyspace,
		Shard:         shard,
		Limit:         getBackupsOptions.Limit,
		Detailed:      getBackupsOptions.Detailed,
		DetailedLimit: getBackupsOptions.DetailedLimit,
	})
	if err != nil {
		return err
	}

	if getBackupsOptions.OutputJSON || getBackupsOptions.Detailed {
		data, err := cli.MarshalJSON(resp)
		if err != nil {
			return err
		}

		fmt.Printf("%s\n", data)
		return nil
	}

	names := make([]string, len(resp.Backups))
	for i, b := range resp.Backups {
		names[i] = b.Name
	}

	fmt.Printf("%s\n", strings.Join(names, "\n"))

	return nil
}

func init() {
	GetBackups.Flags().Uint32VarP(&getBackupsOptions.Limit, "limit", "l", 0, "Retrieve only the most recent N backups")
	GetBackups.Flags().BoolVarP(&getBackupsOptions.OutputJSON, "json", "j", false, "Output backup info in JSON format rather than a list of backups")
	GetBackups.Flags().BoolVar(&getBackupsOptions.Detailed, "detailed", false, "Get detailed backup info, such as the engine used for each backup, and its status. Implies --json.")
	GetBackups.Flags().Uint32Var(&getBackupsOptions.DetailedLimit, "detailed-limit", 0, "Get detailed backup info for only the most recent N backups. Ignored if --detailed is not passed.")
	Root.AddCommand(GetBackups)
}
