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
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// GetSchema makes a GetSchema gRPC call to a vtctld.
var GetSchema = &cobra.Command{
	Use:  "GetSchema [--tables TABLES ...] [--exclude-tables EXCLUDE_TABLES ...] [{--table-names-only | --table-sizes-only}] [--include-views] alias",
	Args: cobra.ExactArgs(1),
	RunE: commandGetSchema,
}

var getSchemaOptions = struct {
	Tables         []string
	ExcludeTables  []string
	IncludeViews   bool
	TableNamesOnly bool
	TableSizesOnly bool
}{}

func commandGetSchema(cmd *cobra.Command, args []string) error {
	if getSchemaOptions.TableNamesOnly && getSchemaOptions.TableSizesOnly {
		return errors.New("can only pass one of --table-names-only and --table-sizes-only")
	}

	alias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetSchema(commandCtx, &vtctldatapb.GetSchemaRequest{
		TabletAlias:    alias,
		Tables:         getSchemaOptions.Tables,
		ExcludeTables:  getSchemaOptions.ExcludeTables,
		IncludeViews:   getSchemaOptions.IncludeViews,
		TableNamesOnly: getSchemaOptions.TableNamesOnly,
		TableSizesOnly: getSchemaOptions.TableSizesOnly,
	})
	if err != nil {
		return err
	}

	if getSchemaOptions.TableNamesOnly {
		names := make([]string, len(resp.Schema.TableDefinitions))

		for i, td := range resp.Schema.TableDefinitions {
			names[i] = td.Name
		}

		fmt.Printf("%s\n", strings.Join(names, "\n"))

		return nil
	}

	data, err := cli.MarshalJSON(resp.Schema)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

func init() {
	GetSchema.Flags().StringSliceVar(&getSchemaOptions.Tables, "tables", nil, "TODO")
	GetSchema.Flags().StringSliceVar(&getSchemaOptions.ExcludeTables, "exclude-tables", nil, "TODO")
	GetSchema.Flags().BoolVar(&getSchemaOptions.IncludeViews, "include-views", false, "TODO")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableNamesOnly, "table-names-only", "n", false, "TODO")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableSizesOnly, "table-sizes-only", "s", false, "TODO")

	Root.AddCommand(GetSchema)
}
