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

var (
	// GetSchema makes a GetSchema gRPC call to a vtctld.
	GetSchema = &cobra.Command{
		Use:  "GetSchema [--tables TABLES ...] [--exclude-tables EXCLUDE_TABLES ...] [{--table-names-only | --table-sizes-only}] [--include-views] alias",
		Args: cobra.ExactArgs(1),
		RunE: commandGetSchema,
	}
	// ValidateSchemaKeyspace makes a ValidateSchemaKeyspace gRPC call to a vtctld.
	ValidateSchemaKeyspace = &cobra.Command{
		Use:  "ValidateSchemaKeyspace",
		RunE: commandValidateSchemaKeyspace,
	}
	// ValidateSchemaShard makes a ValidateSchemaShard gRPC call to a vtctld.
	ValidateSchemaShard = &cobra.Command{
		Use:  "ValidateSchemaShard",
		RunE: commandValidateSchemaShard,
	}
)

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

var validateSchemaKeyspaceOptions = struct {
	Keyspace       string
	ExcludeTables  []string
	IncludeVSchema bool
	SkipNoPrimary  bool
	IncludeViews   bool
}{}

func commandValidateSchemaKeyspace(cmd *cobra.Command, args []string) error {

	_, err := client.ValidateSchemaKeyspace(commandCtx, &vtctldatapb.ValidateSchemaKeyspaceRequest{
		Keyspace:       validateSchemaKeyspaceOptions.Keyspace,
		ExcludeTables:  validateSchemaKeyspaceOptions.ExcludeTables,
		IncludeVSchema: validateSchemaKeyspaceOptions.IncludeVSchema,
		SkipNoPrimary:  validateSchemaKeyspaceOptions.SkipNoPrimary,
		IncludeViews:   validateSchemaKeyspaceOptions.IncludeViews,
	})
	if err != nil {
		return err
	}

	return nil
}

var validateSchemaShardOptions = struct {
	Keyspace       string
	ExcludeTables  []string
	IncludeVSchema bool
	IncludeViews   bool
	Shard          string
}{}

func commandValidateSchemaShard(cmd *cobra.Command, args []string) error {

	_, err := client.ValidateSchemaShard(commandCtx, &vtctldatapb.ValidateSchemaShardRequest{
		Keyspace:       validateSchemaShardOptions.Keyspace,
		ExcludeTables:  validateSchemaShardOptions.ExcludeTables,
		IncludeVSchema: validateSchemaShardOptions.IncludeVSchema,
		IncludeViews:   validateSchemaShardOptions.IncludeViews,
		Shard:          validateSchemaShardOptions.Shard,
	})
	if err != nil {
		return err
	}

	return nil
}

func init() {
	GetSchema.Flags().StringSliceVar(&getSchemaOptions.Tables, "tables", nil, "TODO")
	GetSchema.Flags().StringSliceVar(&getSchemaOptions.ExcludeTables, "exclude-tables", nil, "TODO")
	GetSchema.Flags().BoolVar(&getSchemaOptions.IncludeViews, "include-views", false, "TODO")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableNamesOnly, "table-names-only", "n", false, "TODO")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableSizesOnly, "table-sizes-only", "s", false, "TODO")

	Root.AddCommand(GetSchema)

	ValidateSchemaKeyspace.Flags().StringVar(&validateSchemaKeyspaceOptions.Keyspace, "keyspace", "keyspace", "The keyspace in which we want to validate.")
	ValidateSchemaKeyspace.Flags().StringSliceVar(&validateSchemaKeyspaceOptions.ExcludeTables, "exclude-tables", []string{}, "If specified, will exclude these tables from the schema validation")
	ValidateSchemaKeyspace.Flags().BoolVar(&validateSchemaKeyspaceOptions.IncludeViews, "include-views", false, "If specified, include views in the schema validation.")
	ValidateSchemaKeyspace.Flags().BoolVar(&validateSchemaKeyspaceOptions.IncludeVSchema, "include-vschema", false, "If specified, will include a ValidateVSchema check")
	ValidateSchemaKeyspace.Flags().BoolVar(&validateSchemaKeyspaceOptions.SkipNoPrimary, "skip-no-primary", false, "If specified, will omit shards without a primary.")
	Root.AddCommand(ValidateSchemaKeyspace)

	ValidateSchemaShard.Flags().StringVar(&validateSchemaShardOptions.Keyspace, "keyspace", "keyspace", "The keyspace in which we want to validate.")
	ValidateSchemaShard.Flags().StringVar(&validateSchemaShardOptions.Shard, "shard", "shard", "The shard that we want to validate.")
	ValidateSchemaShard.Flags().StringSliceVar(&validateSchemaShardOptions.ExcludeTables, "exclude-tables", []string{}, "If specified, will exclude these tables from the schema validation")
	ValidateSchemaShard.Flags().BoolVar(&validateSchemaShardOptions.IncludeViews, "include-views", false, "If specified, include views in the schema validation.")
	ValidateSchemaShard.Flags().BoolVar(&validateSchemaShardOptions.IncludeVSchema, "include-vschema", false, "If specified, will include a ValidateVSchema check")
	Root.AddCommand(ValidateSchemaShard)
}
