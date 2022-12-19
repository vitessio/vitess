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
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	// ApplySchema makes an ApplySchema gRPC call to a vtctld.
	ApplySchema = &cobra.Command{
		Use:   "ApplySchema [--allow-long-unavailability] [--ddl-strategy <strategy>] [--uuid <uuid> ...] [--migration-context <context>] [--wait-replicas-timeout <duration>] [--skip-preflight] [--caller-id <caller_id>] {--sql-file <file> | --sql <sql>} <keyspace>",
		Short: "Applies the schema change to the specified keyspace on every primary, running in parallel on all shards. The changes are then propagated to replicas via replication.",
		Long: `Applies the schema change to the specified keyspace on every primary, running in parallel on all shards. The changes are then propagated to replicas via replication.

If --allow-long-unavailability is set, schema changes affecting a large number of rows (and possibly incurring a longer period of unavailability) will not be rejected.
--ddl-strategy is used to instruct migrations via vreplication, gh-ost or pt-osc with optional parameters.
--migration-context allows the user to specify a custom migration context for online DDL migrations.
If --skip-preflight, SQL goes directly to shards without going through sanity checks.

The --uuid and --sql flags are repeatable, so they can be passed multiple times to build a list of values.
For --uuid, this is used like "--uuid $first_uuid --uuid $second_uuid".
For --sql, semi-colons and repeated values may be mixed, for example:

	ApplySchema --sql "CREATE TABLE my_table; CREATE TABLE my_other_table"
	ApplySchema --sql "CREATE TABLE my_table" --sql "CREATE TABLE my_other_table"`,
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandApplySchema,
	}
	// GetSchema makes a GetSchema gRPC call to a vtctld.
	GetSchema = &cobra.Command{
		Use:                   "GetSchema [--tables TABLES ...] [--exclude-tables EXCLUDE_TABLES ...] [{--table-names-only | --table-sizes-only}] [--include-views] alias",
		Short:                 "Displays the full schema for a tablet, optionally restricted to the specified tables/views.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandGetSchema,
	}
	// ReloadSchema makes a ReloadSchema gRPC call to a vtctld.
	ReloadSchema = &cobra.Command{
		Use:                   "ReloadSchema <tablet_alias>",
		Short:                 "Reloads the schema on a remote tablet.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandReloadSchema,
	}
	// ReloadSchemaKeyspace makes a ReloadSchemaKeyspace gRPC call to a vtctld.
	ReloadSchemaKeyspace = &cobra.Command{
		Use:                   "ReloadSchemaKeyspace [--concurrency=<concurrency>] [--include-primary] <keyspace>",
		Short:                 "Reloads the schema on all tablets in a keyspace. This is done on a best-effort basis.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandReloadSchemaKeyspace,
	}
	// ReloadSchemaShard makes a ReloadSchemaShard gRPC call to a vtctld.
	ReloadSchemaShard = &cobra.Command{
		Use:                   "ReloadSchemaShard [--concurrency=10] [--include-primary] <keyspace/shard>",
		Short:                 "Reloads the schema on all tablets in a shard. This is done on a best-effort basis.",
		DisableFlagsInUseLine: true,
		Args:                  cobra.ExactArgs(1),
		RunE:                  commandReloadSchemaShard,
	}
)

var applySchemaOptions = struct {
	AllowLongUnavailability bool
	SQL                     []string
	SQLFile                 string
	DDLStrategy             string
	UUIDList                []string
	MigrationContext        string
	WaitReplicasTimeout     time.Duration
	SkipPreflight           bool
	CallerID                string
}{}

func commandApplySchema(cmd *cobra.Command, args []string) error {
	var allSQL string
	if applySchemaOptions.SQLFile != "" {
		if len(applySchemaOptions.SQL) != 0 {
			return errors.New("Exactly one of --sql and --sql-file must be specified, not both.") // nolint
		}

		data, err := os.ReadFile(applySchemaOptions.SQLFile)
		if err != nil {
			return err
		}

		allSQL = string(data)
	} else {
		allSQL = strings.Join(applySchemaOptions.SQL, ";")
	}

	parts, err := sqlparser.SplitStatementToPieces(allSQL)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	var cid *vtrpc.CallerID
	if applySchemaOptions.CallerID != "" {
		cid = &vtrpc.CallerID{Principal: applySchemaOptions.CallerID}
	}

	ks := cmd.Flags().Arg(0)

	resp, err := client.ApplySchema(commandCtx, &vtctldatapb.ApplySchemaRequest{
		Keyspace:                ks,
		AllowLongUnavailability: applySchemaOptions.AllowLongUnavailability,
		DdlStrategy:             applySchemaOptions.DDLStrategy,
		Sql:                     parts,
		SkipPreflight:           applySchemaOptions.SkipPreflight,
		UuidList:                applySchemaOptions.UUIDList,
		MigrationContext:        applySchemaOptions.MigrationContext,
		WaitReplicasTimeout:     protoutil.DurationToProto(applySchemaOptions.WaitReplicasTimeout),
		CallerId:                cid,
	})
	if err != nil {
		return err
	}

	fmt.Println(strings.Join(resp.UuidList, "\n"))
	return nil
}

var getSchemaOptions = struct {
	Tables          []string
	ExcludeTables   []string
	IncludeViews    bool
	TableNamesOnly  bool
	TableSizesOnly  bool
	TableSchemaOnly bool
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
		TabletAlias:     alias,
		Tables:          getSchemaOptions.Tables,
		ExcludeTables:   getSchemaOptions.ExcludeTables,
		IncludeViews:    getSchemaOptions.IncludeViews,
		TableNamesOnly:  getSchemaOptions.TableNamesOnly,
		TableSizesOnly:  getSchemaOptions.TableSizesOnly,
		TableSchemaOnly: getSchemaOptions.TableSchemaOnly,
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

func commandReloadSchema(cmd *cobra.Command, args []string) error {
	tabletAlias, err := topoproto.ParseTabletAlias(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.ReloadSchema(commandCtx, &vtctldatapb.ReloadSchemaRequest{
		TabletAlias: tabletAlias,
	})
	if err != nil {
		return err
	}

	return nil
}

var reloadSchemaKeyspaceOptions = struct {
	Concurrency    uint32
	IncludePrimary bool
}{
	Concurrency: 10,
}

func commandReloadSchemaKeyspace(cmd *cobra.Command, args []string) error {
	cli.FinishedParsing(cmd)

	logger := logutil.NewConsoleLogger()
	resp, err := client.ReloadSchemaKeyspace(commandCtx, &vtctldatapb.ReloadSchemaKeyspaceRequest{
		Keyspace:       cmd.Flags().Arg(0),
		Concurrency:    reloadSchemaKeyspaceOptions.Concurrency,
		IncludePrimary: reloadSchemaKeyspaceOptions.IncludePrimary,
	})
	if resp != nil {
		for _, e := range resp.Events {
			logutil.LogEvent(logger, e)
		}
	}

	return err
}

var reloadSchemaShardOptions = struct {
	Concurrency    uint32
	IncludePrimary bool
}{
	Concurrency: 10,
}

func commandReloadSchemaShard(cmd *cobra.Command, args []string) error {
	keyspace, shard, err := topoproto.ParseKeyspaceShard(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	logger := logutil.NewConsoleLogger()
	resp, err := client.ReloadSchemaShard(commandCtx, &vtctldatapb.ReloadSchemaShardRequest{
		Keyspace:       keyspace,
		Shard:          shard,
		Concurrency:    reloadSchemaShardOptions.Concurrency,
		IncludePrimary: reloadSchemaShardOptions.IncludePrimary,
	})
	if resp != nil {
		for _, e := range resp.Events {
			logutil.LogEvent(logger, e)
		}
	}

	return err
}

func init() {
	ApplySchema.Flags().BoolVar(&applySchemaOptions.AllowLongUnavailability, "allow-long-unavailability", false, "Allow large schema changes which incur a longer unavailability of the database.")
	ApplySchema.Flags().StringVar(&applySchemaOptions.DDLStrategy, "ddl-strategy", string(schema.DDLStrategyDirect), "Online DDL strategy, compatible with @@ddl_strategy session variable (examples: 'gh-ost', 'pt-osc', 'gh-ost --max-load=Threads_running=100'.")
	ApplySchema.Flags().StringSliceVar(&applySchemaOptions.UUIDList, "uuid", nil, "Optional, comma-delimited, repeatable, explicit UUIDs for migration. If given, must match number of DDL changes.")
	ApplySchema.Flags().StringVar(&applySchemaOptions.MigrationContext, "migration-context", "", "For Online DDL, optionally supply a custom unique string used as context for the migration(s) in this command. By default a unique context is auto-generated by Vitess.")
	ApplySchema.Flags().DurationVar(&applySchemaOptions.WaitReplicasTimeout, "wait-replicas-timeout", wrangler.DefaultWaitReplicasTimeout, "Amount of time to wait for replicas to receive the schema change via replication.")
	ApplySchema.Flags().BoolVar(&applySchemaOptions.SkipPreflight, "skip-preflight", false, "Skip pre-apply schema checks, and directly forward schema change query to shards.")
	ApplySchema.Flags().StringVar(&applySchemaOptions.CallerID, "caller-id", "", "Effective caller ID used for the operation and should map to an ACL name which grants this identity the necessary permissions to perform the operation (this is only necessary when strict table ACLs are used).")
	ApplySchema.Flags().StringArrayVar(&applySchemaOptions.SQL, "sql", nil, "Semicolon-delimited, repeatable SQL commands to apply. Exactly one of --sql|--sql-file is required.")
	ApplySchema.Flags().StringVar(&applySchemaOptions.SQLFile, "sql-file", "", "Path to a file containing semicolon-delimited SQL commands to apply. Exactly one of --sql|--sql-file is required.")

	Root.AddCommand(ApplySchema)

	GetSchema.Flags().StringSliceVar(&getSchemaOptions.Tables, "tables", nil, "List of tables to display the schema for. Each is either an exact match, or a regular expression of the form `/regexp/`.")
	GetSchema.Flags().StringSliceVar(&getSchemaOptions.ExcludeTables, "exclude-tables", nil, "List of tables to exclude from the result. Each is either an exact match, or a regular expression of the form `/regexp/`.")
	GetSchema.Flags().BoolVar(&getSchemaOptions.IncludeViews, "include-views", false, "Includes views in the output in addition to base tables.")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableNamesOnly, "table-names-only", "n", false, "Display only table names in the result.")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableSizesOnly, "table-sizes-only", "s", false, "Display only size information for matching tables. Ignored if --table-names-only is set.")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableSchemaOnly, "table-schema-only", "", false, "Skip introspecting columns and fields metadata.")

	Root.AddCommand(GetSchema)

	Root.AddCommand(ReloadSchema)

	ReloadSchemaKeyspace.Flags().Uint32Var(&reloadSchemaKeyspaceOptions.Concurrency, "concurrency", 10, "Number of tablets to reload in parallel. Set to zero for unbounded concurrency.")
	ReloadSchemaKeyspace.Flags().BoolVar(&reloadSchemaKeyspaceOptions.IncludePrimary, "include-primary", false, "Also reload the primary tablets.")
	Root.AddCommand(ReloadSchemaKeyspace)

	ReloadSchemaShard.Flags().Uint32Var(&reloadSchemaShardOptions.Concurrency, "concurrency", 10, "Number of tablets to reload in parallel. Set to zero for unbounded concurrency.")
	ReloadSchemaShard.Flags().BoolVar(&reloadSchemaShardOptions.IncludePrimary, "include-primary", false, "Also reload the primary tablet.")
	Root.AddCommand(ReloadSchemaShard)
}
