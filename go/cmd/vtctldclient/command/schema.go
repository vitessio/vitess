package command

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
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
	GetSchema.Flags().StringSliceVar(&getSchemaOptions.Tables, "tables", nil, "List of tables to display the schema for. Each is either an exact match, or a regular expression of the form `/regexp/`.")
	GetSchema.Flags().StringSliceVar(&getSchemaOptions.ExcludeTables, "exclude-tables", nil, "List of tables to exclude from the result. Each is either an exact match, or a regular expression of the form `/regexp/`.")
	GetSchema.Flags().BoolVar(&getSchemaOptions.IncludeViews, "include-views", false, "Includes views in the output in addition to base tables.")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableNamesOnly, "table-names-only", "n", false, "Display only table names in the result.")
	GetSchema.Flags().BoolVarP(&getSchemaOptions.TableSizesOnly, "table-sizes-only", "s", false, "Display only size information for matching tables. Ignored if --table-names-only is set.")

	Root.AddCommand(GetSchema)

	Root.AddCommand(ReloadSchema)

	ReloadSchemaKeyspace.Flags().Uint32Var(&reloadSchemaKeyspaceOptions.Concurrency, "concurrency", 10, "Number of tablets to reload in parallel. Set to zero for unbounded concurrency.")
	ReloadSchemaKeyspace.Flags().BoolVar(&reloadSchemaKeyspaceOptions.IncludePrimary, "include-primary", false, "Also reload the primary tablets.")
	Root.AddCommand(ReloadSchemaKeyspace)

	ReloadSchemaShard.Flags().Uint32Var(&reloadSchemaShardOptions.Concurrency, "concurrency", 10, "Number of tablets to reload in parallel. Set to zero for unbounded concurrency.")
	ReloadSchemaShard.Flags().BoolVar(&reloadSchemaShardOptions.IncludePrimary, "include-primary", false, "Also reload the primary tablet.")
	Root.AddCommand(ReloadSchemaShard)
}
