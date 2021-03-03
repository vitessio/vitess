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

var (
	// ChangeTabletType makes a ChangeTabletType gRPC call to a vtctld.
	ChangeTabletType = &cobra.Command{
		Use:  "ChangeTabletType [--dry-run] TABLET_ALIAS TABLET_TYPE",
		Args: cobra.ExactArgs(2),
		RunE: commandChangeTabletType,
	}
	// DeleteTablets makes a DeleteTablets gRPC call to a vtctld.
	DeleteTablets = &cobra.Command{
		Use:  "DeleteTablets TABLET_ALIAS [ TABLET_ALIAS ... ]",
		Args: cobra.MinimumNArgs(1),
		RunE: commandDeleteTablets,
	}
	// GetTablet makes a GetTablet gRPC call to a vtctld.
	GetTablet = &cobra.Command{
		Use:  "GetTablet alias",
		Args: cobra.ExactArgs(1),
		RunE: commandGetTablet,
	}
	// GetTablets makes a GetTablets gRPC call to a vtctld.
	GetTablets = &cobra.Command{
		Use:  "GetTablets [--cell $c1, ...] [--keyspace $ks [--shard $shard]]",
		Args: cobra.NoArgs,
		RunE: commandGetTablets,
	}
)

var changeTabletTypeOptions = struct {
	DryRun bool
}{}

func commandChangeTabletType(cmd *cobra.Command, args []string) error {
	aliasStr := cmd.Flags().Arg(0)
	typeStr := cmd.Flags().Arg(1)

	alias, err := topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		return err
	}

	newType, err := topoproto.ParseTabletType(typeStr)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.ChangeTabletType(commandCtx, &vtctldatapb.ChangeTabletTypeRequest{
		TabletAlias: alias,
		DbType:      newType,
		DryRun:      changeTabletTypeOptions.DryRun,
	})
	if err != nil {
		return err
	}

	if resp.WasDryRun {
		fmt.Println("--- DRY RUN ---")
	}

	fmt.Printf("- %v\n", cli.MarshalTabletAWK(resp.BeforeTablet))
	fmt.Printf("+ %v\n", cli.MarshalTabletAWK(resp.AfterTablet))

	return nil
}

var deleteTabletsOptions = struct {
	AllowPrimary bool
}{}

func commandDeleteTablets(cmd *cobra.Command, args []string) error {
	aliases, err := cli.TabletAliasesFromPosArgs(cmd.Flags().Args())
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	_, err = client.DeleteTablets(commandCtx, &vtctldatapb.DeleteTabletsRequest{
		TabletAliases: aliases,
		AllowPrimary:  deleteTabletsOptions.AllowPrimary,
	})

	if err != nil {
		return fmt.Errorf("%w: while deleting %d tablets; please inspect the topo", err, len(aliases))
	}

	fmt.Printf("Successfully deleted %d tablets\n", len(aliases))

	return nil
}

func commandGetTablet(cmd *cobra.Command, args []string) error {
	aliasStr := cmd.Flags().Arg(0)
	alias, err := topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		return err
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetTablet(commandCtx, &vtctldatapb.GetTabletRequest{TabletAlias: alias})
	if err != nil {
		return err
	}

	data, err := cli.MarshalJSON(resp.Tablet)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", data)

	return nil
}

var getTabletsOptions = struct {
	Cells    []string
	Keyspace string
	Shard    string

	Format string
}{}

func commandGetTablets(cmd *cobra.Command, args []string) error {
	format := strings.ToLower(getTabletsOptions.Format)

	switch format {
	case "awk", "json":
	default:
		return fmt.Errorf("invalid output format, got %s", getTabletsOptions.Format)
	}

	if getTabletsOptions.Keyspace == "" && getTabletsOptions.Shard != "" {
		return fmt.Errorf("--shard (= %s) cannot be passed without also passing --keyspace", getTabletsOptions.Shard)
	}

	cli.FinishedParsing(cmd)

	resp, err := client.GetTablets(commandCtx, &vtctldatapb.GetTabletsRequest{
		Cells:    getTabletsOptions.Cells,
		Keyspace: getTabletsOptions.Keyspace,
		Shard:    getTabletsOptions.Shard,
	})
	if err != nil {
		return err
	}

	switch format {
	case "awk":
		for _, t := range resp.Tablets {
			fmt.Println(cli.MarshalTabletAWK(t))
		}
	case "json":
		data, err := cli.MarshalJSON(resp.Tablets)
		if err != nil {
			return err
		}

		fmt.Printf("%s\n", data)
	}

	return nil
}

func init() {
	ChangeTabletType.Flags().BoolVarP(&changeTabletTypeOptions.DryRun, "dry-run", "d", false, "Shows the proposed change without actually executing it")
	Root.AddCommand(ChangeTabletType)

	DeleteTablets.Flags().BoolVarP(&deleteTabletsOptions.AllowPrimary, "allow-primary", "p", false, "Allow the primary tablet of a shard to be deleted. Use with caution.")
	Root.AddCommand(DeleteTablets)

	Root.AddCommand(GetTablet)

	GetTablets.Flags().StringSliceVarP(&getTabletsOptions.Cells, "cell", "c", nil, "list of cells to filter tablets by")
	GetTablets.Flags().StringVarP(&getTabletsOptions.Keyspace, "keyspace", "k", "", "keyspace to filter tablets by")
	GetTablets.Flags().StringVarP(&getTabletsOptions.Shard, "shard", "s", "", "shard to filter tablets by")
	GetTablets.Flags().StringVar(&getTabletsOptions.Format, "format", "awk", "Output format to use; valid choices are (json, awk)")
	Root.AddCommand(GetTablets)
}
