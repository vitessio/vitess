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
	"time"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/cmd/vtctldclient/cli"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
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

func commandGetTablet(cmd *cobra.Command, args []string) error {
	aliasStr := cmd.Flags().Arg(0)
	alias, err := topoproto.ParseTabletAlias(aliasStr)
	if err != nil {
		return err
	}

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
		lineFn := func(t *topodatapb.Tablet) string {
			ti := topo.TabletInfo{
				Tablet: t,
			}

			keyspace := t.Keyspace
			if keyspace == "" {
				keyspace = "<null>"
			}

			shard := t.Shard
			if shard == "" {
				shard = "<null>"
			}

			mtst := "<null>"
			// special case for old primary that hasn't been updated in the topo
			// yet.
			if t.MasterTermStartTime != nil && t.MasterTermStartTime.Seconds > 0 {
				mtst = logutil.ProtoToTime(t.MasterTermStartTime).Format(time.RFC3339)
			}

			return fmt.Sprintf("%v %v %v %v %v %v %v %v", topoproto.TabletAliasString(t.Alias), keyspace, shard, topoproto.TabletTypeLString(t.Type), ti.Addr(), ti.MysqlAddr(), cli.MarshalMapAWK(t.Tags), mtst)
		}

		for _, t := range resp.Tablets {
			fmt.Println(lineFn(t))
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
	Root.AddCommand(GetTablet)

	GetTablets.Flags().StringSliceVarP(&getTabletsOptions.Cells, "cell", "c", nil, "list of cells to filter tablets by")
	GetTablets.Flags().StringVarP(&getTabletsOptions.Keyspace, "keyspace", "k", "", "keyspace to filter tablets by")
	GetTablets.Flags().StringVarP(&getTabletsOptions.Shard, "shard", "s", "", "shard to filter tablets by")
	GetTablets.Flags().StringVar(&getTabletsOptions.Format, "format", "awk", "Output format to use; valid choices are (json, awk)")
	Root.AddCommand(GetTablets)
}
