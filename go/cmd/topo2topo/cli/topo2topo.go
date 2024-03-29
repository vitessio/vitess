/*
Copyright 2023 The Vitess Authors.

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

package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/helpers"
)

var (
	fromImplementation  string
	fromServerAddress   string
	fromRoot            string
	toImplementation    string
	toServerAddress     string
	toRoot              string
	compare             bool
	doKeyspaces         bool
	doShards            bool
	doShardReplications bool
	doTablets           bool
	doRoutingRules      bool

	Main = &cobra.Command{
		Use:   "topo2topo",
		Short: "topo2topo copies Vitess topology data from one topo server to another.",
		Long: `topo2topo copies Vitess topology data from one topo server to another.
It can also be used to compare data between two topologies.`,
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		Version: servenv.AppVersion.String(),
		RunE:    run,
	}
)

func init() {
	servenv.MoveFlagsToCobraCommand(Main)

	Main.Flags().StringVar(&fromImplementation, "from_implementation", fromImplementation, "topology implementation to copy data from")
	Main.Flags().StringVar(&fromServerAddress, "from_server", fromServerAddress, "topology server address to copy data from")
	Main.Flags().StringVar(&fromRoot, "from_root", fromRoot, "topology server root to copy data from")
	Main.Flags().StringVar(&toImplementation, "to_implementation", toImplementation, "topology implementation to copy data to")
	Main.Flags().StringVar(&toServerAddress, "to_server", toServerAddress, "topology server address to copy data to")
	Main.Flags().StringVar(&toRoot, "to_root", toRoot, "topology server root to copy data to")
	Main.Flags().BoolVar(&compare, "compare", compare, "compares data between topologies")
	Main.Flags().BoolVar(&doKeyspaces, "do-keyspaces", doKeyspaces, "copies the keyspace information")
	Main.Flags().BoolVar(&doShards, "do-shards", doShards, "copies the shard information")
	Main.Flags().BoolVar(&doShardReplications, "do-shard-replications", doShardReplications, "copies the shard replication information")
	Main.Flags().BoolVar(&doTablets, "do-tablets", doTablets, "copies the tablet information")
	Main.Flags().BoolVar(&doRoutingRules, "do-routing-rules", doRoutingRules, "copies the routing rules")

	acl.RegisterFlags(Main.Flags())
	grpccommon.RegisterFlags(Main.Flags())
}

func run(cmd *cobra.Command, args []string) error {
	defer logutil.Flush()
	servenv.Init()

	fromTS, err := topo.OpenServer(fromImplementation, fromServerAddress, fromRoot)
	if err != nil {
		return fmt.Errorf("Cannot open 'from' topo %v: %w", fromImplementation, err)
	}
	toTS, err := topo.OpenServer(toImplementation, toServerAddress, toRoot)
	if err != nil {
		return fmt.Errorf("Cannot open 'to' topo %v: %w", toImplementation, err)
	}

	ctx := context.Background()

	if compare {
		return compareTopos(ctx, fromTS, toTS)
	}

	parser, err := sqlparser.New(sqlparser.Options{
		MySQLServerVersion: servenv.MySQLServerVersion(),
		TruncateUILen:      servenv.TruncateUILen,
		TruncateErrLen:     servenv.TruncateErrLen,
	})
	if err != nil {
		return fmt.Errorf("cannot create sqlparser: %w", err)
	}

	return copyTopos(ctx, fromTS, toTS, parser)
}

func copyTopos(ctx context.Context, fromTS, toTS *topo.Server, parser *sqlparser.Parser) error {
	if doKeyspaces {
		if err := helpers.CopyKeyspaces(ctx, fromTS, toTS, parser); err != nil {
			return err
		}
	}
	if doShards {
		if err := helpers.CopyShards(ctx, fromTS, toTS); err != nil {
			return err
		}
	}
	if doShardReplications {
		if err := helpers.CopyShardReplications(ctx, fromTS, toTS); err != nil {
			return err
		}
	}
	if doTablets {
		if err := helpers.CopyTablets(ctx, fromTS, toTS); err != nil {
			return err
		}
	}
	if doRoutingRules {
		if err := helpers.CopyRoutingRules(ctx, fromTS, toTS); err != nil {
			return err
		}
	}

	return nil
}

func compareTopos(ctx context.Context, fromTS, toTS *topo.Server) (err error) {
	if doKeyspaces {
		err = helpers.CompareKeyspaces(ctx, fromTS, toTS)
		if err != nil {
			return fmt.Errorf("Compare keyspaces failed: %w", err)
		}
	}
	if doShards {
		err = helpers.CompareShards(ctx, fromTS, toTS)
		if err != nil {
			return fmt.Errorf("Compare shards failed: %w", err)
		}
	}
	if doShardReplications {
		err = helpers.CompareShardReplications(ctx, fromTS, toTS)
		if err != nil {
			return fmt.Errorf("Compare shard replications failed: %w", err)
		}
	}
	if doTablets {
		err = helpers.CompareTablets(ctx, fromTS, toTS)
		if err != nil {
			return fmt.Errorf("Compare tablets failed: %w", err)
		}
	}

	fmt.Println("Topologies are in sync")
	return nil
}
