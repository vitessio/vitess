/*
Copyright 2019 The Vitess Authors.

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

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
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
)

func init() {
	servenv.OnParse(func(fs *pflag.FlagSet) {
		fs.StringVar(&fromImplementation, "from_implementation", fromImplementation, "topology implementation to copy data from")
		fs.StringVar(&fromServerAddress, "from_server", fromServerAddress, "topology server address to copy data from")
		fs.StringVar(&fromRoot, "from_root", fromRoot, "topology server root to copy data from")
		fs.StringVar(&toImplementation, "to_implementation", toImplementation, "topology implementation to copy data to")
		fs.StringVar(&toServerAddress, "to_server", toServerAddress, "topology server address to copy data to")
		fs.StringVar(&toRoot, "to_root", toRoot, "topology server root to copy data to")
		fs.BoolVar(&compare, "compare", compare, "compares data between topologies")
		fs.BoolVar(&doKeyspaces, "do-keyspaces", doKeyspaces, "copies the keyspace information")
		fs.BoolVar(&doShards, "do-shards", doShards, "copies the shard information")
		fs.BoolVar(&doShardReplications, "do-shard-replications", doShardReplications, "copies the shard replication information")
		fs.BoolVar(&doTablets, "do-tablets", doTablets, "copies the tablet information")
		fs.BoolVar(&doRoutingRules, "do-routing-rules", doRoutingRules, "copies the routing rules")

		acl.RegisterFlags(fs)
	})
}

func main() {
	defer exit.RecoverAll()
	defer logutil.Flush()

	fs := pflag.NewFlagSet("topo2topo", pflag.ExitOnError)
	grpccommon.RegisterFlags(fs)
	log.RegisterFlags(fs)
	logutil.RegisterFlags(fs)

	servenv.ParseFlags("topo2topo")

	fromTS, err := topo.OpenServer(fromImplementation, fromServerAddress, fromRoot)
	if err != nil {
		log.Exitf("Cannot open 'from' topo %v: %v", fromImplementation, err)
	}
	toTS, err := topo.OpenServer(toImplementation, toServerAddress, toRoot)
	if err != nil {
		log.Exitf("Cannot open 'to' topo %v: %v", toImplementation, err)
	}

	ctx := context.Background()

	if compare {
		compareTopos(ctx, fromTS, toTS)
		return
	}
	copyTopos(ctx, fromTS, toTS)
}

func copyTopos(ctx context.Context, fromTS, toTS *topo.Server) {
	if doKeyspaces {
		helpers.CopyKeyspaces(ctx, fromTS, toTS)
	}
	if doShards {
		helpers.CopyShards(ctx, fromTS, toTS)
	}
	if doShardReplications {
		helpers.CopyShardReplications(ctx, fromTS, toTS)
	}
	if doTablets {
		helpers.CopyTablets(ctx, fromTS, toTS)
	}
	if doRoutingRules {
		helpers.CopyRoutingRules(ctx, fromTS, toTS)
	}
}

func compareTopos(ctx context.Context, fromTS, toTS *topo.Server) {
	var err error
	if doKeyspaces {
		err = helpers.CompareKeyspaces(ctx, fromTS, toTS)
		if err != nil {
			log.Exitf("Compare keyspaces failed: %v", err)
		}
	}
	if doShards {
		err = helpers.CompareShards(ctx, fromTS, toTS)
		if err != nil {
			log.Exitf("Compare shards failed: %v", err)
		}
	}
	if doShardReplications {
		err = helpers.CompareShardReplications(ctx, fromTS, toTS)
		if err != nil {
			log.Exitf("Compare shard replications failed: %v", err)
		}
	}
	if doTablets {
		err = helpers.CompareTablets(ctx, fromTS, toTS)
		if err != nil {
			log.Exitf("Compare tablets failed: %v", err)
		}
	}
	if err == nil {
		fmt.Println("Topologies are in sync")
		os.Exit(0)
	}
}
