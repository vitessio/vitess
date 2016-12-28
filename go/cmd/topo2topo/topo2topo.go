// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/helpers"
	"golang.org/x/net/context"
)

var (
	fromImplementation = flag.String("from_implementation", "", "topology implementation to copy data from")
	fromServerAddress  = flag.String("from_server", "", "topology server address to copy data from")
	fromRoot           = flag.String("from_root", "", "topology server root to copy data from")

	toImplementation = flag.String("to_implementation", "", "topology implementation to copy data to")
	toServerAddress  = flag.String("to_server", "", "topology server address to copy data to")
	toRoot           = flag.String("to_root", "", "topology server root to copy data to")

	doKeyspaces         = flag.Bool("do-keyspaces", false, "copies the keyspace information")
	doShards            = flag.Bool("do-shards", false, "copies the shard information")
	doShardReplications = flag.Bool("do-shard-replications", false, "copies the shard replication information")
	doTablets           = flag.Bool("do-tablets", false, "copies the tablet information")
)

func main() {
	defer exit.RecoverAll()
	defer logutil.Flush()

	flag.Parse()
	args := flag.Args()
	if len(args) != 0 {
		flag.Usage()
		log.Fatalf("topo2topo doesn't take any parameter.")
	}

	fromTS, err := topo.OpenServer(*fromImplementation, *fromServerAddress, *fromRoot)
	if err != nil {
		log.Fatalf("Cannot open 'from' topo %v: %v", *fromImplementation, err)
	}
	toTS, err := topo.OpenServer(*toImplementation, *toServerAddress, *toRoot)
	if err != nil {
		log.Fatalf("Cannot open 'to' topo %v: %v", *toImplementation, err)
	}

	ctx := context.Background()

	if *doKeyspaces {
		helpers.CopyKeyspaces(ctx, fromTS.Impl, toTS.Impl)
	}
	if *doShards {
		helpers.CopyShards(ctx, fromTS.Impl, toTS.Impl)
	}
	if *doShardReplications {
		helpers.CopyShardReplications(ctx, fromTS.Impl, toTS.Impl)
	}
	if *doTablets {
		helpers.CopyTablets(ctx, fromTS.Impl, toTS.Impl)
	}
}
