// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/helpers"
	"golang.org/x/net/context"
)

var fromTopo = flag.String("from", "", "topology to copy data from")
var toTopo = flag.String("to", "", "topology to copy data to")

var doKeyspaces = flag.Bool("do-keyspaces", false, "copies the keyspace information")
var doShards = flag.Bool("do-shards", false, "copies the shard information")
var doShardReplications = flag.Bool("do-shard-replications", false, "copies the shard replication information")
var doTablets = flag.Bool("do-tablets", false, "copies the tablet information")

func main() {
	defer exit.RecoverAll()
	defer logutil.Flush()

	flag.Parse()
	args := flag.Args()
	if len(args) != 0 {
		flag.Usage()
		os.Exit(1)
	}

	if *fromTopo == "" || *toTopo == "" {
		log.Errorf("Need both from and to topo")
		exit.Return(1)
	}

	ctx := context.Background()
	fromTS := topo.GetServerByName(*fromTopo)
	toTS := topo.GetServerByName(*toTopo)

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
