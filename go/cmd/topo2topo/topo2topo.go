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
)

var fromTopo = flag.String("from", "", "topology to copy data from")
var toTopo = flag.String("to", "", "topology to copy data to")

var doKeyspaces = flag.Bool("do-keyspaces", false, "copies the keyspace information")
var doShards = flag.Bool("do-shards", false, "copies the shard information")
var doShardReplications = flag.Bool("do-shard-replications", false, "copies the shard replication information")
var doTablets = flag.Bool("do-tablets", false, "copies the tablet information")

var deleteKeyspaceShards = flag.Bool("delete-keyspace-shards", false, "when copying shards, first removes the destination shards (will nuke the replication graph)")

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

	fromTS := topo.GetServerByName(*fromTopo)
	toTS := topo.GetServerByName(*toTopo)

	if *doKeyspaces {
		helpers.CopyKeyspaces(fromTS, toTS)
	}
	if *doShards {
		helpers.CopyShards(fromTS, toTS, *deleteKeyspaceShards)
	}
	if *doShardReplications {
		helpers.CopyShardReplications(fromTS, toTS)
	}
	if *doTablets {
		helpers.CopyTablets(fromTS, toTS)
	}
}
