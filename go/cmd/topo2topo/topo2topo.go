// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
)

var fromTopo = flag.String("from", "", "topology to copy data from")
var toTopo = flag.String("to", "", "topology to copy data to")

var doKeyspaces = flag.Bool("do-keyspaces", false, "copies the keyspace information")
var doShards = flag.Bool("do-shards", false, "copies the shard information")
var doTablets = flag.Bool("do-tablets", false, "copies the tablet information")

var deleteKeyspaceShards = flag.Bool("delete-keyspace-shards", false, "when copying shards, first removes the destination shards (will nuke the replication graph)")

var logLevel = flag.String("log.level", "INFO", "set log level")

func main() {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			log.Fatalf("panic: %v", tb.Errorf("%v", panicErr))
		}
	}()

	flag.Parse()
	args := flag.Args()
	if len(args) != 0 {
		flag.Usage()
		os.Exit(1)
	}

	logLevel, err := relog.LogNameToLogLevel(*logLevel)
	if err != nil {
		log.Fatalf("%v", err)
	}
	relog.SetLevel(logLevel)

	if *fromTopo == "" || *toTopo == "" {
		log.Fatalf("Need both from and to topo")
	}

	fromTS := topo.GetServerByName(*fromTopo)
	toTS := topo.GetServerByName(*toTopo)

	if *doKeyspaces {
		topotools.CopyKeyspaces(fromTS, toTS)
	}
	if *doShards {
		topotools.CopyShards(fromTS, toTS, *deleteKeyspaceShards)
	}
	if *doTablets {
		topotools.CopyTablets(fromTS, toTS)
	}
}
