// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

var cell = flag.String("cell", "test_nj", "cell to use")

func main() {
	flag.Parse()
	ts := topo.GetServer()
	defer topo.CloseServers()
	keyspaces, err := ts.GetKeyspaces()
	if err != nil {
		log.Errorf("error: %v", err)
		return
	}
	log.Infof("keyspaces: %v", keyspaces)
	for _, keyspace := range keyspaces {
		shards, err := ts.GetShardNames(keyspace)
		if err != nil {
			log.Errorf("error: %v", err)
			return
		}
		log.Infof("shards: %v", shards)
		for _, shard := range shards {
			typs, err := ts.GetSrvTabletTypesPerShard(*cell, keyspace, shard)
			if err != nil {
				log.Errorf("error: %v", err)
				return
			}
			log.Infof("tablet types: %v", typs)
			for _, typ := range typs {
				endpoints, err := ts.GetEndPoints(*cell, keyspace, shard, typ)
				if err != nil {
					log.Errorf("error: %v", err)
					return
				}
				log.Infof("end points: %v", endpoints)
			}
		}
	}
}
