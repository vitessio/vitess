// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

// As with all distributed systems, things can skew. These functions
// explore data in topology server and attempt to square that with reality.
//
// Given the node counts are usually large, this work should be done
// with as much parallelism as is viable.
//
// This may eventually move into a separate package.

// waitForResults will wait for all the errors to come back.
// There is no timeout, as individual calls will use the context and timeout
// and fail at the end anyway.
func (wr *Wrangler) waitForResults(wg *sync.WaitGroup, results chan error) error {
	go func() {
		wg.Wait()
		close(results)
	}()

	var finalErr error
	for err := range results {
		finalErr = fmt.Errorf("some validation errors - see log")
		log.Errorf("%v", err)
	}
	return finalErr
}

// Validate all tablets in all discoverable cells, even if they are
// not in the replication graph.
func (wr *Wrangler) validateAllTablets(wg *sync.WaitGroup, results chan<- error) {
	cellSet := make(map[string]bool, 16)

	keyspaces, err := wr.ts.GetKeyspaces()
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetKeyspaces failed: %v", err)
		return
	}
	for _, keyspace := range keyspaces {
		shards, err := wr.ts.GetShardNames(keyspace)
		if err != nil {
			results <- fmt.Errorf("TopologyServer.GetShardNames(%v) failed: %v", keyspace, err)
			return
		}

		for _, shard := range shards {
			aliases, err := topo.FindAllTabletAliasesInShard(wr.ctx, wr.ts, keyspace, shard)
			if err != nil {
				results <- fmt.Errorf("TopologyServer.FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err)
				return
			}
			for _, alias := range aliases {
				cellSet[alias.Cell] = true
			}
		}
	}

	for cell := range cellSet {
		aliases, err := wr.ts.GetTabletsByCell(cell)
		if err != nil {
			results <- fmt.Errorf("TopologyServer.GetTabletsByCell(%v) failed: %v", cell, err)
			continue
		}

		for _, alias := range aliases {
			wg.Add(1)
			go func(alias topo.TabletAlias) {
				defer wg.Done()
				if err := topo.Validate(wr.ts, alias); err != nil {
					results <- fmt.Errorf("Validate(%v) failed: %v", alias, err)
				} else {
					wr.Logger().Infof("tablet %v is valid", alias)
				}
			}(alias)
		}
	}
}

func (wr *Wrangler) validateKeyspace(keyspace string, pingTablets bool, wg *sync.WaitGroup, results chan<- error) {
	// Validate replication graph by traversing each shard.
	shards, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetShardNames(%v) failed: %v", keyspace, err)
		return
	}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			wr.validateShard(keyspace, shard, pingTablets, wg, results)
		}(shard)
	}
}

// FIXME(msolomon) This validate presumes the master is up and running.
// Even when that isn't true, there are validation processes that might be valuable.
func (wr *Wrangler) validateShard(keyspace, shard string, pingTablets bool, wg *sync.WaitGroup, results chan<- error) {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetShard(%v, %v) failed: %v", keyspace, shard, err)
		return
	}

	aliases, err := topo.FindAllTabletAliasesInShard(wr.ctx, wr.ts, keyspace, shard)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err)
		return
	}

	tabletMap, _ := topo.GetTabletMap(wr.ctx, wr.ts, aliases)

	var masterAlias topo.TabletAlias
	for _, alias := range aliases {
		tabletInfo, ok := tabletMap[alias]
		if !ok {
			results <- fmt.Errorf("tablet %v not found in map", alias)
			continue
		}
		if tabletInfo.Type == topo.TYPE_MASTER {
			if masterAlias.Cell != "" {
				results <- fmt.Errorf("shard %v/%v already has master %v but found other master %v", keyspace, shard, masterAlias, alias)
			} else {
				masterAlias = alias
			}
		}
	}

	if masterAlias.Cell == "" {
		results <- fmt.Errorf("no master for shard %v/%v", keyspace, shard)
	} else if shardInfo.MasterAlias != masterAlias {
		results <- fmt.Errorf("master mismatch for shard %v/%v: found %v, expected %v", keyspace, shard, masterAlias, shardInfo.MasterAlias)
	}

	for _, alias := range aliases {
		wg.Add(1)
		go func(alias topo.TabletAlias) {
			defer wg.Done()
			if err := topo.Validate(wr.ts, alias); err != nil {
				results <- fmt.Errorf("Validate(%v) failed: %v", alias, err)
			} else {
				wr.Logger().Infof("tablet %v is valid", alias)
			}
		}(alias)
	}

	if pingTablets {
		wr.validateReplication(shardInfo, tabletMap, results)
		wr.pingTablets(tabletMap, wg, results)
	}

	return
}

func normalizeIP(ip string) string {
	// Normalize loopback to avoid spurious validation errors.
	if strings.HasPrefix(ip, "127.") {
		return "127.0.0.1"
	}
	return ip
}

func (wr *Wrangler) validateReplication(shardInfo *topo.ShardInfo, tabletMap map[topo.TabletAlias]*topo.TabletInfo, results chan<- error) {
	masterTablet, ok := tabletMap[shardInfo.MasterAlias]
	if !ok {
		results <- fmt.Errorf("master %v not in tablet map", shardInfo.MasterAlias)
		return
	}

	slaveList, err := wr.tmc.GetSlaves(wr.ctx, masterTablet)
	if err != nil {
		results <- fmt.Errorf("GetSlaves(%v) failed: %v", masterTablet, err)
		return
	}
	if len(slaveList) == 0 {
		results <- fmt.Errorf("no slaves of tablet %v found", shardInfo.MasterAlias)
		return
	}

	tabletIPMap := make(map[string]*topo.Tablet)
	slaveIPMap := make(map[string]bool)
	for _, tablet := range tabletMap {
		tabletIPMap[normalizeIP(tablet.IPAddr)] = tablet.Tablet
	}

	// See if every slave is in the replication graph.
	for _, slaveAddr := range slaveList {
		if tabletIPMap[normalizeIP(slaveAddr)] == nil {
			results <- fmt.Errorf("slave %v not in replication graph for shard %v/%v (mysql instance without vttablet?)", slaveAddr, shardInfo.Keyspace(), shardInfo.ShardName())
		}
		slaveIPMap[normalizeIP(slaveAddr)] = true
	}

	// See if every entry in the replication graph is connected to the master.
	for _, tablet := range tabletMap {
		if !tablet.IsSlaveType() {
			continue
		}

		if !slaveIPMap[normalizeIP(tablet.IPAddr)] {
			results <- fmt.Errorf("slave %v not replicating: %v %q", tablet.Alias, tablet.IPAddr, slaveList)
		}
	}
}

func (wr *Wrangler) pingTablets(tabletMap map[topo.TabletAlias]*topo.TabletInfo, wg *sync.WaitGroup, results chan<- error) {
	for tabletAlias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(tabletAlias topo.TabletAlias, tabletInfo *topo.TabletInfo) {
			defer wg.Done()

			if err := wr.tmc.Ping(wr.ctx, tabletInfo); err != nil {
				results <- fmt.Errorf("Ping(%v) failed: %v %v", tabletAlias, err, tabletInfo.Hostname)
			}
		}(tabletAlias, tabletInfo)
	}
}

// Validate a whole TopologyServer tree
func (wr *Wrangler) Validate(pingTablets bool) error {
	// Results from various actions feed here.
	results := make(chan error, 16)
	wg := &sync.WaitGroup{}

	// Validate all tablets in all cells, even if they are not discoverable
	// by the replication graph.
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateAllTablets(wg, results)
	}()

	// Validate replication graph by traversing each keyspace and then each shard.
	keyspaces, err := wr.ts.GetKeyspaces()
	if err != nil {
		results <- fmt.Errorf("GetKeyspaces failed: %v", err)
	} else {
		for _, keyspace := range keyspaces {
			wg.Add(1)
			go func(keyspace string) {
				defer wg.Done()
				wr.validateKeyspace(keyspace, pingTablets, wg, results)
			}(keyspace)
		}
	}
	return wr.waitForResults(wg, results)
}

// ValidateKeyspace will validate a bunch of information in a keyspace
// is correct.
func (wr *Wrangler) ValidateKeyspace(keyspace string, pingTablets bool) error {
	wg := &sync.WaitGroup{}
	results := make(chan error, 16)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateKeyspace(keyspace, pingTablets, wg, results)
	}()
	return wr.waitForResults(wg, results)
}

// ValidateShard will validate a bunch of information in a shard is correct.
func (wr *Wrangler) ValidateShard(keyspace, shard string, pingTablets bool) error {
	wg := &sync.WaitGroup{}
	results := make(chan error, 16)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateShard(keyspace, shard, pingTablets, wg, results)
	}()
	return wr.waitForResults(wg, results)
}
