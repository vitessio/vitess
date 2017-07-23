/*
Copyright 2017 Google Inc.

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

package wrangler

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
		finalErr = errors.New("some validation errors - see log")
		wr.Logger().Errorf("%v", err)
	}
	return finalErr
}

// Validate all tablets in all discoverable cells, even if they are
// not in the replication graph.
func (wr *Wrangler) validateAllTablets(ctx context.Context, wg *sync.WaitGroup, results chan<- error) {
	cellSet := make(map[string]bool, 16)

	keyspaces, err := wr.ts.GetKeyspaces(ctx)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetKeyspaces failed: %v", err)
		return
	}
	for _, keyspace := range keyspaces {
		shards, err := wr.ts.GetShardNames(ctx, keyspace)
		if err != nil {
			results <- fmt.Errorf("TopologyServer.GetShardNames(%v) failed: %v", keyspace, err)
			return
		}

		for _, shard := range shards {
			aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
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
		aliases, err := wr.ts.GetTabletsByCell(ctx, cell)
		if err != nil {
			results <- fmt.Errorf("TopologyServer.GetTabletsByCell(%v) failed: %v", cell, err)
			continue
		}

		for _, alias := range aliases {
			wg.Add(1)
			go func(alias *topodatapb.TabletAlias) {
				defer wg.Done()
				if err := topo.Validate(ctx, wr.ts, alias); err != nil {
					results <- fmt.Errorf("Validate(%v) failed: %v", topoproto.TabletAliasString(alias), err)
				} else {
					wr.Logger().Infof("tablet %v is valid", topoproto.TabletAliasString(alias))
				}
			}(alias)
		}
	}
}

func (wr *Wrangler) validateKeyspace(ctx context.Context, keyspace string, pingTablets bool, wg *sync.WaitGroup, results chan<- error) {
	// Validate replication graph by traversing each shard.
	shards, err := wr.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetShardNames(%v) failed: %v", keyspace, err)
		return
	}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			wr.validateShard(ctx, keyspace, shard, pingTablets, wg, results)
		}(shard)
	}
}

func (wr *Wrangler) validateShard(ctx context.Context, keyspace, shard string, pingTablets bool, wg *sync.WaitGroup, results chan<- error) {
	shardInfo, err := wr.ts.GetShard(ctx, keyspace, shard)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.GetShard(%v, %v) failed: %v", keyspace, shard, err)
		return
	}

	aliases, err := wr.ts.FindAllTabletAliasesInShard(ctx, keyspace, shard)
	if err != nil {
		results <- fmt.Errorf("TopologyServer.FindAllTabletAliasesInShard(%v, %v) failed: %v", keyspace, shard, err)
		return
	}

	tabletMap, _ := wr.ts.GetTabletMap(ctx, aliases)

	var masterAlias *topodatapb.TabletAlias
	for _, alias := range aliases {
		tabletInfo, ok := tabletMap[topoproto.TabletAliasString(alias)]
		if !ok {
			results <- fmt.Errorf("tablet %v not found in map", topoproto.TabletAliasString(alias))
			continue
		}
		if tabletInfo.Type == topodatapb.TabletType_MASTER {
			if masterAlias != nil {
				results <- fmt.Errorf("shard %v/%v already has master %v but found other master %v", keyspace, shard, topoproto.TabletAliasString(masterAlias), topoproto.TabletAliasString(alias))
			} else {
				masterAlias = alias
			}
		}
	}

	if masterAlias == nil {
		results <- fmt.Errorf("no master for shard %v/%v", keyspace, shard)
	} else if !topoproto.TabletAliasEqual(shardInfo.MasterAlias, masterAlias) {
		results <- fmt.Errorf("master mismatch for shard %v/%v: found %v, expected %v", keyspace, shard, topoproto.TabletAliasString(masterAlias), topoproto.TabletAliasString(shardInfo.MasterAlias))
	}

	for _, alias := range aliases {
		wg.Add(1)
		go func(alias *topodatapb.TabletAlias) {
			defer wg.Done()
			if err := topo.Validate(ctx, wr.ts, alias); err != nil {
				results <- fmt.Errorf("Validate(%v) failed: %v", topoproto.TabletAliasString(alias), err)
			} else {
				wr.Logger().Infof("tablet %v is valid", topoproto.TabletAliasString(alias))
			}
		}(alias)
	}

	if pingTablets {
		wr.validateReplication(ctx, shardInfo, tabletMap, results)
		wr.pingTablets(ctx, tabletMap, wg, results)
	}

	return
}

func normalizeIP(ip string) string {
	// Normalize loopback to avoid spurious validation errors.
	if parsedIP := net.ParseIP(ip); parsedIP != nil && parsedIP.IsLoopback() {
		// Note that this also maps IPv6 localhost to IPv4 localhost
		// as GetSlaves() will return only IPv4 addresses.
		return "127.0.0.1"
	}
	return ip
}

func (wr *Wrangler) validateReplication(ctx context.Context, shardInfo *topo.ShardInfo, tabletMap map[string]*topo.TabletInfo, results chan<- error) {
	if shardInfo.MasterAlias == nil {
		results <- fmt.Errorf("no master in shard record %v/%v", shardInfo.Keyspace(), shardInfo.ShardName())
		return
	}

	shardInfoMasterAliasStr := topoproto.TabletAliasString(shardInfo.MasterAlias)
	masterTabletInfo, ok := tabletMap[shardInfoMasterAliasStr]
	if !ok {
		results <- fmt.Errorf("master %v not in tablet map", shardInfoMasterAliasStr)
		return
	}

	slaveList, err := wr.tmc.GetSlaves(ctx, masterTabletInfo.Tablet)
	if err != nil {
		results <- fmt.Errorf("GetSlaves(%v) failed: %v", masterTabletInfo, err)
		return
	}
	if len(slaveList) == 0 {
		results <- fmt.Errorf("no slaves of tablet %v found", shardInfoMasterAliasStr)
		return
	}

	tabletIPMap := make(map[string]*topodatapb.Tablet)
	slaveIPMap := make(map[string]bool)
	for _, tablet := range tabletMap {
		ip, err := topoproto.MySQLIP(tablet.Tablet)
		if err != nil {
			results <- fmt.Errorf("could not resolve IP for tablet %s: %v", topoproto.MysqlHostname(tablet.Tablet), err)
			continue
		}
		tabletIPMap[normalizeIP(ip)] = tablet.Tablet
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

		ip, err := topoproto.MySQLIP(tablet.Tablet)
		if err != nil {
			results <- fmt.Errorf("could not resolve IP for tablet %s: %v", topoproto.MysqlHostname(tablet.Tablet), err)
			continue
		}
		if !slaveIPMap[normalizeIP(ip)] {
			results <- fmt.Errorf("slave %v not replicating: %v slave list: %q", topoproto.TabletAliasString(tablet.Alias), ip, slaveList)
		}
	}
}

func (wr *Wrangler) pingTablets(ctx context.Context, tabletMap map[string]*topo.TabletInfo, wg *sync.WaitGroup, results chan<- error) {
	for tabletAlias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(tabletAlias string, tabletInfo *topo.TabletInfo) {
			defer wg.Done()

			if err := wr.tmc.Ping(ctx, tabletInfo.Tablet); err != nil {
				results <- fmt.Errorf("Ping(%v) failed: %v tablet hostname: %v", tabletAlias, err, tabletInfo.Hostname)
			}
		}(tabletAlias, tabletInfo)
	}
}

// Validate a whole TopologyServer tree
func (wr *Wrangler) Validate(ctx context.Context, pingTablets bool) error {
	// Results from various actions feed here.
	results := make(chan error, 16)
	wg := &sync.WaitGroup{}

	// Validate all tablets in all cells, even if they are not discoverable
	// by the replication graph.
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateAllTablets(ctx, wg, results)
	}()

	// Validate replication graph by traversing each keyspace and then each shard.
	keyspaces, err := wr.ts.GetKeyspaces(ctx)
	if err != nil {
		results <- fmt.Errorf("GetKeyspaces failed: %v", err)
	} else {
		for _, keyspace := range keyspaces {
			wg.Add(1)
			go func(keyspace string) {
				defer wg.Done()
				wr.validateKeyspace(ctx, keyspace, pingTablets, wg, results)
			}(keyspace)
		}
	}
	return wr.waitForResults(wg, results)
}

// ValidateKeyspace will validate a bunch of information in a keyspace
// is correct.
func (wr *Wrangler) ValidateKeyspace(ctx context.Context, keyspace string, pingTablets bool) error {
	wg := &sync.WaitGroup{}
	results := make(chan error, 16)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateKeyspace(ctx, keyspace, pingTablets, wg, results)
	}()
	return wr.waitForResults(wg, results)
}

// ValidateShard will validate a bunch of information in a shard is correct.
func (wr *Wrangler) ValidateShard(ctx context.Context, keyspace, shard string, pingTablets bool) error {
	wg := &sync.WaitGroup{}
	results := make(chan error, 16)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wr.validateShard(ctx, keyspace, shard, pingTablets, wg, results)
	}()
	return wr.waitForResults(wg, results)
}
