// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go.net/context"

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

type vresult struct {
	name string
	err  error
}

func (wr *Wrangler) waitForResults(wg *sync.WaitGroup, results chan vresult) error {
	timer := time.NewTimer(wr.ActionTimeout())
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	var err error
wait:
	for {
		select {
		case vd := <-results:
			log.Infof("checking %v", vd.name)
			if vd.err != nil {
				err = fmt.Errorf("some validation errors - see log")
				log.Errorf("%v: %v", vd.name, vd.err)
			}
		case <-timer.C:
			err = fmt.Errorf("timed out during validate")
			break wait
		case <-done:
			// To prevent a false positive, once we are 'done',
			// drain the result channel completely.
			for {
				select {
				case vd := <-results:
					log.Infof("checking %v", vd.name)
					if vd.err != nil {
						err = fmt.Errorf("some validation errors - see log")
						log.Errorf("%v: %v", vd.name, vd.err)
					}
				default:
					break wait
				}
			}
		}
	}

	return err
}

// Validate all tablets in all discoverable cells, even if they are
// not in the replication graph.
func (wr *Wrangler) validateAllTablets(wg *sync.WaitGroup, results chan<- vresult) {

	cellSet := make(map[string]bool, 16)

	keyspaces, err := wr.ts.GetKeyspaces()
	if err != nil {
		results <- vresult{"TopologyServer.GetKeyspaces", err}
		return
	}
	for _, keyspace := range keyspaces {
		shards, err := wr.ts.GetShardNames(keyspace)
		if err != nil {
			results <- vresult{"TopologyServer.GetShardNames(" + keyspace + ")", err}
			return
		}

		for _, shard := range shards {
			aliases, err := topo.FindAllTabletAliasesInShard(context.TODO(), wr.ts, keyspace, shard)
			if err != nil {
				results <- vresult{"TopologyServer.FindAllTabletAliasesInShard(" + keyspace + "," + shard + ")", err}
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
			results <- vresult{"GetTabletsByCell(" + cell + ")", err}
		} else {
			for _, alias := range aliases {
				wg.Add(1)
				go func(alias topo.TabletAlias) {
					results <- vresult{alias.String(), topo.Validate(wr.ts, alias)}
					wg.Done()
				}(alias)
			}
		}
	}
}

func (wr *Wrangler) validateKeyspace(keyspace string, pingTablets bool, wg *sync.WaitGroup, results chan<- vresult) {
	// Validate replication graph by traversing each shard.
	shards, err := wr.ts.GetShardNames(keyspace)
	if err != nil {
		results <- vresult{"TopologyServer.GetShardNames(" + keyspace + ")", err}
	}
	for _, shard := range shards {
		wg.Add(1)
		go func(shard string) {
			wr.validateShard(keyspace, shard, pingTablets, wg, results)
			wg.Done()
		}(shard)
	}
}

// FIXME(msolomon) This validate presumes the master is up and running.
// Even when that isn't true, there are validation processes that might be valuable.
func (wr *Wrangler) validateShard(keyspace, shard string, pingTablets bool, wg *sync.WaitGroup, results chan<- vresult) {
	shardInfo, err := wr.ts.GetShard(keyspace, shard)
	if err != nil {
		results <- vresult{keyspace + "/" + shard, err}
		return
	}

	aliases, err := topo.FindAllTabletAliasesInShard(context.TODO(), wr.ts, keyspace, shard)
	if err != nil {
		results <- vresult{keyspace + "/" + shard, err}
	}

	tabletMap, _ := topo.GetTabletMap(context.TODO(), wr.ts, aliases)

	var masterAlias topo.TabletAlias
	for _, alias := range aliases {
		tabletInfo, ok := tabletMap[alias]
		if !ok {
			results <- vresult{alias.String(), fmt.Errorf("tablet not found in map")}
			continue
		}
		if tabletInfo.Parent.Uid == topo.NO_TABLET {
			if masterAlias.Cell != "" {
				results <- vresult{alias.String(), fmt.Errorf("tablet already has a master %v", masterAlias)}
			} else {
				masterAlias = alias
			}
		}
	}

	if masterAlias.Cell == "" {
		results <- vresult{keyspace + "/" + shard, fmt.Errorf("no master for shard")}
	} else if shardInfo.MasterAlias != masterAlias {
		results <- vresult{keyspace + "/" + shard, fmt.Errorf("master mismatch for shard: found %v, expected %v", masterAlias, shardInfo.MasterAlias)}
	}

	for _, alias := range aliases {
		wg.Add(1)
		go func(alias topo.TabletAlias) {
			results <- vresult{alias.String(), topo.Validate(wr.ts, alias)}
			wg.Done()
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

func strInList(sl []string, s string) bool {
	for _, x := range sl {
		if x == s {
			return true
		}
	}
	return false
}

func (wr *Wrangler) validateReplication(shardInfo *topo.ShardInfo, tabletMap map[topo.TabletAlias]*topo.TabletInfo, results chan<- vresult) {
	masterTablet, ok := tabletMap[shardInfo.MasterAlias]
	if !ok {
		results <- vresult{shardInfo.MasterAlias.String(), fmt.Errorf("master not in tablet map")}
		return
	}

	slaveList, err := wr.tmc.GetSlaves(context.TODO(), masterTablet, wr.ActionTimeout())
	if err != nil {
		results <- vresult{shardInfo.MasterAlias.String(), err}
		return
	}
	if len(slaveList) == 0 {
		results <- vresult{shardInfo.MasterAlias.String(), fmt.Errorf("no slaves found")}
		return
	}

	// Some addresses don't resolve in all locations, just use IP address
	if err != nil {
		results <- vresult{shardInfo.MasterAlias.String(), fmt.Errorf("resolve slaves failed: %v", err)}
		return
	}

	tabletIpMap := make(map[string]*topo.Tablet)
	slaveIpMap := make(map[string]bool)
	for _, tablet := range tabletMap {
		tabletIpMap[normalizeIP(tablet.IPAddr)] = tablet.Tablet
	}

	// See if every slave is in the replication graph.
	for _, slaveAddr := range slaveList {
		if tabletIpMap[normalizeIP(slaveAddr)] == nil {
			results <- vresult{shardInfo.Keyspace() + "/" + shardInfo.ShardName(), fmt.Errorf("slave not in replication graph: %v (mysql instance without vttablet?)", slaveAddr)}
		}
		slaveIpMap[normalizeIP(slaveAddr)] = true
	}

	// See if every entry in the replication graph is connected to the master.
	for _, tablet := range tabletMap {
		if !tablet.IsSlaveType() {
			continue
		}

		if !slaveIpMap[normalizeIP(tablet.IPAddr)] {
			results <- vresult{tablet.Alias.String(), fmt.Errorf("slave not replicating: %v %q", tablet.IPAddr, slaveList)}
		}
	}
}

func (wr *Wrangler) pingTablets(tabletMap map[topo.TabletAlias]*topo.TabletInfo, wg *sync.WaitGroup, results chan<- vresult) {
	for tabletAlias, tabletInfo := range tabletMap {
		wg.Add(1)
		go func(tabletAlias topo.TabletAlias, tabletInfo *topo.TabletInfo) {
			defer wg.Done()

			if err := wr.ts.ValidateTabletPidNode(tabletAlias); err != nil {
				results <- vresult{tabletAlias.String(), fmt.Errorf("no pid node on %v: %v", tabletInfo.Hostname, err)}
				return
			}

			if err := wr.tmc.Ping(context.TODO(), tabletInfo, wr.ActionTimeout()); err != nil {
				results <- vresult{tabletAlias.String(), fmt.Errorf("Ping failed: %v %v", err, tabletInfo.Hostname)}
			}
		}(tabletAlias, tabletInfo)
	}
}

// Validate a whole TopologyServer tree
func (wr *Wrangler) Validate(pingTablets bool) error {
	// Results from various actions feed here.
	results := make(chan vresult, 16)
	wg := &sync.WaitGroup{}

	// Validate all tablets in all cells, even if they are not discoverable
	// by the replication graph.
	wg.Add(1)
	go func() {
		wr.validateAllTablets(wg, results)
		wg.Done()
	}()

	// Validate replication graph by traversing each keyspace and then each shard.
	keyspaces, err := wr.ts.GetKeyspaces()
	if err != nil {
		results <- vresult{"TopologyServer.GetKeyspaces", err}
	} else {
		for _, keyspace := range keyspaces {
			wg.Add(1)
			go func(keyspace string) {
				wr.validateKeyspace(keyspace, pingTablets, wg, results)
				wg.Done()
			}(keyspace)
		}
	}
	return wr.waitForResults(wg, results)
}

func (wr *Wrangler) ValidateKeyspace(keyspace string, pingTablets bool) error {
	wg := &sync.WaitGroup{}
	results := make(chan vresult, 16)
	wg.Add(1)
	go func() {
		wr.validateKeyspace(keyspace, pingTablets, wg, results)
		wg.Done()
	}()
	return wr.waitForResults(wg, results)
}

func (wr *Wrangler) ValidateShard(keyspace, shard string, pingTablets bool) error {
	wg := &sync.WaitGroup{}
	results := make(chan vresult, 16)
	wg.Add(1)
	go func() {
		wr.validateShard(keyspace, shard, pingTablets, wg, results)
		wg.Done()
	}()
	return wr.waitForResults(wg, results)
}
