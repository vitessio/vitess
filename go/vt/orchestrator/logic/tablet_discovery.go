/*
Copyright 2020 The Vitess Authors.

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

package logic

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	ts              *topo.Server
	clustersToWatch = flag.String("clusters_to_watch", "", "Comma-separated list of keyspaces or keyspace/shards that this instance will monitor and repair. Defaults to all clusters in the topology. Example: \"ks1,ks2/-80\"")
)

// OpenTabletDiscovery opens the vitess topo if enables and returns a ticker
// channel for polling.
func OpenTabletDiscovery() <-chan time.Time {
	// TODO(sougou): If there's a shutdown signal, we have to close the topo.
	ts = topo.Open()
	// TODO(sougou): remove ts and push some functions into inst.
	inst.TopoServ = ts
	// Clear existing cache and perform a new refresh.
	if _, err := db.ExecOrchestrator("delete from vitess_tablet"); err != nil {
		log.Errore(err)
	}
	refreshTabletsUsing(func(instanceKey *inst.InstanceKey) {
		_ = inst.InjectSeed(instanceKey)
	})
	// TODO(sougou): parameterize poll interval.
	return time.Tick(15 * time.Second) //nolint SA1015: using time.Tick leaks the underlying ticker
}

// RefreshTablets reloads the tablets from topo.
func RefreshTablets() {
	refreshTabletsUsing(func(instanceKey *inst.InstanceKey) {
		DiscoverInstance(*instanceKey)
	})
}

func refreshTabletsUsing(loader func(instanceKey *inst.InstanceKey)) {
	if !IsLeaderOrActive() {
		return
	}
	if *clustersToWatch == "" { // all known clusters
		ctx, cancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
		defer cancel()
		cells, err := ts.GetKnownCells(ctx)
		if err != nil {
			log.Errore(err)
			return
		}

		refreshCtx, refreshCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
		defer refreshCancel()
		var wg sync.WaitGroup
		for _, cell := range cells {
			wg.Add(1)
			go func(cell string) {
				defer wg.Done()
				refreshTabletsInCell(refreshCtx, cell, loader)
			}(cell)
		}
		wg.Wait()
	} else {
		// Parse input and build list of keyspaces / shards
		var keyspaceShards []*topo.KeyspaceShard
		inputs := strings.Split(*clustersToWatch, ",")
		for _, ks := range inputs {
			if strings.Contains(ks, "/") {
				// This is a keyspace/shard specification
				input := strings.Split(ks, "/")
				keyspaceShards = append(keyspaceShards, &topo.KeyspaceShard{Keyspace: input[0], Shard: input[1]})
			} else {
				// Assume this is a keyspace and find all shards in keyspace
				ctx, cancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
				defer cancel()
				shards, err := ts.GetShardNames(ctx, ks)
				if err != nil {
					// Log the errr and continue
					log.Errorf("Error fetching shards for keyspace: %v", ks)
					continue
				}
				if len(shards) == 0 {
					log.Errorf("Topo has no shards for ks: %v", ks)
					continue
				}
				for _, s := range shards {
					keyspaceShards = append(keyspaceShards, &topo.KeyspaceShard{Keyspace: ks, Shard: s})
				}
			}
		}
		if len(keyspaceShards) == 0 {
			log.Errorf("Found no keyspaceShards for input: %v", *clustersToWatch)
			return
		}
		refreshCtx, refreshCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
		defer refreshCancel()
		var wg sync.WaitGroup
		for _, ks := range keyspaceShards {
			wg.Add(1)
			go func(ks *topo.KeyspaceShard) {
				defer wg.Done()
				refreshTabletsInKeyspaceShard(refreshCtx, ks.Keyspace, ks.Shard, loader)
			}(ks)
		}
		wg.Wait()
	}
}

func refreshTabletsInCell(ctx context.Context, cell string, loader func(instanceKey *inst.InstanceKey)) {
	tablets, err := topotools.GetTabletMapForCell(ctx, ts, cell)
	if err != nil {
		log.Errorf("Error fetching topo info for cell %v: %v", cell, err)
		return
	}
	query := "select hostname, port, info from vitess_tablet where cell = ?"
	args := sqlutils.Args(cell)
	refreshTablets(tablets, query, args, loader)
}

func refreshTabletsInKeyspaceShard(ctx context.Context, keyspace, shard string, loader func(instanceKey *inst.InstanceKey)) {
	tablets, err := ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		log.Errorf("Error fetching tablets for keyspace/shard %v/%v: %v", keyspace, shard, err)
		return
	}
	query := "select hostname, port, info from vitess_tablet where keyspace = ? and shard = ?"
	args := sqlutils.Args(keyspace, shard)
	refreshTablets(tablets, query, args, loader)
}

func refreshTablets(tablets map[string]*topo.TabletInfo, query string, args []interface{}, loader func(instanceKey *inst.InstanceKey)) {
	// Discover new tablets.
	// TODO(sougou): enhance this to work with multi-schema,
	// where each instanceKey can have multiple tablets.
	latestInstances := make(map[inst.InstanceKey]bool)
	for _, tabletInfo := range tablets {
		tablet := tabletInfo.Tablet
		if tablet.MysqlHostname == "" {
			continue
		}
		if tablet.Type != topodatapb.TabletType_MASTER && !topo.IsReplicaType(tablet.Type) {
			continue
		}
		instanceKey := inst.InstanceKey{
			Hostname: tablet.MysqlHostname,
			Port:     int(tablet.MysqlPort),
		}
		latestInstances[instanceKey] = true
		old, err := inst.ReadTablet(instanceKey)
		if err != nil && err != inst.ErrTabletAliasNil {
			log.Errore(err)
			continue
		}
		if proto.Equal(tablet, old) {
			continue
		}
		if err := inst.SaveTablet(tablet); err != nil {
			log.Errore(err)
			continue
		}
		loader(&instanceKey)
		log.Infof("Discovered: %v", tablet)
	}

	// Forget tablets that were removed.
	toForget := make(map[inst.InstanceKey]*topodatapb.Tablet)
	err := db.QueryOrchestrator(query, args, func(row sqlutils.RowMap) error {
		curKey := inst.InstanceKey{
			Hostname: row.GetString("hostname"),
			Port:     row.GetInt("port"),
		}
		if !latestInstances[curKey] {
			tablet := &topodatapb.Tablet{}
			if err := proto.UnmarshalText(row.GetString("info"), tablet); err != nil {
				log.Errore(err)
				return nil
			}
			toForget[curKey] = tablet
		}
		return nil
	})
	if err != nil {
		log.Errore(err)
	}
	for instanceKey, tablet := range toForget {
		log.Infof("Forgetting: %v", tablet)
		_, err := db.ExecOrchestrator(`
					delete
						from vitess_tablet
					where
						hostname=? and port=?`,
			instanceKey.Hostname,
			instanceKey.Port,
		)
		if err != nil {
			log.Errore(err)
		}
		if err := inst.ForgetInstance(&instanceKey); err != nil {
			log.Errore(err)
		}
	}
}

// LockShard locks the keyspace-shard preventing others from performing conflicting actions.
func LockShard(instanceKey inst.InstanceKey) (func(*error), error) {
	if instanceKey.Hostname == "" {
		return nil, errors.New("Can't lock shard: instance is unspecified")
	}

	tablet, err := inst.ReadTablet(instanceKey)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Config.LockShardTimeoutSeconds)*time.Second)
	defer cancel()
	_, unlock, err := ts.LockShard(ctx, tablet.Keyspace, tablet.Shard, "Orc Recovery")
	return unlock, err
}

// TabletRefresh refreshes the tablet info.
func TabletRefresh(instanceKey inst.InstanceKey) (*topodatapb.Tablet, error) {
	tablet, err := inst.ReadTablet(instanceKey)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer cancel()
	ti, err := ts.GetTablet(ctx, tablet.Alias)
	if err != nil {
		return nil, err
	}
	if err := inst.SaveTablet(ti.Tablet); err != nil {
		return nil, err
	}
	return ti.Tablet, nil
}

// TabletDemoteMaster requests the master tablet to stop accepting transactions.
func TabletDemoteMaster(instanceKey inst.InstanceKey) error {
	return tabletDemoteMaster(instanceKey, true)
}

// TabletUndoDemoteMaster requests the master tablet to undo the demote.
func TabletUndoDemoteMaster(instanceKey inst.InstanceKey) error {
	return tabletDemoteMaster(instanceKey, false)
}

func tabletDemoteMaster(instanceKey inst.InstanceKey, forward bool) error {
	if instanceKey.Hostname == "" {
		return errors.New("Can't demote/undo master: instance is unspecified")
	}
	tablet, err := inst.ReadTablet(instanceKey)
	if err != nil {
		return err
	}
	tmc := tmclient.NewTabletManagerClient()
	// TODO(sougou): this should be controllable because we may want
	// to give a longer timeout for a graceful takeover.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if forward {
		_, err = tmc.DemoteMaster(ctx, tablet)
	} else {
		err = tmc.UndoDemoteMaster(ctx, tablet)
	}
	return err
}

func ShardMaster(instanceKey *inst.InstanceKey) (masterKey *inst.InstanceKey, err error) {
	tablet, err := inst.ReadTablet(*instanceKey)
	if err != nil {
		return nil, err
	}
	sCtx, sCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer sCancel()
	si, err := ts.GetShard(sCtx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, err
	}
	if !si.HasMaster() {
		return nil, fmt.Errorf("no master tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	tCtx, tCancel := context.WithTimeout(context.Background(), *topo.RemoteOperationTimeout)
	defer tCancel()
	master, err := ts.GetTablet(tCtx, si.MasterAlias)
	if err != nil {
		return nil, err
	}
	return &inst.InstanceKey{
		Hostname: master.MysqlHostname,
		Port:     int(master.MysqlPort),
	}, nil
}
