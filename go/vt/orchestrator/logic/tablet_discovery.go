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
	"fmt"
	"sync"
	"time"

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
	ts *topo.Server
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
	cells, err := ts.GetKnownCells(context.TODO())
	if err != nil {
		log.Errore(err)
		return
	}

	var wg sync.WaitGroup
	for _, cell := range cells {
		wg.Add(1)
		go func(cell string) {
			defer wg.Done()
			refreshTabletsInCell(cell, loader)
		}(cell)
	}
	wg.Wait()
}

func refreshTabletsInCell(cell string, loader func(instanceKey *inst.InstanceKey)) {
	latestInstances := make(map[inst.InstanceKey]bool)
	tablets, err := topotools.GetAllTablets(context.TODO(), ts, cell)
	if err != nil {
		log.Errorf("Error fetching topo info for cell %v: %v", cell, err)
		return
	}

	// Discover new tablets.
	// TODO(sougou): enhance this to work with multi-schema,
	// where each instanceKey can have multiple tablets.
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
	query := "select hostname, port, info from vitess_tablet where cell = ?"
	err = db.QueryOrchestrator(query, sqlutils.Args(cell), func(row sqlutils.RowMap) error {
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
		log.Infof("Forgeting: %v", tablet)
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
	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
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
	ti, err := ts.GetTablet(context.TODO(), tablet.Alias)
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
	ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Second)
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
	si, err := ts.GetShard(context.TODO(), tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, err
	}
	if !si.HasMaster() {
		return nil, fmt.Errorf("no master tablet for shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	master, err := ts.GetTablet(context.TODO(), si.MasterAlias)
	if err != nil {
		return nil, err
	}
	return &inst.InstanceKey{
		Hostname: master.MysqlHostname,
		Port:     int(master.MysqlPort),
	}, nil
}
