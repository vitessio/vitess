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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"

	"vitess.io/vitess/go/vt/vtorc/config"

	"github.com/openark/golib/sqlutils"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	ts                *topo.Server
	tmc               tmclient.TabletManagerClient
	clustersToWatch   []string
	shutdownWaitTime  = 30 * time.Second
	shardsLockCounter int32
	// ErrNoPrimaryTablet is a fixed error message.
	ErrNoPrimaryTablet = errors.New("no primary tablet found")
)

// RegisterFlags registers the flags required by VTOrc
func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&clustersToWatch, "clusters_to_watch", clustersToWatch, "Comma-separated list of keyspaces or keyspace/shards that this instance will monitor and repair. Defaults to all clusters in the topology. Example: \"ks1,ks2/-80\"")
	fs.DurationVar(&shutdownWaitTime, "shutdown_wait_time", shutdownWaitTime, "Maximum time to wait for VTOrc to release all the locks that it is holding before shutting down on SIGTERM")
}

// OpenTabletDiscovery opens the vitess topo if enables and returns a ticker
// channel for polling.
func OpenTabletDiscovery() <-chan time.Time {
	// TODO(sougou): If there's a shutdown signal, we have to close the topo.
	ts = topo.Open()
	// TODO(sougou): remove ts and push some functions into inst.
	inst.TopoServ = ts
	tmc = tmclient.NewTabletManagerClient()
	// Clear existing cache and perform a new refresh.
	if _, err := db.ExecVTOrc("delete from vitess_tablet"); err != nil {
		log.Error(err)
	}
	refreshTabletsUsing(func(instanceKey *inst.InstanceKey) {
		_ = inst.InjectSeed(instanceKey)
	}, false /* forceRefresh */)
	return time.Tick(time.Second * time.Duration(config.Config.TopoInformationRefreshSeconds)) //nolint SA1015: using time.Tick leaks the underlying ticker
}

// refreshAllTablets reloads the tablets from topo and discovers the ones which haven't been refreshed in a while
func refreshAllTablets() {
	refreshTabletsUsing(func(instanceKey *inst.InstanceKey) {
		DiscoverInstance(*instanceKey, false /* forceDiscovery */)
	}, false /* forceRefresh */)
}

func refreshTabletsUsing(loader func(instanceKey *inst.InstanceKey), forceRefresh bool) {
	if !IsLeaderOrActive() {
		return
	}
	if len(clustersToWatch) == 0 { // all known clusters
		ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
		defer cancel()
		cells, err := ts.GetKnownCells(ctx)
		if err != nil {
			log.Error(err)
			return
		}

		refreshCtx, refreshCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
		defer refreshCancel()
		var wg sync.WaitGroup
		for _, cell := range cells {
			wg.Add(1)
			go func(cell string) {
				defer wg.Done()
				refreshTabletsInCell(refreshCtx, cell, loader, forceRefresh)
			}(cell)
		}
		wg.Wait()
	} else {
		// Parse input and build list of keyspaces / shards
		var keyspaceShards []*topo.KeyspaceShard
		for _, ks := range clustersToWatch {
			if strings.Contains(ks, "/") {
				// This is a keyspace/shard specification
				input := strings.Split(ks, "/")
				keyspaceShards = append(keyspaceShards, &topo.KeyspaceShard{Keyspace: input[0], Shard: input[1]})
			} else {
				// Assume this is a keyspace and find all shards in keyspace
				ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
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
			log.Errorf("Found no keyspaceShards for input: %+v", clustersToWatch)
			return
		}
		refreshCtx, refreshCancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
		defer refreshCancel()
		var wg sync.WaitGroup
		for _, ks := range keyspaceShards {
			wg.Add(1)
			go func(ks *topo.KeyspaceShard) {
				defer wg.Done()
				refreshTabletsInKeyspaceShard(refreshCtx, ks.Keyspace, ks.Shard, loader, forceRefresh)
			}(ks)
		}
		wg.Wait()
	}
}

func refreshTabletsInCell(ctx context.Context, cell string, loader func(instanceKey *inst.InstanceKey), forceRefresh bool) {
	tablets, err := topotools.GetTabletMapForCell(ctx, ts, cell)
	if err != nil {
		log.Errorf("Error fetching topo info for cell %v: %v", cell, err)
		return
	}
	query := "select hostname, port, info from vitess_tablet where cell = ?"
	args := sqlutils.Args(cell)
	refreshTablets(tablets, query, args, loader, forceRefresh)
}

// forceRefreshAllTabletsInShard is used to refresh all the tablet's information (both MySQL information and topo records)
// for a given shard. This function is meant to be called before or after a cluster-wide operation that we know will
// change the replication information for the entire cluster drastically enough to warrant a full forceful refresh
func forceRefreshAllTabletsInShard(ctx context.Context, keyspace, shard string) {
	log.Infof("force refresh of all tablets in shard - %v/%v", keyspace, shard)
	refreshCtx, refreshCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer refreshCancel()
	refreshTabletsInKeyspaceShard(refreshCtx, keyspace, shard, func(instanceKey *inst.InstanceKey) {
		DiscoverInstance(*instanceKey, true)
	}, true)
}

// refreshTabletInfoOfShard only refreshes the tablet records from the topo-server for all the tablets
// of the given keyspace-shard.
func refreshTabletInfoOfShard(ctx context.Context, keyspace, shard string) {
	log.Infof("refresh of tablet records of shard - %v/%v", keyspace, shard)
	refreshTabletsInKeyspaceShard(ctx, keyspace, shard, func(instanceKey *inst.InstanceKey) {
		// No-op
		// We only want to refresh the tablet information for the given shard
	}, false)
}

func refreshTabletsInKeyspaceShard(ctx context.Context, keyspace, shard string, loader func(instanceKey *inst.InstanceKey), forceRefresh bool) {
	tablets, err := ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		log.Errorf("Error fetching tablets for keyspace/shard %v/%v: %v", keyspace, shard, err)
		return
	}
	query := "select hostname, port, info from vitess_tablet where keyspace = ? and shard = ?"
	args := sqlutils.Args(keyspace, shard)
	refreshTablets(tablets, query, args, loader, forceRefresh)
}

func refreshTablets(tablets map[string]*topo.TabletInfo, query string, args []any, loader func(instanceKey *inst.InstanceKey), forceRefresh bool) {
	// Discover new tablets.
	// TODO(sougou): enhance this to work with multi-schema,
	// where each instanceKey can have multiple tablets.
	latestInstances := make(map[inst.InstanceKey]bool)
	for _, tabletInfo := range tablets {
		tablet := tabletInfo.Tablet
		if tablet.MysqlHostname == "" {
			continue
		}
		if tablet.Type != topodatapb.TabletType_PRIMARY && !topo.IsReplicaType(tablet.Type) {
			continue
		}
		instanceKey := inst.InstanceKey{
			Hostname: tablet.MysqlHostname,
			Port:     int(tablet.MysqlPort),
		}
		latestInstances[instanceKey] = true
		old, err := inst.ReadTablet(instanceKey)
		if err != nil && err != inst.ErrTabletAliasNil {
			log.Error(err)
			continue
		}
		if !forceRefresh && proto.Equal(tablet, old) {
			continue
		}
		if err := inst.SaveTablet(tablet); err != nil {
			log.Error(err)
			continue
		}
		loader(&instanceKey)
		log.Infof("Discovered: %v", tablet)
	}

	// Forget tablets that were removed.
	toForget := make(map[inst.InstanceKey]*topodatapb.Tablet)
	err := db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		curKey := inst.InstanceKey{
			Hostname: row.GetString("hostname"),
			Port:     row.GetInt("port"),
		}
		if !latestInstances[curKey] {
			tablet := &topodatapb.Tablet{}
			if err := prototext.Unmarshal([]byte(row.GetString("info")), tablet); err != nil {
				log.Error(err)
				return nil
			}
			toForget[curKey] = tablet
		}
		return nil
	})
	if err != nil {
		log.Error(err)
	}
	for instanceKey, tablet := range toForget {
		log.Infof("Forgetting: %v", tablet)
		_, err := db.ExecVTOrc(`
					delete
						from vitess_tablet
					where
						hostname=? and port=?`,
			instanceKey.Hostname,
			instanceKey.Port,
		)
		if err != nil {
			log.Error(err)
		}
		if err := inst.ForgetInstance(&instanceKey); err != nil {
			log.Error(err)
		}
	}
}

// LockShard locks the keyspace-shard preventing others from performing conflicting actions.
func LockShard(ctx context.Context, instanceKey inst.InstanceKey) (context.Context, func(*error), error) {
	if instanceKey.Hostname == "" {
		return nil, nil, errors.New("Can't lock shard: instance is unspecified")
	}
	val := atomic.LoadInt32(&hasReceivedSIGTERM)
	if val > 0 {
		return nil, nil, errors.New("Can't lock shard: SIGTERM received")
	}

	tablet, err := inst.ReadTablet(instanceKey)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(config.Config.LockShardTimeoutSeconds)*time.Second)
	atomic.AddInt32(&shardsLockCounter, 1)
	ctx, unlock, err := ts.LockShard(ctx, tablet.Keyspace, tablet.Shard, "Orc Recovery")
	if err != nil {
		cancel()
		atomic.AddInt32(&shardsLockCounter, -1)
		return nil, nil, err
	}
	return ctx, func(e *error) {
		defer atomic.AddInt32(&shardsLockCounter, -1)
		unlock(e)
		cancel()
	}, nil
}

// tabletUndoDemotePrimary calls the said RPC for the given tablet.
func tabletUndoDemotePrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	return tmc.UndoDemotePrimary(ctx, tablet, semiSync)
}

// setReadOnly calls the said RPC for the given tablet
func setReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error {
	return tmc.SetReadOnly(ctx, tablet)
}

// setReplicationSource calls the said RPC with the parameters provided
func setReplicationSource(ctx context.Context, replica *topodatapb.Tablet, primary *topodatapb.Tablet, semiSync bool) error {
	return tmc.SetReplicationSource(ctx, replica, primary.Alias, 0, "", true, semiSync)
}

// shardPrimary finds the primary of the given keyspace-shard by reading the vtorc backend
func shardPrimary(keyspace string, shard string) (primary *topodatapb.Tablet, err error) {
	query := `SELECT
		info,
		hostname,
		port,
		tablet_type,
		primary_timestamp
	FROM 
		vitess_tablet
	WHERE
		keyspace = ? AND shard = ?
		AND tablet_type = ?
	ORDER BY
		primary_timestamp DESC
	LIMIT 1
`
	err = db.Db.QueryVTOrc(query, sqlutils.Args(keyspace, shard, topodatapb.TabletType_PRIMARY), func(m sqlutils.RowMap) error {
		if primary == nil {
			primary = &topodatapb.Tablet{}
			return prototext.Unmarshal([]byte(m.GetString("info")), primary)
		}
		return nil
	})
	if primary == nil && err == nil {
		err = ErrNoPrimaryTablet
	}
	return primary, err
}

// restartsReplication restarts the replication on the provided replicaKey. It also sets the correct semi-sync settings when it starts replication
func restartReplication(replicaKey *inst.InstanceKey) error {
	replicaTablet, err := inst.ReadTablet(*replicaKey)
	if err != nil {
		log.Info("Could not read tablet - %+v", replicaKey)
		return err
	}

	primaryTablet, err := shardPrimary(replicaTablet.Keyspace, replicaTablet.Shard)
	if err != nil {
		log.Info("Could not compute primary for %v/%v", replicaTablet.Keyspace, replicaTablet.Shard)
		return err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(replicaTablet)
	if err != nil {
		log.Info("Could not read the durability policy for %v/%v", replicaTablet.Keyspace, replicaTablet.Shard)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Config.WaitReplicasTimeoutSeconds)*time.Second)
	defer cancel()
	err = tmc.StopReplication(ctx, replicaTablet)
	if err != nil {
		log.Info("Could not stop replication on %v", topoproto.TabletAliasString(replicaTablet.Alias))
		return err
	}
	err = tmc.StartReplication(ctx, replicaTablet, inst.IsReplicaSemiSync(durabilityPolicy, primaryTablet, replicaTablet))
	if err != nil {
		log.Info("Could not start replication on %v", topoproto.TabletAliasString(replicaTablet.Alias))
		return err
	}
	return nil
}
