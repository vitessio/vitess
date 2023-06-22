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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"

	"golang.org/x/exp/slices"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
	tmc = tmclient.NewTabletManagerClient()
	// Clear existing cache and perform a new refresh.
	if _, err := db.ExecVTOrc("delete from vitess_tablet"); err != nil {
		log.Error(err)
	}
	return time.Tick(time.Second * time.Duration(config.Config.TopoInformationRefreshSeconds)) //nolint SA1015: using time.Tick leaks the underlying ticker
}

// refreshAllTablets reloads the tablets from topo and discovers the ones which haven't been refreshed in a while
func refreshAllTablets() {
	refreshTabletsUsing(func(tabletAlias string) {
		DiscoverInstance(tabletAlias, false /* forceDiscovery */)
	}, false /* forceRefresh */)
}

func refreshTabletsUsing(loader func(tabletAlias string), forceRefresh bool) {
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
				refreshTabletsInKeyspaceShard(refreshCtx, ks.Keyspace, ks.Shard, loader, forceRefresh, nil)
			}(ks)
		}
		wg.Wait()
	}
}

func refreshTabletsInCell(ctx context.Context, cell string, loader func(tabletAlias string), forceRefresh bool) {
	tablets, err := topotools.GetTabletMapForCell(ctx, ts, cell)
	if err != nil {
		log.Errorf("Error fetching topo info for cell %v: %v", cell, err)
		return
	}
	query := "select alias from vitess_tablet where cell = ?"
	args := sqlutils.Args(cell)
	refreshTablets(tablets, query, args, loader, forceRefresh, nil)
}

// forceRefreshAllTabletsInShard is used to refresh all the tablet's information (both MySQL information and topo records)
// for a given shard. This function is meant to be called before or after a cluster-wide operation that we know will
// change the replication information for the entire cluster drastically enough to warrant a full forceful refresh
func forceRefreshAllTabletsInShard(ctx context.Context, keyspace, shard string, tabletsToIgnore []string) {
	refreshCtx, refreshCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer refreshCancel()
	refreshTabletsInKeyspaceShard(refreshCtx, keyspace, shard, func(tabletAlias string) {
		DiscoverInstance(tabletAlias, true)
	}, true, tabletsToIgnore)
}

// refreshTabletInfoOfShard only refreshes the tablet records from the topo-server for all the tablets
// of the given keyspace-shard.
func refreshTabletInfoOfShard(ctx context.Context, keyspace, shard string) {
	log.Infof("refresh of tablet records of shard - %v/%v", keyspace, shard)
	refreshTabletsInKeyspaceShard(ctx, keyspace, shard, func(tabletAlias string) {
		// No-op
		// We only want to refresh the tablet information for the given shard
	}, false, nil)
}

func refreshTabletsInKeyspaceShard(ctx context.Context, keyspace, shard string, loader func(tabletAlias string), forceRefresh bool, tabletsToIgnore []string) {
	tablets, err := ts.GetTabletMapForShard(ctx, keyspace, shard)
	if err != nil {
		log.Errorf("Error fetching tablets for keyspace/shard %v/%v: %v", keyspace, shard, err)
		return
	}
	query := "select alias from vitess_tablet where keyspace = ? and shard = ?"
	args := sqlutils.Args(keyspace, shard)
	refreshTablets(tablets, query, args, loader, forceRefresh, tabletsToIgnore)
}

func refreshTablets(tablets map[string]*topo.TabletInfo, query string, args []any, loader func(tabletAlias string), forceRefresh bool, tabletsToIgnore []string) {
	// Discover new tablets.
	latestInstances := make(map[string]bool)
	var wg sync.WaitGroup
	for _, tabletInfo := range tablets {
		tablet := tabletInfo.Tablet
		if tablet.Type != topodatapb.TabletType_PRIMARY && !topo.IsReplicaType(tablet.Type) {
			continue
		}
		tabletAliasString := topoproto.TabletAliasString(tablet.Alias)
		latestInstances[tabletAliasString] = true
		old, err := inst.ReadTablet(tabletAliasString)
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			if slices.Contains(tabletsToIgnore, topoproto.TabletAliasString(tablet.Alias)) {
				return
			}
			loader(tabletAliasString)
		}()
		log.Infof("Discovered: %v", tablet)
	}
	wg.Wait()

	// Forget tablets that were removed.
	var toForget []string
	err := db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		tabletAlias := row.GetString("alias")
		if !latestInstances[tabletAlias] {
			toForget = append(toForget, tabletAlias)
		}
		return nil
	})
	if err != nil {
		log.Error(err)
	}
	for _, tabletAlias := range toForget {
		if err := inst.ForgetInstance(tabletAlias); err != nil {
			log.Error(err)
		}
	}
}

func getLockAction(analysedInstance string, code inst.AnalysisCode) string {
	return fmt.Sprintf("VTOrc Recovery for %v on %v", code, analysedInstance)
}

// LockShard locks the keyspace-shard preventing others from performing conflicting actions.
func LockShard(ctx context.Context, tabletAlias string, lockAction string) (context.Context, func(*error), error) {
	if tabletAlias == "" {
		return nil, nil, errors.New("can't lock shard: instance is unspecified")
	}
	val := atomic.LoadInt32(&hasReceivedSIGTERM)
	if val > 0 {
		return nil, nil, errors.New("can't lock shard: SIGTERM received")
	}

	tablet, err := inst.ReadTablet(tabletAlias)
	if err != nil {
		return nil, nil, err
	}

	atomic.AddInt32(&shardsLockCounter, 1)
	ctx, unlock, err := ts.TryLockShard(ctx, tablet.Keyspace, tablet.Shard, lockAction)
	if err != nil {
		atomic.AddInt32(&shardsLockCounter, -1)
		return nil, nil, err
	}
	return ctx, func(e *error) {
		defer atomic.AddInt32(&shardsLockCounter, -1)
		unlock(e)
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
		info
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
			opts := prototext.UnmarshalOptions{DiscardUnknown: true}
			return opts.Unmarshal([]byte(m.GetString("info")), primary)
		}
		return nil
	})
	if primary == nil && err == nil {
		err = ErrNoPrimaryTablet
	}
	return primary, err
}

// restartsReplication restarts the replication on the provided replicaKey. It also sets the correct semi-sync settings when it starts replication
func restartReplication(replicaAlias string) error {
	replicaTablet, err := inst.ReadTablet(replicaAlias)
	if err != nil {
		log.Info("Could not read tablet - %+v", replicaAlias)
		return err
	}

	primaryTablet, err := shardPrimary(replicaTablet.Keyspace, replicaTablet.Shard)
	if err != nil {
		log.Info("Could not compute primary for %v/%v", replicaTablet.Keyspace, replicaTablet.Shard)
		return err
	}

	durabilityPolicy, err := inst.GetDurabilityPolicy(replicaTablet.Keyspace)
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
	err = tmc.StartReplication(ctx, replicaTablet, reparentutil.IsReplicaSemiSync(durabilityPolicy, primaryTablet, replicaTablet))
	if err != nil {
		log.Info("Could not start replication on %v", topoproto.TabletAliasString(replicaTablet.Alias))
		return err
	}
	return nil
}
