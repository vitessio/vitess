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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtorc/config"
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
	// shardsToWatch is a map storing the shards for a given keyspace that need to be watched.
	// We store the key range for all the shards that we want to watch.
	// This is populated by parsing `--clusters_to_watch` flag.
	shardsToWatch map[string][]*topodatapb.KeyRange

	// ErrNoPrimaryTablet is a fixed error message.
	ErrNoPrimaryTablet = errors.New("no primary tablet found")
)

// RegisterFlags registers the flags required by VTOrc
func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringSliceVar(&clustersToWatch, "clusters_to_watch", clustersToWatch, "Comma-separated list of keyspaces or keyspace/keyranges that this instance will monitor and repair. Defaults to all clusters in the topology. Example: \"ks1,ks2/-80\"")
	fs.DurationVar(&shutdownWaitTime, "shutdown_wait_time", shutdownWaitTime, "Maximum time to wait for VTOrc to release all the locks that it is holding before shutting down on SIGTERM")
}

// initializeShardsToWatch parses the --clusters_to_watch flag-value
// into a map of keyspace/shards.
func initializeShardsToWatch() error {
	shardsToWatch = make(map[string][]*topodatapb.KeyRange)
	if len(clustersToWatch) == 0 {
		return nil
	}

	for _, ks := range clustersToWatch {
		if strings.Contains(ks, "/") && !strings.HasSuffix(ks, "/") {
			// Validate keyspace/shard parses.
			k, s, err := topoproto.ParseKeyspaceShard(ks)
			if err != nil {
				log.Errorf("Could not parse keyspace/shard %q: %+v", ks, err)
				continue
			}
			if !key.IsValidKeyRange(s) {
				return fmt.Errorf("invalid key range %q while parsing clusters to watch", s)
			}
			// Parse the shard name into key range value.
			keyRanges, err := key.ParseShardingSpec(s)
			if err != nil {
				return fmt.Errorf("could not parse shard name %q: %+v", s, err)
			}
			shardsToWatch[k] = append(shardsToWatch[k], keyRanges...)
		} else {
			// Remove trailing slash if exists.
			ks = strings.TrimSuffix(ks, "/")
			// We store the entire range of key range if nothing is specified.
			shardsToWatch[ks] = []*topodatapb.KeyRange{key.NewCompleteKeyRange()}
		}
	}

	if len(shardsToWatch) == 0 {
		log.Error("No keyspace/shards to watch, watching all keyspaces")
	}
	return nil
}

// shouldWatchTablet checks if the given tablet is part of the watch list.
func shouldWatchTablet(tablet *topodatapb.Tablet) bool {
	// If we are watching all keyspaces, then we want to watch this tablet too.
	if len(shardsToWatch) == 0 {
		return true
	}
	shardRanges, ok := shardsToWatch[tablet.GetKeyspace()]
	// If we don't have the keyspace in our map, then this tablet
	// doesn't need to be watched.
	if !ok {
		return false
	}
	// Get the tablet's key range, and check if
	// it is part of the shard ranges we are watching.
	kr := tablet.GetKeyRange()
	for _, shardRange := range shardRanges {
		if key.KeyRangeContainsKeyRange(shardRange, kr) {
			return true
		}
	}
	return false
}

// OpenTabletDiscovery opens the vitess topo if enables and returns a ticker
// channel for polling.
func OpenTabletDiscovery() <-chan time.Time {
	// TODO(sougou): If there's a shutdown signal, we have to close the topo.
	ts = topo.Open()
	tmc = inst.InitializeTMC()
	// Clear existing cache and perform a new refresh.
	if _, err := db.ExecVTOrc("delete from vitess_tablet"); err != nil {
		log.Error(err)
	}
	// Parse --clusters_to_watch into a filter.
	err := initializeShardsToWatch()
	if err != nil {
		log.Fatalf("Error parsing --clusters-to-watch: %v", err)
	}
	// We refresh all information from the topo once before we start the ticks to do
	// it on a timer.
	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()
	if err := refreshAllInformation(ctx); err != nil {
		log.Errorf("failed to initialize topo information: %+v", err)
	}
	return time.Tick(time.Second * time.Duration(config.Config.TopoInformationRefreshSeconds)) //nolint SA1015: using time.Tick leaks the underlying ticker
}

// getAllTablets gets all tablets from all cells using a goroutine per cell.
func getAllTablets(ctx context.Context, cells []string) []*topo.TabletInfo {
	var tabletsMu sync.Mutex
	tablets := make([]*topo.TabletInfo, 0)
	eg, ctx := errgroup.WithContext(ctx)
	for _, cell := range cells {
		eg.Go(func() error {
			t, err := ts.GetTabletsByCell(ctx, cell, nil)
			if err != nil {
				log.Errorf("Failed to load tablets from cell %s: %+v", cell, err)
				return nil
			}
			tabletsMu.Lock()
			defer tabletsMu.Unlock()
			tablets = append(tablets, t...)
			return nil
		})
	}
	_ = eg.Wait() // always nil
	return tablets
}

// refreshAllTablets reloads the tablets from topo and discovers the ones which haven't been refreshed in a while
func refreshAllTablets(ctx context.Context) error {
	return refreshTabletsUsing(ctx, func(tabletAlias string) {
		DiscoverInstance(tabletAlias, false /* forceDiscovery */)
	}, false /* forceRefresh */)
}

// refreshTabletsUsing refreshes tablets using a provided loader.
func refreshTabletsUsing(ctx context.Context, loader func(tabletAlias string), forceRefresh bool) error {
	if !IsLeaderOrActive() {
		return nil
	}

	// Get all cells.
	ctx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	cells, err := ts.GetKnownCells(ctx)
	if err != nil {
		return err
	}

	// Get all tablets from all cells.
	getTabletsCtx, getTabletsCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer getTabletsCancel()
	tablets := getAllTablets(getTabletsCtx, cells)
	if len(tablets) == 0 {
		log.Error("Found no tablets")
		return nil
	}

	// Filter tablets that should not be watched using shardsToWatch map.
	matchedTablets := make([]*topo.TabletInfo, 0, len(tablets))
	func() {
		for _, t := range tablets {
			if shouldWatchTablet(t.Tablet) {
				matchedTablets = append(matchedTablets, t)
			}
		}
	}()

	// Refresh the filtered tablets.
	query := "select alias from vitess_tablet"
	refreshTablets(matchedTablets, query, nil, loader, forceRefresh, nil)
	return nil
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
	tablets, err := ts.GetTabletsByShard(ctx, keyspace, shard)
	if err != nil {
		log.Errorf("Error fetching tablets for keyspace/shard %v/%v: %v", keyspace, shard, err)
		return
	}
	query := "select alias from vitess_tablet where keyspace = ? and shard = ?"
	args := sqlutils.Args(keyspace, shard)
	refreshTablets(tablets, query, args, loader, forceRefresh, tabletsToIgnore)
}

func refreshTablets(tablets []*topo.TabletInfo, query string, args []any, loader func(tabletAlias string), forceRefresh bool, tabletsToIgnore []string) {
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
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.UndoDemotePrimary(tmcCtx, tablet, semiSync)
}

// setReadOnly calls the said RPC for the given tablet
func setReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.SetReadOnly(tmcCtx, tablet)
}

// changeTabletType calls the said RPC for the given tablet with the given parameters.
func changeTabletType(ctx context.Context, tablet *topodatapb.Tablet, tabletType topodatapb.TabletType, semiSync bool) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.ChangeType(tmcCtx, tablet, tabletType, semiSync)
}

// resetReplicationParameters resets the replication parameters on the given tablet.
func resetReplicationParameters(ctx context.Context, tablet *topodatapb.Tablet) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.ResetReplicationParameters(tmcCtx, tablet)
}

// setReplicationSource calls the said RPC with the parameters provided
func setReplicationSource(ctx context.Context, replica *topodatapb.Tablet, primary *topodatapb.Tablet, semiSync bool) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tmc.SetReplicationSource(tmcCtx, replica, primary.Alias, 0, "", true, semiSync)
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
		log.Info("Could not stop replication on %v", replicaAlias)
		return err
	}
	err = tmc.StartReplication(ctx, replicaTablet, reparentutil.IsReplicaSemiSync(durabilityPolicy, primaryTablet, replicaTablet))
	if err != nil {
		log.Info("Could not start replication on %v", replicaAlias)
		return err
	}
	return nil
}
