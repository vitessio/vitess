package schemaswap

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/logutil"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
)

var (
	delayBetweenErrors = flag.Duration("schema_swap_delay_between_errors", time.Minute,
		"time to wait after a retryable error happened in the schema swap process")
	adminQueryTimeout = flag.Duration("schema_swap_admin_query_timeout", 30*time.Second,
		"timeout for SQL queries used to save and retrieve meta information for schema swap process")
)

// Swap contains meta-information and methods controlling schema swap process as a whole.
type Swap struct {
	// ctx is the context of the whole schema swap process. Once this context is cancelled
	// the schema swap process stops.
	ctx context.Context
	// keyspace is the name of the keyspace on which schema swap process operates.
	keyspace string
	// swapID is the id of the executed schema swap. This is recorded in the database to
	// distinguish backups from before and after an applied schema change, and to prevent
	// several schema swaps from running at the same time.
	swapID uint64
	// topoServer is the topo server implementation used to discover the topology of the keyspace.
	topoServer topo.Server
	// tabletClient is the client implementation used by the schema swap process to control
	// all tablets in the keyspace.
	tabletClient tmclient.TabletManagerClient
}

// shardSchemaSwap contains data related to schema swap happening on a specific shard.
type shardSchemaSwap struct {
	// parent is the structure with meta-information about the whole schema swap process.
	parent *Swap
	// shard is the name of the shard this struct operates on.
	shard string

	// tabletHealthCheck watches after the healthiness of all tablets in the shard.
	tabletHealthCheck discovery.HealthCheck
	// tabletWatchers contains list of topology watchers monitoring changes in the shard
	// topology. There are several of them because the watchers are per-cell.
	tabletWatchers []*discovery.TopologyWatcher

	// allTabletsLock is a mutex protecting access to contents of health check related
	// variables below.
	allTabletsLock sync.RWMutex
	// allTablets is the list of all tablets on the shard mapped by the key provided
	// by discovery. The contents of the map is guarded by allTabletsLock.
	allTablets map[string]*discovery.TabletStats
	// healthWaitingTablet is a key (the same key as used in allTablets) of a tablet that
	// is currently being waited on to become healthy and to catch up with replication.
	// The variable is guarded by allTabletsLock.
	healthWaitingTablet string
	// healthWaitingChannel is a channel that should be closed when the tablet that is
	// currently being waited on becomes healthy and catches up with replication. The
	// variable is set to nil when nothing is waited on at the moment. The variable is
	// protected by allTabletsLock.
	healthWaitingChannel *chan interface{}
}

// startHealthWatchers launches the topology watchers and health checking to monitor
// all tablets on the shard. Function should be called before the start of the schema
// swap process.
func (shardSwap *shardSchemaSwap) startHealthWatchers() error {
	shardSwap.tabletHealthCheck = discovery.NewHealthCheck(
		*vtctl.HealthCheckTopologyRefresh, *vtctl.HealthcheckRetryDelay, *vtctl.HealthCheckTimeout)
	shardSwap.tabletHealthCheck.SetListener(shardSwap, true /* sendDownEvents */)

	topoServer := shardSwap.parent.topoServer
	cellList, err := topoServer.GetKnownCells(shardSwap.parent.ctx)
	if err != nil {
		return err
	}
	for _, cell := range cellList {
		watcher := discovery.NewShardReplicationWatcher(
			topoServer,
			shardSwap.tabletHealthCheck,
			cell,
			shardSwap.parent.keyspace,
			shardSwap.shard,
			*vtctl.HealthCheckTimeout,
			discovery.DefaultTopoReadConcurrency)
		shardSwap.tabletWatchers = append(shardSwap.tabletWatchers, watcher)
	}
	for _, watcher := range shardSwap.tabletWatchers {
		if err := watcher.WaitForInitialTopology(); err != nil {
			return err
		}
	}
	shardSwap.tabletHealthCheck.WaitForInitialStatsUpdates()
	return nil
}

// stopHealthWatchers stops the health checking and topology monitoring. The function
// should be called when schema swap process has finished (successfully or with error)
// and this struct is going to be disposed.
func (shardSwap *shardSchemaSwap) stopHealthWatchers() {
	for _, watcher := range shardSwap.tabletWatchers {
		watcher.Stop()
	}
	err := shardSwap.tabletHealthCheck.Close()
	if err != nil {
		log.Errorf("Error closing health checking: %v", err)
	}
}

// isTabletHealthy verifies that the given TabletStats represents a healthy tablet that is
// caught up with replication to a serving level.
func isTabletHealthy(tabletStats *discovery.TabletStats) bool {
	return tabletStats.Stats.HealthError == "" && !discovery.IsReplicationLagHigh(tabletStats)
}

// startWaitingOnUnhealthyTablet registers the tablet as being waited on in a way that
// doesn't race with StatsUpdate(). If the tablet is already healthy then the function
// will return nil as the channel and nil as the error. If the tablet is unhealthy now
// then function will return the channel that will be closed once tablet becomes healthy
// and caught up with replication. Note that the channel is returned so that the caller
// could wait on it without necessity to lock allTabletsLock.
func (shardSwap *shardSchemaSwap) startWaitingOnUnhealthyTablet(tablet *topodatapb.Tablet) (*chan interface{}, error) {
	shardSwap.allTabletsLock.Lock()
	defer shardSwap.allTabletsLock.Unlock()

	tabletKey := discovery.TabletToMapKey(tablet)
	tabletStats, tabletFound := shardSwap.allTablets[tabletKey]
	if !tabletFound {
		return nil, fmt.Errorf("Tablet %v has disappeared while doing schema swap", tablet.Alias)
	}
	if isTabletHealthy(tabletStats) {
		return nil, nil
	}
	waitingChannel := make(chan interface{})
	shardSwap.healthWaitingChannel = &waitingChannel
	shardSwap.healthWaitingTablet = tabletKey
	return shardSwap.healthWaitingChannel, nil
}

// checkWaitingTabletHealthiness verifies whether the provided TabletStats represent the
// tablet that is being waited to become healthy, and notifies the waiting go routine if
// it is the tablet and if it is healthy now.
// The function should be called with shardSwap.allTabletsLock mutex locked.
func (shardSwap *shardSchemaSwap) checkWaitingTabletHealthiness(tabletStats *discovery.TabletStats) {
	if shardSwap.healthWaitingTablet == tabletStats.Key && isTabletHealthy(tabletStats) {
		close(*shardSwap.healthWaitingChannel)
		shardSwap.healthWaitingChannel = nil
		shardSwap.healthWaitingTablet = ""
	}
}

// StatsUpdate is the part of discovery.HealthCheckStatsListener interface. It makes sure
// that when a change of tablet health happens it's recorded in allTablets list, and if
// this is the tablet that is being waited for after restore, the function wakes up the
// waiting go routine.
func (shardSwap *shardSchemaSwap) StatsUpdate(newTabletStats *discovery.TabletStats) {
	shardSwap.allTabletsLock.Lock()
	defer shardSwap.allTabletsLock.Unlock()

	existingStats, found := shardSwap.allTablets[newTabletStats.Key]
	if newTabletStats.Up {
		if found {
			*existingStats = *newTabletStats
		} else {
			shardSwap.allTablets[newTabletStats.Key] = newTabletStats
		}
		shardSwap.checkWaitingTabletHealthiness(newTabletStats)
	} else {
		delete(shardSwap.allTablets, newTabletStats.Key)
	}
}

// getTabletList returns the list of all known tablets in the shard so that the caller
// could operate with it without holding the allTabletsLock.
func (shardSwap *shardSchemaSwap) getTabletList() []discovery.TabletStats {
	shardSwap.allTabletsLock.RLock()
	defer shardSwap.allTabletsLock.RUnlock()

	tabletList := make([]discovery.TabletStats, 0, len(shardSwap.allTablets))
	for _, tabletStats := range shardSwap.allTablets {
		tabletList = append(tabletList, *tabletStats)
	}
	return tabletList
}

// orderTabletsForSwap is an alias for the slice of TabletStats. It implements
// sort.Interface interface so that it's possible to sort the array in the order
// in which schema swap will propagate.
type orderTabletsForSwap []discovery.TabletStats

// Len is part of sort.Interface interface.
func (array orderTabletsForSwap) Len() int {
	return len(array)
}

// Swap is part of sort.Interface interface.
func (array orderTabletsForSwap) Swap(i, j int) {
	array[i], array[j] = array[j], array[i]
}

// tabletSortIndex returns a number representing the order in which schema swap will
// be propagated to the tablet. The last should be the master, then tablets doing
// backup/restore to not interrupt that process, then unhealthy tablets (so that we
// don't wait for them and the new schema is propagated to healthy tablets faster),
// then will go 'replica' tablets, and the first will be 'rdonly' and all other
// non-replica and non-master types. The sorting order within each of those 5 buckets
// doesn't matter.
func tabletSortIndex(tabletStats *discovery.TabletStats) int {
	switch {
	case tabletStats.Tablet.Type == topodatapb.TabletType_MASTER:
		return 5
	case tabletStats.Tablet.Type == topodatapb.TabletType_BACKUP || tabletStats.Tablet.Type == topodatapb.TabletType_RESTORE:
		return 4
	case tabletStats.Stats.HealthError != "":
		return 3
	case tabletStats.Tablet.Type == topodatapb.TabletType_REPLICA:
		return 2
	default:
		return 1
	}
}

// Less is part of sort.Interface interface. It should compare too elements of the array.
func (array orderTabletsForSwap) Less(i, j int) bool {
	return tabletSortIndex(&array[i]) < tabletSortIndex(&array[j])
}

// isSwapApplied verifies whether the schema swap was already applied to the tablet. It's
// considered to be applied if _vt.local_metadata table has the swap id in the row titled
// 'LastAppliedSchemaSwap'.
func (shardSwap *shardSchemaSwap) isSwapApplied(tablet *topodatapb.Tablet) (bool, error) {
	sqlCtx, cancelSQLCtx := context.WithTimeout(shardSwap.parent.ctx, *adminQueryTimeout)
	defer cancelSQLCtx()

	swapIDProto, err := shardSwap.parent.tabletClient.ExecuteFetchAsDba(
		sqlCtx,
		tablet,
		true, /* usePool */
		[]byte("SELECT value FROM _vt.local_metadata WHERE name = 'LastAppliedSchemaSwap'"),
		1,     /* maxRows */
		false, /* disableBinlogs */
		false /* reloadSchema */)
	if err != nil {
		return false, err
	}
	swapIDResult := sqltypes.Proto3ToResult(swapIDProto)
	if len(swapIDResult.Rows) == 0 {
		// No such row means we need to apply the swap.
		return false, nil
	}
	swapID, err := swapIDResult.Rows[0][0].ParseUint64()
	if err != nil {
		return false, err
	}
	return swapID == shardSwap.parent.swapID, nil
}

// findNextTablet searches for the next tablet where we need to apply the schema swap. The
// tablets are processed in the order described on tabletSortIndex() function above. If
// it hits any error while searching for the tablet it returns it to the caller. If the
// only tablet that's left without schema swap applied is the master then the function
// returns nil as the tablet and nil as the error.
func (shardSwap *shardSchemaSwap) findNextTablet() (*topodatapb.Tablet, error) {
	tabletList := shardSwap.getTabletList()
	sort.Sort(orderTabletsForSwap(tabletList))
	for _, tabletStats := range tabletList {
		if tabletStats.Tablet.Type == topodatapb.TabletType_MASTER {
			// Only master is left.
			return nil, nil
		}
		swapApplied, err := shardSwap.isSwapApplied(tabletStats.Tablet)
		if err != nil {
			return nil, err
		}
		if !swapApplied {
			return tabletStats.Tablet, nil
		}
	}
	return nil, fmt.Errorf("Something is wrong! Cannot find master on shard %v", shardSwap.shard)
}

// swapOnTablet performs the schema swap on the provided tablet and then waits for it
// to become healthy and to catch up with replication.
func (shardSwap *shardSchemaSwap) swapOnTablet(tablet *topodatapb.Tablet) error {
	log.Infof("Restoring tablet %v from backup", tablet.Alias)
	eventStream, err := shardSwap.parent.tabletClient.RestoreFromBackup(shardSwap.parent.ctx, tablet)
	if err != nil {
		return err
	}

waitForRestore:
	for {
		event, err := eventStream.Recv()
		switch err {
		case nil:
			log.Infof("Restore process on tablet %v: %v", tablet.Alias, logutil.EventString(event))
		case io.EOF:
			break waitForRestore
		default:
			return err
		}
	}
	// Check if the tablet has restored from a backup with schema change applied.
	swapApplied, err := shardSwap.isSwapApplied(tablet)
	if err != nil {
		return err
	}
	if !swapApplied {
		return fmt.Errorf("Restore from backup cannot pick up new schema. Backup from tablet with new schema needs to be taken.")
	}

	waitChannel, err := shardSwap.startWaitingOnUnhealthyTablet(tablet)
	if err != nil {
		return err
	}
	if waitChannel != nil {
		log.Infof("Waiting for tablet %v to catch up with replication", tablet.Alias)
		select {
		case <-shardSwap.parent.ctx.Done():
			return shardSwap.parent.ctx.Err()
		case <-*waitChannel:
			// Tablet has caught up, we can return successfully.
		}
	}
	return nil
}

// propagateToNonMasterTablets propagates the schema change to all non-master tablets
// in the shard. When it returns nil it means that only master can remain left with the
// old schema, everything else is verified to have schema swap applied.
func (shardSwap *shardSchemaSwap) propagateToNonMasterTablets() error {
	for {
		tablet, err := shardSwap.findNextTablet()
		if err == nil {
			if tablet == nil {
				return nil
			}
			err = shardSwap.swapOnTablet(tablet)
			if err == nil {
				log.Infof("Schema is successfully swapped on tablet %v.", tablet.Alias)
				continue
			}
			log.Infof("Error swapping schema on tablet %v: %v", tablet.Alias, err)
		} else {
			log.Infof("Error searching for next tablet to swap schema on: %v.", err)
		}

		log.Infof("Sleeping for %v seconds.", *delayBetweenErrors)
		select {
		case <-shardSwap.parent.ctx.Done():
			return shardSwap.parent.ctx.Err()
		case <-time.After(*delayBetweenErrors):
			// Waiting is done going to the next loop.
		}
	}
}
