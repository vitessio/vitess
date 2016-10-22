package schemaswap

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/logutil"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	delayBetweenErrors = flag.Duration("schema_swap_delay_between_errors", time.Minute,
		"time to wait after a retryable error happened in the schema swap process")
	adminQueryTimeout = flag.Duration("schema_swap_admin_query_timeout", 30*time.Second,
		"timeout for SQL queries used to save and retrieve meta information for schema swap process")
	backupConcurrency = flag.Int("schema_swap_backup_concurrency", 4,
		"number of simultaneous compression/checksum jobs to run for seed backup during schema swap")
	reparentTimeout = flag.Duration("schema_swap_reparent_timeout", 30*time.Second,
		"timeout to wait for slaves when doing reparent during schema swap")
)

const (
	lastStartedMetadataName  = "LastStartedSchemaSwap"
	lastFinishedMetadataName = "LastFinishedSchemaSwap"
	currentSQLMetadataName   = "CurrentSchemaSwapSQL"
	lastAppliedMetadataName  = "LastAppliedSchemaSwap"
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
	// allShards is a list of schema swap objects for each shard in the keyspace.
	allShards []*shardSchemaSwap
}

// shardSchemaSwap contains data related to schema swap happening on a specific shard.
type shardSchemaSwap struct {
	// parent is the structure with meta-information about the whole schema swap process.
	parent *Swap
	// shardName is the name of the shard this struct operates on.
	shardName string

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

// shardSwapMetadata contains full metadata about schema swaps on a certain shard.
type shardSwapMetadata struct {
	err error
	// IDs of last started and last finished schema swap on the shard
	lastStartedSwap, lastFinishedSwap uint64
	// currentSQL is the SQL of the currentlr running schema swap (if there is any)
	currentSQL string
}

// Run is the main entry point of the schema swap process. It drives the process from start
// to finish, including possible restart of already started process. In the latter case the
// method should be just called again and it will pick up already started process. The only
// input argument is the SQL statements that comprise the schema change that needs to be
// pushed using the schema swap process.
func (schemaSwap *Swap) Run(sql string) error {
	if err := schemaSwap.createShardObjects(); err != nil {
		return err
	}
	if err := schemaSwap.initializeSwap(sql); err != nil {
		return err
	}
	errHealthWatchers := schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.startHealthWatchers()
		})
	// Note: this defer statement is before the error is checked because some shards may
	// succeed while others fail. We should try to stop health watching on all shards no
	// matter if there was some failure or not.
	defer schemaSwap.stopAllHealthWatchers()
	if errHealthWatchers != nil {
		return errHealthWatchers
	}
	err := schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.applySeedSchemaChange(sql)
		})
	if err != nil {
		return err
	}
	err = schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.takeSeedBackup()
		})
	if err != nil {
		return err
	}
	err = schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.propagateToNonMasterTablets()
		})
	if err != nil {
		return err
	}
	err = schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.propagateToMaster()
		})
	if err != nil {
		return err
	}
	return schemaSwap.finalizeSwap()
}

// runOnAllShards is a helper method that executes the passed function for all shards in parallel.
// The method returns no error if the function succeeds on all shards. If on any of the shards
// the function fails then the method returns error. If several shards return error then only one
// of them is returned.
func (schemaSwap *Swap) runOnAllShards(shardFunc func(shard *shardSchemaSwap) error) error {
	var errorRecorder concurrency.AllErrorRecorder
	var waitGroup sync.WaitGroup
	for _, shardSwap := range schemaSwap.allShards {
		waitGroup.Add(1)
		go func(shard *shardSchemaSwap) {
			defer waitGroup.Done()
			errorRecorder.RecordError(shardFunc(shard))
		}(shardSwap)
	}
	waitGroup.Wait()
	return errorRecorder.Error()
}

// createShardObjects creates per-shard swap objects for all shards in the keyspace.
func (schemaSwap *Swap) createShardObjects() error {
	shardsList, err := schemaSwap.topoServer.FindAllShardsInKeyspace(schemaSwap.ctx, schemaSwap.keyspace)
	if err != nil {
		return err
	}
	for _, shardInfo := range shardsList {
		shardSwap := &shardSchemaSwap{
			parent:    schemaSwap,
			shardName: shardInfo.ShardName(),
		}
		schemaSwap.allShards = append(schemaSwap.allShards, shardSwap)
	}
	return nil
}

// stopAllHealthWatchers stops watching for health on each shard. It's separated into a separate
// function mainly to make "defer" statement where it's used simpler.
func (schemaSwap *Swap) stopAllHealthWatchers() {
	schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			shard.stopHealthWatchers()
			return nil
		})
}

// initializeSwap starts the schema swap process. If there is already a schema swap process started
// the the method just picks up that. Otherwise it starts a new one and writes into the database that
// the process was started.
func (schemaSwap *Swap) initializeSwap(sql string) error {
	var waitGroup sync.WaitGroup
	metadataList := make([]shardSwapMetadata, len(schemaSwap.allShards))
	for i, shard := range schemaSwap.allShards {
		waitGroup.Add(1)
		go shard.readShardMetadata(&metadataList[i], &waitGroup)
	}
	waitGroup.Wait()

	var recorder concurrency.AllErrorRecorder
	for i, metadata := range metadataList {
		if metadata.err != nil {
			recorder.RecordError(metadata.err)
		} else if metadata.lastStartedSwap == metadata.lastFinishedSwap {
			// The shard doesn't have schema swap started yet.
			nextSwapID := metadata.lastFinishedSwap + 1
			if schemaSwap.swapID == 0 {
				schemaSwap.swapID = nextSwapID
			} else if schemaSwap.swapID != nextSwapID {
				recorder.RecordError(fmt.Errorf(
					"Next schema swap id on shard %v should be %v, while for other shard(s) it should be %v",
					schemaSwap.allShards[i].shardName, nextSwapID, schemaSwap.swapID))
			}
		} else if metadata.lastStartedSwap < metadata.lastFinishedSwap {
			recorder.RecordError(fmt.Errorf(
				"Bad swap metadata on shard %v: LastFinishedSchemaSwap=%v is greater than LastStartedSchemaSwap=%v",
				schemaSwap.allShards[i].shardName, metadata.lastFinishedSwap, metadata.lastStartedSwap))
		} else if schemaSwap.swapID != 0 && schemaSwap.swapID != metadata.lastStartedSwap {
			recorder.RecordError(fmt.Errorf(
				"Shard %v has an already started schema swap with an id %v, while for other shard(s) id should be equal to %v",
				schemaSwap.allShards[i].shardName, metadata.lastStartedSwap, schemaSwap.swapID))
		} else if metadata.currentSQL != sql {
			recorder.RecordError(fmt.Errorf(
				"Shard %v has an already started schema swap with a different set of SQL statements",
				schemaSwap.allShards[i].shardName))
		} else {
			schemaSwap.swapID = metadata.lastStartedSwap
		}
	}
	if recorder.HasErrors() {
		return recorder.Error()
	}

	return schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.writeStartedSwap(sql)
		})
}

// finalizeSwap finishes the completed swap process by verifying that it's actually complete
// and then modifying the database to register the completion.
func (schemaSwap *Swap) finalizeSwap() error {
	err := schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.verifySwapApplied()
		})
	if err != nil {
		return err
	}
	err = schemaSwap.runOnAllShards(
		func(shard *shardSchemaSwap) error {
			return shard.writeFinishedSwap()
		})
	return err
}

// getMasterTablet returns the tablet that is currently master on the shard.
func (shardSwap *shardSchemaSwap) getMasterTablet() (*topodatapb.Tablet, error) {
	topoServer := shardSwap.parent.topoServer
	shardInfo, err := topoServer.GetShard(shardSwap.parent.ctx, shardSwap.parent.keyspace, shardSwap.shardName)
	if err != nil {
		return nil, err
	}
	tabletInfo, err := topoServer.GetTablet(shardSwap.parent.ctx, shardInfo.MasterAlias)
	if err != nil {
		return nil, err
	}
	return tabletInfo.Tablet, nil
}

// readShardMetadata reads info about schema swaps on this shard from _vt.shard_metadata table.
func (shardSwap *shardSchemaSwap) readShardMetadata(metadata *shardSwapMetadata, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	tablet, err := shardSwap.getMasterTablet()
	if err != nil {
		metadata.err = err
		return
	}
	query := fmt.Sprintf(
		"SELECT name, value FROM _vt.shard_metadata WHERE name in ('%s', '%s', '%s')",
		lastStartedMetadataName, lastFinishedMetadataName, currentSQLMetadataName)
	queryResult, err := shardSwap.executeAdminQuery(tablet, query, 3 /* maxRows */)
	if err != nil {
		metadata.err = err
		return
	}
	for _, row := range queryResult.Rows {
		switch row[0].String() {
		case lastStartedMetadataName:
			swapID, err := row[1].ParseUint64()
			if err != nil {
				log.Warningf("Could not parse value of last started schema swap id ('%s'), ignoring the value: %v", row[1].String(), err)
			} else {
				metadata.lastStartedSwap = swapID
			}
		case lastFinishedMetadataName:
			swapID, err := row[1].ParseUint64()
			if err != nil {
				log.Warningf("Could not parse value of last finished schema swap id ('%s'), ignoring the value: %v", row[1].String(), err)
			} else {
				metadata.lastFinishedSwap = swapID
			}
		case currentSQLMetadataName:
			metadata.currentSQL = row[1].String()
		}
	}
}

// writeStartedSwap registers in the _vt.shard_metadata table in the database the information
// about the new schema swap process being started.
func (shardSwap *shardSchemaSwap) writeStartedSwap(sql string) error {
	tablet, err := shardSwap.getMasterTablet()
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		"INSERT INTO _vt.shard_metadata (name, value) VALUES ('%s', '%d') ON DUPLICATE KEY UPDATE value = '%d'",
		lastStartedMetadataName, shardSwap.parent.swapID, shardSwap.parent.swapID)
	_, err = shardSwap.executeAdminQuery(tablet, query, 0 /* maxRows */)
	if err != nil {
		return err
	}
	queryBuf := bytes.Buffer{}
	queryBuf.WriteString("INSERT INTO _vt.shard_metadata (name, value) VALUES ('")
	queryBuf.WriteString(currentSQLMetadataName)
	queryBuf.WriteString("',")
	sqlValue, err := sqltypes.BuildValue(sql)
	if err != nil {
		return err
	}
	sqlValue.EncodeSQL(&queryBuf)
	queryBuf.WriteString(") ON DUPLICATE KEY UPDATE value = ")
	sqlValue.EncodeSQL(&queryBuf)
	_, err = shardSwap.executeAdminQuery(tablet, queryBuf.String(), 0 /* maxRows */)
	return err
}

// writeFinishedSwap registers in the _vt.shard_metadata table in the database that the schema
// swap process has finished.
func (shardSwap *shardSchemaSwap) writeFinishedSwap() error {
	tablet, err := shardSwap.getMasterTablet()
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		"INSERT INTO _vt.shard_metadata (name, value) VALUES ('%s', '%d') ON DUPLICATE KEY UPDATE value = '%d'",
		lastFinishedMetadataName, shardSwap.parent.swapID, shardSwap.parent.swapID)
	_, err = shardSwap.executeAdminQuery(tablet, query, 0 /* maxRows */)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("DELETE FROM _vt.shard_metadata WHERE name = '%s'", currentSQLMetadataName)
	_, err = shardSwap.executeAdminQuery(tablet, query, 0 /* maxRows */)
	return err
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
			shardSwap.shardName,
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

// executeAdminQuery executes a query on a given tablet as 'allprivs' user. The query is executed
// using timeout value from --schema_swap_admin_query_timeout flag.
func (shardSwap *shardSchemaSwap) executeAdminQuery(tablet *topodatapb.Tablet, query string, maxRows int) (*sqltypes.Result, error) {
	sqlCtx, cancelSQLCtx := context.WithTimeout(shardSwap.parent.ctx, *adminQueryTimeout)
	defer cancelSQLCtx()

	sqlResultProto, err := shardSwap.parent.tabletClient.ExecuteFetchAsAllPrivs(
		sqlCtx,
		tablet,
		[]byte(query),
		maxRows,
		false /* reloadSchema */)
	if err != nil {
		return nil, err
	}
	return sqltypes.Proto3ToResult(sqlResultProto), nil
}

// isSwapApplied verifies whether the schema swap was already applied to the tablet. It's
// considered to be applied if _vt.local_metadata table has the swap id in the row titled
// 'LastAppliedSchemaSwap'.
func (shardSwap *shardSchemaSwap) isSwapApplied(tablet *topodatapb.Tablet) (bool, error) {
	swapIDResult, err := shardSwap.executeAdminQuery(
		tablet,
		fmt.Sprintf("SELECT value FROM _vt.local_metadata WHERE name = '%s'", lastAppliedMetadataName),
		1 /* maxRows */)
	if err != nil {
		return false, err
	}
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

// findNextTabletToSwap searches for the next tablet where we need to apply the schema swap. The
// tablets are processed in the order described on tabletSortIndex() function above. If
// it hits any error while searching for the tablet it returns it to the caller. If the
// only tablet that's left without schema swap applied is the master then the function
// returns nil as the tablet and nil as the error.
func (shardSwap *shardSchemaSwap) findNextTabletToSwap() (*topodatapb.Tablet, error) {
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
	return nil, fmt.Errorf("Something is wrong! Cannot find master on shard %v", shardSwap.shardName)
}

// applySeedSchemaChange chooses a healthy tablet as a schema swap seed and applies the schema change
// on it. In the choice of the seed tablet any RDONLY tablet is preferred, but if there are no healthy
// RDONLY tablets the REPLICA one is chosen. If there are any non-MASTER tablets indicating that the
// schema swap was already applied on them, then the method assumes that it's the seed tablet from the
// already started process and doesn't try to apply the schema change on any other tablet.
func (shardSwap *shardSchemaSwap) applySeedSchemaChange(sql string) error {
	tabletList := shardSwap.getTabletList()
	sort.Sort(orderTabletsForSwap(tabletList))
	for _, tabletStats := range tabletList {
		swapApplied, err := shardSwap.isSwapApplied(tabletStats.Tablet)
		if err != nil {
			return err
		}
		if swapApplied && tabletStats.Tablet.Type != topodatapb.TabletType_MASTER {
			return nil
		}
	}
	seedTablet := tabletList[0].Tablet
	if seedTablet.Type == topodatapb.TabletType_MASTER {
		return fmt.Errorf("The only candidate for a schema swap seed is the master %v. Aborting the swap.", seedTablet)
	}
	// Draining the tablet for it to not be used for execution of user queries.
	savedSeedType := seedTablet.Type
	err := shardSwap.parent.tabletClient.ChangeType(shardSwap.parent.ctx, seedTablet, topodatapb.TabletType_DRAINED)
	if err != nil {
		return err
	}
	_, err = shardSwap.parent.topoServer.UpdateTabletFields(shardSwap.parent.ctx, seedTablet.Alias,
		func(tablet *topodatapb.Tablet) error {
			tablet.Tags["drain_reason"] = "Drained as online schema swap seed"
			return nil
		})
	if err != nil {
		// This is not a critical error, we'll just log it.
		log.Errorf("Got error trying to set drain_reason on tablet %v: %v", seedTablet.Alias, err)
	}
	// TODO: Add support for multi-statement schema swaps.
	_, err = shardSwap.parent.tabletClient.ExecuteFetchAsDba(
		shardSwap.parent.ctx,
		seedTablet,
		true, /* usePool */
		[]byte(sql),
		0,    /* maxRows */
		true, /* disableBinlogs */
		true /* reloadSchema */)
	if err != nil {
		return err
	}
	updateAppliedSwapQuery := fmt.Sprintf(
		"INSERT INTO _vt.local_metadata (name, value) VALUES ('%s', '%d') ON DUPLICATE KEY UPDATE value = '%d'",
		lastAppliedMetadataName, shardSwap.parent.swapID, shardSwap.parent.swapID)
	_, err = shardSwap.parent.tabletClient.ExecuteFetchAsDba(
		shardSwap.parent.ctx,
		seedTablet,
		true, /* usePool */
		[]byte(updateAppliedSwapQuery),
		0,    /* maxRows */
		true, /* disableBinlogs */
		false /* reloadSchema */)
	if err != nil {
		return err
	}

	// Undraining the tablet, returing it to the same type as it was before.
	_, err = shardSwap.parent.topoServer.UpdateTabletFields(shardSwap.parent.ctx, seedTablet.Alias,
		func(tablet *topodatapb.Tablet) error {
			delete(tablet.Tags, "drain_reason")
			return nil
		})
	if err != nil {
		// This is not a critical error, we'll just log it.
		log.Errorf("Got error trying to set drain_reason on tablet %v: %v", seedTablet.Alias, err)
	}
	err = shardSwap.parent.tabletClient.ChangeType(shardSwap.parent.ctx, seedTablet, savedSeedType)
	if err != nil {
		return err
	}

	return nil
}

// takeSeedBackup takes backup on the seed tablet. The method assumes that the seed tablet is any tablet that
// has info that schema swap was already applied on it. This way the function can be re-used if the seed backup
// is lost somewhere half way through the schema swap process.
func (shardSwap *shardSchemaSwap) takeSeedBackup() error {
	tabletList := shardSwap.getTabletList()
	sort.Sort(orderTabletsForSwap(tabletList))
	var seedTablet *topodatapb.Tablet
	for _, tabletStats := range tabletList {
		swapApplied, err := shardSwap.isSwapApplied(tabletStats.Tablet)
		if err != nil {
			return err
		}
		if swapApplied && tabletStats.Tablet.Type != topodatapb.TabletType_MASTER {
			seedTablet = tabletStats.Tablet
			break
		}
	}
	if seedTablet == nil {
		return fmt.Errorf("Cannot find the seed tablet on shard %v", shardSwap.shardName)
	}

	eventStream, err := shardSwap.parent.tabletClient.Backup(shardSwap.parent.ctx, seedTablet, *backupConcurrency)
	if err != nil {
		return err
	}
waitForBackup:
	for {
		event, err := eventStream.Recv()
		switch err {
		case nil:
			log.Infof("Backup process on tablet %v: %v", seedTablet.Alias, logutil.EventString(event))
		case io.EOF:
			break waitForBackup
		default:
			return err
		}
	}

	return shardSwap.waitForTabletToBeHealthy(seedTablet)
}

// verifySwapApplied checks that all tablets in the shard have schema swap applied on them.
func (shardSwap *shardSchemaSwap) verifySwapApplied() error {
	tabletList := shardSwap.getTabletList()
	for _, tabletStats := range tabletList {
		swapApplied, err := shardSwap.isSwapApplied(tabletStats.Tablet)
		if err != nil {
			return err
		}
		if !swapApplied {
			return fmt.Errorf("Schema swap was not applied on tablet %v", tabletStats.Tablet.Alias)
		}
	}
	return nil
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

	return shardSwap.waitForTabletToBeHealthy(tablet)
}

// waitForTabletToBeHealthy waits until the given tablet becomes healthy and caught up with
// replication. If the tablet is already healthy and caught up when this method is called then
// it returns immediately.
func (shardSwap *shardSchemaSwap) waitForTabletToBeHealthy(tablet *topodatapb.Tablet) error {
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
		tablet, err := shardSwap.findNextTabletToSwap()
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

// propagateToMaster propagates the schema change to the master. If the master already has
// the schema change applied then the method does nothing.
func (shardSwap *shardSchemaSwap) propagateToMaster() error {
	masterTablet, err := shardSwap.getMasterTablet()
	if err != nil {
		return err
	}
	swapApplied, err := shardSwap.isSwapApplied(masterTablet)
	if swapApplied {
		return nil
	}
	wr := wrangler.New(logutil.NewConsoleLogger(), shardSwap.parent.topoServer, shardSwap.parent.tabletClient)
	err = wr.PlannedReparentShard(
		shardSwap.parent.ctx,
		shardSwap.parent.keyspace,
		shardSwap.shardName,
		nil,                /* masterElectTabletAlias */
		masterTablet.Alias, /* avoidMasterAlias */
		*reparentTimeout)
	if err != nil {
		return err
	}
	return shardSwap.swapOnTablet(masterTablet)
}
