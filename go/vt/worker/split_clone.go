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

package worker

import (
	"fmt"
	"html/template"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/worker/events"
	"vitess.io/vitess/go/vt/wrangler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// cloneType specifies whether it is a horizontal resharding or a vertical split.
// TODO(mberlin): Remove this once we merged both into one command.
type cloneType int

const (
	horizontalResharding cloneType = iota
	verticalSplit
)

// servingTypes is the list of tabletTypes which the source keyspace must be serving.
var servingTypes = []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}

// SplitCloneWorker will clone the data within a keyspace from a
// source set of shards to a destination set of shards.
type SplitCloneWorker struct {
	StatusWorker

	wr                    *wrangler.Wrangler
	cloneType             cloneType
	cell                  string
	destinationKeyspace   string
	shard                 string
	online                bool
	offline               bool
	useConsistentSnapshot bool
	// verticalSplit only: List of tables which should be split out.
	tables []string
	// horizontalResharding only: List of tables which will be skipped.
	excludeTables          []string
	chunkCount             int
	minRowsPerChunk        int
	sourceReaderCount      int
	writeQueryMaxRows      int
	writeQueryMaxSize      int
	destinationWriterCount int
	minHealthyTablets      int
	tabletType             topodatapb.TabletType
	maxTPS                 int64
	maxReplicationLag      int64
	cleaner                *wrangler.Cleaner
	tabletTracker          *TabletTracker

	// populated during WorkerStateInit, read-only after that
	destinationKeyspaceInfo *topo.KeyspaceInfo
	sourceShards            []*topo.ShardInfo
	destinationShards       []*topo.ShardInfo
	keyspaceSchema          *vindexes.KeyspaceSchema
	// healthCheck is used for the destination shards to a) find out the current
	// MASTER tablet, b) get the list of healthy RDONLY tablets and c) track the
	// replication lag of all REPLICA tablets.
	// It must be closed at the end of the command.
	healthCheck discovery.HealthCheck
	tsc         *discovery.TabletStatsCache

	// populated during WorkerStateFindTargets, read-only after that
	sourceTablets []*topodatapb.Tablet
	lastPos       string // contains the GTID position for the source
	transactions  []int64

	// shardWatchers contains a TopologyWatcher for each source and destination
	// shard. It updates the list of tablets in the healthcheck if replicas are
	// added/removed.
	// Each watcher must be stopped at the end of the command.
	shardWatchers []*discovery.TopologyWatcher
	// destinationDbNames stores for each destination keyspace/shard the MySQL
	// database name.
	// Example Map Entry: test_keyspace/-80 => vt_test_keyspace
	destinationDbNames map[string]string

	// throttlersMu guards the fields within this group.
	throttlersMu sync.Mutex
	// throttlers has a throttler for each destination shard.
	// Map key format: "keyspace/shard" e.g. "test_keyspace/-80"
	// Throttlers will be added/removed during WorkerStateClone(Online|Offline).
	throttlers map[string]*throttler.Throttler

	// sourceAliases has the list of tablets (per source shard) we took
	// offline for the WorkerStateCloneOffline phase.
	// Populated shortly before WorkerStateCloneOffline, read-only after that.
	sourceAliases []*topodatapb.TabletAlias

	// formattedOfflineSourcesMu guards all fields in this group.
	formattedOfflineSourcesMu sync.Mutex
	// formattedOfflineSources is a space separated list of
	// "sourceAliases". It is used by the StatusAs* methods to output the
	// used source tablets during the offline clone phase.
	formattedOfflineSources string

	// tableStatusList* holds the status for each table.
	// populated during WorkerStateCloneOnline
	tableStatusListOnline *tableStatusList
	// populated during WorkerStateCloneOffline
	tableStatusListOffline *tableStatusList

	ev event.Updater
}

// newSplitCloneWorker returns a new worker object for the SplitClone command.
func newSplitCloneWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, online, offline bool, excludeTables []string, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, destinationWriterCount, minHealthyTablets int, tabletType topodatapb.TabletType, maxTPS, maxReplicationLag int64, useConsistentSnapshot bool) (Worker, error) {
	return newCloneWorker(wr, horizontalResharding, cell, keyspace, shard, online, offline, nil /* tables */, excludeTables, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, destinationWriterCount, minHealthyTablets, tabletType, maxTPS, maxReplicationLag, useConsistentSnapshot)
}

// newVerticalSplitCloneWorker returns a new worker object for the
// VerticalSplitClone command.
func newVerticalSplitCloneWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, online, offline bool, tables []string, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, destinationWriterCount, minHealthyTablets int, tabletType topodatapb.TabletType, maxTPS, maxReplicationLag int64, useConsistentSnapshot bool) (Worker, error) {
	return newCloneWorker(wr, verticalSplit, cell, keyspace, shard, online, offline, tables, nil /* excludeTables */, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, destinationWriterCount, minHealthyTablets, tabletType, maxTPS, maxReplicationLag, useConsistentSnapshot)
}

// newCloneWorker returns a new SplitCloneWorker object which is used both by
// the SplitClone and VerticalSplitClone command.
// TODO(mberlin): Rename SplitCloneWorker to cloneWorker.
func newCloneWorker(wr *wrangler.Wrangler, cloneType cloneType, cell, keyspace, shard string, online, offline bool, tables, excludeTables []string, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, destinationWriterCount, minHealthyTablets int, tabletType topodatapb.TabletType, maxTPS, maxReplicationLag int64, useConsistentSnapshot bool) (Worker, error) {
	if cloneType != horizontalResharding && cloneType != verticalSplit {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "unknown cloneType: %v This is a bug. Please report", cloneType)
	}

	// Verify user defined flags.
	if !online && !offline {
		return nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "at least one clone phase (-online, -offline) must be enabled (and not set to false)")
	}
	if tables != nil && len(tables) == 0 {
		return nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "list of tablets to be split out must not be empty")
	}
	if chunkCount <= 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "chunk_count must be > 0: %v", chunkCount)
	}
	if minRowsPerChunk <= 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "min_rows_per_chunk must be > 0: %v", minRowsPerChunk)
	}
	if sourceReaderCount <= 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "source_reader_count must be > 0: %v", sourceReaderCount)
	}
	if writeQueryMaxRows <= 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "write_query_max_rows must be > 0: %v", writeQueryMaxRows)
	}
	if writeQueryMaxSize <= 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "write_query_max_size must be > 0: %v", writeQueryMaxSize)
	}
	if destinationWriterCount <= 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "destination_writer_count must be > 0: %v", destinationWriterCount)
	}
	if minHealthyTablets < 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "min_healthy_tablets must be >= 0: %v", minHealthyTablets)
	}
	if maxTPS != throttler.MaxRateModuleDisabled {
		wr.Logger().Infof("throttling enabled and set to a max of %v transactions/second", maxTPS)
	}
	if maxTPS != throttler.MaxRateModuleDisabled && maxTPS < int64(destinationWriterCount) {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "-max_tps must be >= -destination_writer_count: %v >= %v", maxTPS, destinationWriterCount)
	}
	if maxReplicationLag <= 0 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "max_replication_lag must be >= 1s: %v", maxReplicationLag)
	}
	if tabletType != topodatapb.TabletType_REPLICA && tabletType != topodatapb.TabletType_RDONLY {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "tablet_type must be RDONLY or REPLICA: %v", topodatapb.TabletType_name[int32(tabletType)])
	}

	scw := &SplitCloneWorker{
		StatusWorker:           NewStatusWorker(),
		wr:                     wr,
		cloneType:              cloneType,
		cell:                   cell,
		destinationKeyspace:    keyspace,
		shard:                  shard,
		online:                 online,
		offline:                offline,
		tables:                 tables,
		excludeTables:          excludeTables,
		chunkCount:             chunkCount,
		minRowsPerChunk:        minRowsPerChunk,
		sourceReaderCount:      sourceReaderCount,
		writeQueryMaxRows:      writeQueryMaxRows,
		writeQueryMaxSize:      writeQueryMaxSize,
		destinationWriterCount: destinationWriterCount,
		minHealthyTablets:      minHealthyTablets,
		maxTPS:                 maxTPS,
		maxReplicationLag:      maxReplicationLag,
		cleaner:                &wrangler.Cleaner{},
		tabletTracker:          NewTabletTracker(),
		tabletType:             tabletType,
		tableStatusListOnline:  &tableStatusList{},
		tableStatusListOffline: &tableStatusList{},
		useConsistentSnapshot:  useConsistentSnapshot,

		throttlers:         make(map[string]*throttler.Throttler),
		destinationDbNames: make(map[string]string),
	}
	scw.initializeEventDescriptor()
	return scw, nil
}

func (scw *SplitCloneWorker) initializeEventDescriptor() {
	switch scw.cloneType {
	case horizontalResharding:
		scw.ev = &events.SplitClone{
			Cell:          scw.cell,
			Keyspace:      scw.destinationKeyspace,
			Shard:         scw.shard,
			ExcludeTables: scw.excludeTables,
		}
	case verticalSplit:
		scw.ev = &events.VerticalSplitClone{
			Cell:     scw.cell,
			Keyspace: scw.destinationKeyspace,
			Shard:    scw.shard,
			Tables:   scw.tables,
		}
	}
}

func (scw *SplitCloneWorker) setState(state StatusWorkerState) {
	scw.SetState(state)
	event.DispatchUpdate(scw.ev, state.String())
}

func (scw *SplitCloneWorker) setErrorState(err error) {
	scw.SetState(WorkerStateError)
	event.DispatchUpdate(scw.ev, "error: "+err.Error())
}

func (scw *SplitCloneWorker) formatOnlineSources() string {
	aliases := scw.tabletTracker.TabletsInUse()
	if aliases == "" {
		return "no online source tablets currently in use"
	}
	return aliases
}

func (scw *SplitCloneWorker) setFormattedOfflineSources(aliases []*topodatapb.TabletAlias) {
	scw.formattedOfflineSourcesMu.Lock()
	defer scw.formattedOfflineSourcesMu.Unlock()

	var sources []string
	for _, alias := range aliases {
		sources = append(sources, topoproto.TabletAliasString(alias))
	}
	scw.formattedOfflineSources = strings.Join(sources, " ")
}

// FormattedOfflineSources returns a space separated list of tablets which
// are in use during the offline clone phase.
func (scw *SplitCloneWorker) FormattedOfflineSources() string {
	scw.formattedOfflineSourcesMu.Lock()
	defer scw.formattedOfflineSourcesMu.Unlock()

	if scw.formattedOfflineSources == "" {
		return "no offline source tablets currently in use"
	}
	return scw.formattedOfflineSources
}

// StatusAsHTML implements the Worker interface
func (scw *SplitCloneWorker) StatusAsHTML() template.HTML {
	state := scw.State()

	result := "<b>Working on:</b> " + scw.destinationKeyspace + "/" + scw.shard + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateCloneOnline:
		result += "<b>Running:</b></br>\n"
		result += "<b>Copying from:</b> " + scw.formatOnlineSources() + "</br>\n"
		statuses, eta := scw.tableStatusListOnline.format()
		result += "<b>ETA:</b> " + eta.String() + "</br>\n"
		result += strings.Join(statuses, "</br>\n")
	case WorkerStateCloneOffline:
		result += "<b>Running:</b></br>\n"
		result += "<b>Copying from:</b> " + scw.FormattedOfflineSources() + "</br>\n"
		statuses, eta := scw.tableStatusListOffline.format()
		result += "<b>ETA:</b> " + eta.String() + "</br>\n"
		result += strings.Join(statuses, "</br>\n")
		if scw.online {
			result += "</br>\n"
			result += "<b>Result from preceding Online Clone:</b></br>\n"
			statuses, _ := scw.tableStatusListOnline.format()
			result += strings.Join(statuses, "</br>\n")
		}
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
		if scw.online {
			result += "</br>\n"
			result += "<b>Online Clone Result:</b></br>\n"
			statuses, _ := scw.tableStatusListOnline.format()
			result += strings.Join(statuses, "</br>\n")
		}
		if scw.offline {
			result += "</br>\n"
			result += "<b>Offline Clone Result:</b></br>\n"
			statuses, _ := scw.tableStatusListOffline.format()
			result += strings.Join(statuses, "</br>\n")
		}
	}

	if (state == WorkerStateCloneOnline || state == WorkerStateCloneOffline) && (scw.maxTPS != throttler.MaxRateModuleDisabled || scw.maxReplicationLag != throttler.ReplicationLagModuleDisabled) {
		result += "</br>\n"
		result += `<b>Resharding Throttler:</b> <a href="/throttlerz">see /throttlerz for details</a></br>`
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface
func (scw *SplitCloneWorker) StatusAsText() string {
	state := scw.State()

	result := "Working on: " + scw.destinationKeyspace + "/" + scw.shard + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateCloneOnline:
		result += "Running:\n"
		result += "Comparing source and destination using: " + scw.formatOnlineSources() + "\n"
		statuses, eta := scw.tableStatusListOnline.format()
		result += "ETA: " + eta.String() + "\n"
		result += strings.Join(statuses, "\n")
	case WorkerStateCloneOffline:
		result += "Running:\n"
		result += "Copying from: " + scw.FormattedOfflineSources() + "\n"
		statuses, eta := scw.tableStatusListOffline.format()
		result += "ETA: " + eta.String() + "\n"
		result += strings.Join(statuses, "\n")
		if scw.online {
			result += "\n"
			result += "\n"
			result += "Result from preceding Online Clone:\n"
			statuses, _ := scw.tableStatusListOnline.format()
			result += strings.Join(statuses, "\n")
		}
	case WorkerStateDone:
		result += "Success:"
		if scw.online {
			result += "\n"
			result += "\n"
			result += "Online Clone Result:\n"
			statuses, _ := scw.tableStatusListOnline.format()
			result += strings.Join(statuses, "\n")
		}
		if scw.offline {
			result += "\n"
			result += "\n"
			result += "Offline Clone Result:\n"
			statuses, _ := scw.tableStatusListOffline.format()
			result += strings.Join(statuses, "\n")
		}
	}
	return result
}

// Run implements the Worker interface
func (scw *SplitCloneWorker) Run(ctx context.Context) error {
	resetVars()

	// Run the command.
	err := scw.run(ctx)

	// Cleanup.
	scw.setState(WorkerStateCleanUp)
	// Reverse any changes e.g. setting the tablet type of a source RDONLY tablet.
	cerr := scw.cleaner.CleanUp(scw.wr)
	if cerr != nil {
		if err != nil {
			scw.wr.Logger().Errorf2(cerr, "CleanUp failed in addition to job error: %v")
		} else {
			err = cerr
		}
	}

	// Stop watchers to prevent new tablets from getting added to the healthCheck.
	for _, watcher := range scw.shardWatchers {
		watcher.Stop()
	}
	// Stop healthCheck to make sure it stops calling our listener implementation.
	if scw.healthCheck != nil {
		// After Close returned, we can be sure that it won't call our listener
		// implementation (method StatsUpdate) anymore.
		if err := scw.healthCheck.Close(); err != nil {
			scw.wr.Logger().Errorf2(err, "HealthCheck.Close() failed")
		}
	}

	if err != nil {
		scw.setErrorState(err)
		return err
	}
	scw.setState(WorkerStateDone)
	return nil
}

func (scw *SplitCloneWorker) run(ctx context.Context) error {
	// Phase 1: read what we need to do.
	if err := scw.init(ctx); err != nil {
		return vterrors.Wrap(err, "init() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// Phase 2: Find destination master tablets.
	if err := scw.findDestinationMasters(ctx); err != nil {
		return vterrors.Wrap(err, "findDestinationMasters() failed")
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// Phase 3: (optional) online clone.
	if scw.online {
		scw.wr.Logger().Infof("Online clone will be run now.")
		// 3a: Wait for minimum number of source tablets (required for the diff).
		if err := scw.waitForTablets(ctx, scw.sourceShards, *waitForHealthyTabletsTimeout); err != nil {
			return vterrors.Wrap(err, "waitForTablets(sourceShards) failed")
		}
		// 3b: Clone the data.
		start := time.Now()
		if err := scw.clone(ctx, WorkerStateCloneOnline); err != nil {
			return vterrors.Wrap(err, "online clone() failed")
		}
		d := time.Since(start)
		if err := checkDone(ctx); err != nil {
			return err
		}
		// TODO(mberlin): Output diff report of the online clone.
		// Round duration to second granularity to make it more readable.
		scw.wr.Logger().Infof("Online clone finished after %v.", time.Duration(d.Nanoseconds()/time.Second.Nanoseconds()*time.Second.Nanoseconds()))
	} else {
		scw.wr.Logger().Infof("Online clone skipped because --online=false was specified.")
	}

	// Phase 4: offline clone.
	if scw.offline {
		scw.wr.Logger().Infof("Offline clone will be run now.")
		if scw.online {
			// Wait until the inserts from the online clone were propagated
			// from the destination master to the rdonly tablets.
			// TODO(mberlin): Remove the sleep and get the destination master position
			// instead and wait until all selected destination tablets have reached
			// it.
			time.Sleep(1 * time.Second)
		}

		// 4a: Make sure the sources are producing a stable view of the data
		if scw.useConsistentSnapshot {
			if err := scw.findTransactionalSources(ctx); err != nil {
				return vterrors.Wrap(err, "findSourceTablets() failed")
			}
		} else {
			if err := scw.findOfflineSourceTablets(ctx); err != nil {
				return vterrors.Wrap(err, "findSourceTablets() failed")
			}
		}
		if err := checkDone(ctx); err != nil {
			return err
		}

		// 4b: Clone the data.
		start := time.Now()
		if err := scw.clone(ctx, WorkerStateCloneOffline); err != nil {
			return vterrors.Wrap(err, "offline clone() failed")
		}
		if err := scw.setUpVReplication(ctx); err != nil {
			return vterrors.Wrap(err, "failed to set up replication")
		}

		d := time.Since(start)
		if err := checkDone(ctx); err != nil {
			return err
		}
		// TODO(mberlin): Output diff report of the offline clone.
		// Round duration to second granularity to make it more readable.
		scw.wr.Logger().Infof("Offline clone finished after %v.", time.Duration(d.Nanoseconds()/time.Second.Nanoseconds()*time.Second.Nanoseconds()))
	} else {
		scw.wr.Logger().Infof("Offline clone skipped because --offline=false was specified.")
	}

	return nil
}

// init phase:
// - read the destination keyspace, make sure it has 'servedFrom' values
func (scw *SplitCloneWorker) init(ctx context.Context) error {
	scw.setState(WorkerStateInit)

	// read the keyspace and validate it
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	var err error
	scw.destinationKeyspaceInfo, err = scw.wr.TopoServer().GetKeyspace(shortCtx, scw.destinationKeyspace)
	cancel()
	if err != nil {
		return vterrors.Wrapf(err, "cannot read (destination) keyspace %v", scw.destinationKeyspace)
	}

	// Set source and destination shard infos.
	switch scw.cloneType {
	case horizontalResharding:
		if err := scw.initShardsForHorizontalResharding(ctx); err != nil {
			return vterrors.Wrap(err, "failed initShardsForHorizontalResharding")
		}
	case verticalSplit:
		if err := scw.initShardsForVerticalSplit(ctx); err != nil {
			return vterrors.Wrap(err, "failed initShardsForVerticalSplit")
		}
	}

	if err := scw.sanityCheckShardInfos(); err != nil {
		return vterrors.Wrap(err, "failed sanityCheckShardInfos")
	}

	if scw.cloneType == horizontalResharding {
		if err := scw.loadVSchema(ctx); err != nil {
			return vterrors.Wrap(err, "failed loadVSchema")
		}
	}

	// Initialize healthcheck and add destination shards to it.
	scw.healthCheck = discovery.NewHealthCheck(*healthcheckRetryDelay, *healthCheckTimeout)
	scw.tsc = discovery.NewTabletStatsCacheDoNotSetListener(scw.wr.TopoServer(), scw.cell)
	// We set sendDownEvents=true because it's required by TabletStatsCache.
	scw.healthCheck.SetListener(scw, true /* sendDownEvents */)

	// Start watchers to get tablets added automatically to healthCheck.
	allShards := append(scw.sourceShards, scw.destinationShards...)
	for _, si := range allShards {
		watcher := discovery.NewShardReplicationWatcher(ctx, scw.wr.TopoServer(), scw.healthCheck,
			scw.cell, si.Keyspace(), si.ShardName(),
			*healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
		scw.shardWatchers = append(scw.shardWatchers, watcher)
	}

	return nil
}

func (scw *SplitCloneWorker) initShardsForHorizontalResharding(ctx context.Context) error {
	// find the OverlappingShards in the keyspace
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	osList, err := topotools.FindOverlappingShards(shortCtx, scw.wr.TopoServer(), scw.destinationKeyspace)
	cancel()
	if err != nil {
		return vterrors.Wrapf(err, "cannot FindOverlappingShards in %v", scw.destinationKeyspace)
	}

	// find the shard we mentioned in there, if any
	os := topotools.OverlappingShardsForShard(osList, scw.shard)
	if os == nil {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "the specified shard %v/%v is not in any overlapping shard", scw.destinationKeyspace, scw.shard)
	}
	scw.wr.Logger().Infof("Found overlapping shards: %+v\n", os)

	// one side should have served types, the other one none,
	// figure out wich is which, then double check them all
	if len(os.Left[0].ServedTypes) > 0 {
		scw.sourceShards = os.Left
		scw.destinationShards = os.Right
	} else {
		scw.sourceShards = os.Right
		scw.destinationShards = os.Left
	}

	if scw.useConsistentSnapshot && len(scw.sourceShards) > 1 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cannot use consistent snapshot against multiple source shards")
	}

	return nil
}

func (scw *SplitCloneWorker) initShardsForVerticalSplit(ctx context.Context) error {
	if len(scw.destinationKeyspaceInfo.ServedFroms) == 0 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "destination keyspace %v has no KeyspaceServedFrom", scw.destinationKeyspace)
	}

	// Determine the source keyspace.
	servedFrom := ""
	for _, st := range servingTypes {
		sf := scw.destinationKeyspaceInfo.GetServedFrom(st)
		if sf == nil {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "destination keyspace %v is serving type %v", scw.destinationKeyspace, st)
		}
		if servedFrom == "" {
			servedFrom = sf.Keyspace
		} else {
			if servedFrom != sf.Keyspace {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "destination keyspace %v is serving from multiple source keyspaces %v and %v", scw.destinationKeyspace, servedFrom, sf.Keyspace)
			}
		}
	}
	sourceKeyspace := servedFrom

	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	shardMap, err := scw.wr.TopoServer().FindAllShardsInKeyspace(shortCtx, sourceKeyspace)
	cancel()
	if err != nil {
		return vterrors.Wrapf(err, "cannot find source shard for source keyspace %s", sourceKeyspace)
	}
	if len(shardMap) != 1 {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "found the wrong number of source shards, there should be only one, %v", shardMap)
	}
	var sourceShard string
	for s := range shardMap {
		sourceShard = s
	}

	// Init the source and destination shard info.
	sourceShardInfo, err := scw.wr.TopoServer().GetShard(ctx, sourceKeyspace, sourceShard)
	if err != nil {
		return err
	}
	scw.sourceShards = []*topo.ShardInfo{sourceShardInfo}
	destShardInfo, err := scw.wr.TopoServer().GetShard(ctx, scw.destinationKeyspace, scw.shard)
	if err != nil {
		return err
	}
	scw.destinationShards = []*topo.ShardInfo{destShardInfo}

	return nil
}

func (scw *SplitCloneWorker) sanityCheckShardInfos() error {
	// Verify that filtered replication is not already enabled.
	for _, si := range scw.destinationShards {
		if len(si.SourceShards) > 0 {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION,
				"destination shard %v/%v has filtered replication already enabled from a previous resharding (ShardInfo is set)."+
					" This requires manual intervention e.g. use vtctl SourceShardDelete to remove it",
				si.Keyspace(), si.ShardName())
		}
	}
	// Verify that the source is serving all serving types.
	for _, st := range servingTypes {
		for _, si := range scw.sourceShards {
			if si.GetServedType(st) == nil {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "source shard %v/%v is not serving type %v", si.Keyspace(), si.ShardName(), st)
			}
		}
	}

	switch scw.cloneType {
	case horizontalResharding:
		// Verify that the destination is not serving yet.
		for _, si := range scw.destinationShards {
			if len(si.ServedTypes) > 0 {
				return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "destination shard %v/%v is serving some types", si.Keyspace(), si.ShardName())
			}
		}
	case verticalSplit:
		// Verify that the destination is serving all types.
		for _, st := range servingTypes {
			for _, si := range scw.destinationShards {
				if si.GetServedType(st) == nil {
					return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "source shard %v/%v is not serving type %v", si.Keyspace(), si.ShardName(), st)
				}
			}
		}
	}

	return nil
}

func (scw *SplitCloneWorker) loadVSchema(ctx context.Context) error {
	var keyspaceSchema *vindexes.KeyspaceSchema
	if *useV3ReshardingMode {
		kschema, err := scw.wr.TopoServer().GetVSchema(ctx, scw.destinationKeyspace)
		if err != nil {
			return vterrors.Wrapf(err, "cannot load VSchema for keyspace %v", scw.destinationKeyspace)
		}
		if kschema == nil {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no VSchema for keyspace %v", scw.destinationKeyspace)
		}

		keyspaceSchema, err = vindexes.BuildKeyspaceSchema(kschema, scw.destinationKeyspace)
		if err != nil {
			return vterrors.Wrapf(err, "cannot build vschema for keyspace %v", scw.destinationKeyspace)
		}
		scw.keyspaceSchema = keyspaceSchema
	}
	return nil
}

// findOfflineSourceTablets phase:
// - find one rdonly in the source shard
// - mark it as 'worker' pointing back to us
// - get the aliases of all the source tablets
func (scw *SplitCloneWorker) findOfflineSourceTablets(ctx context.Context) error {
	scw.setState(WorkerStateFindTargets)

	// find an appropriate tablet in the source shards
	scw.sourceAliases = make([]*topodatapb.TabletAlias, len(scw.sourceShards))
	for i, si := range scw.sourceShards {
		var err error
		scw.sourceAliases[i], err = FindWorkerTablet(ctx, scw.wr, scw.cleaner, scw.tsc, scw.cell, si.Keyspace(), si.ShardName(), scw.minHealthyTablets, scw.tabletType)
		if err != nil {
			return vterrors.Wrapf(err, "FindWorkerTablet() failed for %v/%v/%v", scw.cell, si.Keyspace(), si.ShardName())
		}
		scw.wr.Logger().Infof("Using tablet %v as source for %v/%v", topoproto.TabletAliasString(scw.sourceAliases[i]), si.Keyspace(), si.ShardName())
	}
	scw.setFormattedOfflineSources(scw.sourceAliases)

	// get the tablet info for them, and stop their replication
	scw.sourceTablets = make([]*topodatapb.Tablet, len(scw.sourceAliases))
	for i, alias := range scw.sourceAliases {
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		ti, err := scw.wr.TopoServer().GetTablet(shortCtx, alias)
		cancel()
		if err != nil {
			return vterrors.Wrapf(err, "cannot read tablet %v", topoproto.TabletAliasString(alias))
		}
		scw.sourceTablets[i] = ti.Tablet

		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		err = scw.wr.TabletManagerClient().StopSlave(shortCtx, scw.sourceTablets[i])
		cancel()
		if err != nil {
			return vterrors.Wrapf(err, "cannot stop replication on tablet %v", topoproto.TabletAliasString(alias))
		}

		wrangler.RecordStartSlaveAction(scw.cleaner, scw.sourceTablets[i])
	}

	return nil
}

// findTransactionalSources phase:
// - get the aliases of all the source tablets
func (scw *SplitCloneWorker) findTransactionalSources(ctx context.Context) error {
	scw.setState(WorkerStateFindTargets)

	if len(scw.sourceShards) > 1 {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "consistent snapshot can only be used with a single source shard")
	}
	var err error

	// find an appropriate tablet in the source shard
	si := scw.sourceShards[0]
	scw.sourceAliases = make([]*topodatapb.TabletAlias, 1)
	scw.sourceAliases[0], err = FindHealthyTablet(ctx, scw.wr, scw.tsc, scw.cell, si.Keyspace(), si.ShardName(), scw.minHealthyTablets, scw.tabletType)
	if err != nil {
		return vterrors.Wrapf(err, "FindHealthyTablet() failed for %v/%v/%v", scw.cell, si.Keyspace(), si.ShardName())
	}
	scw.wr.Logger().Infof("Using tablet %v as source for %v/%v", topoproto.TabletAliasString(scw.sourceAliases[0]), si.Keyspace(), si.ShardName())
	scw.setFormattedOfflineSources(scw.sourceAliases)

	// get the tablet info
	scw.sourceTablets = make([]*topodatapb.Tablet, 1)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	ti, err := scw.wr.TopoServer().GetTablet(shortCtx, scw.sourceAliases[0])
	cancel()
	if err != nil {
		return vterrors.Wrapf(err, "cannot read tablet %v", topoproto.TabletAliasString(scw.sourceAliases[0]))
	}
	scw.sourceTablets[0] = ti.Tablet

	// stop replication and create transactions to work on
	txs, gtid, err := CreateConsistentTransactions(ctx, ti, scw.wr, scw.cleaner, scw.sourceReaderCount)
	if err != nil {
		return vterrors.Wrapf(err, "error creating consistent transactions")
	}
	scw.wr.Logger().Infof("created %v transactions", len(txs))
	scw.lastPos = gtid
	scw.transactions = txs
	return nil
}

// findDestinationMasters finds for each destination shard the current master.
func (scw *SplitCloneWorker) findDestinationMasters(ctx context.Context) error {
	scw.setState(WorkerStateFindTargets)

	// Make sure we find a master for each destination shard and log it.
	scw.wr.Logger().Infof("Finding a MASTER tablet for each destination shard...")
	for _, si := range scw.destinationShards {
		waitCtx, waitCancel := context.WithTimeout(ctx, *waitForHealthyTabletsTimeout)
		err := scw.tsc.WaitForTablets(waitCtx, scw.cell, si.Keyspace(), si.ShardName(), topodatapb.TabletType_MASTER)
		waitCancel()
		if err != nil {
			return vterrors.Wrapf(err, "cannot find MASTER tablet for destination shard for %v/%v (in cell: %v)", si.Keyspace(), si.ShardName(), scw.cell)
		}
		masters := scw.tsc.GetHealthyTabletStats(si.Keyspace(), si.ShardName(), topodatapb.TabletType_MASTER)
		if len(masters) == 0 {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cannot find MASTER tablet for destination shard for %v/%v (in cell: %v) in HealthCheck: empty TabletStats list", si.Keyspace(), si.ShardName(), scw.cell)
		}
		master := masters[0]

		// Get the MySQL database name of the tablet.
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		scw.destinationDbNames[keyspaceAndShard] = topoproto.TabletDbName(master.Tablet)

		scw.wr.Logger().Infof("Using tablet %v as destination master for %v/%v", topoproto.TabletAliasString(master.Tablet.Alias), si.Keyspace(), si.ShardName())
	}
	scw.wr.Logger().Infof("NOTE: The used master of a destination shard might change over the course of the copy e.g. due to a reparent. The HealthCheck module will track and log master changes and any error message will always refer the actually used master address.")

	return nil
}

// waitForTablets waits for enough serving tablets in the given
// shard (which can be used as input during the diff).
func (scw *SplitCloneWorker) waitForTablets(ctx context.Context, shardInfos []*topo.ShardInfo, timeout time.Duration) error {
	var wg sync.WaitGroup
	rec := concurrency.AllErrorRecorder{}

	if scw.minHealthyTablets > 0 && len(shardInfos) > 0 {
		scw.wr.Logger().Infof("Waiting %v for %d %s/%s RDONLY tablet(s)", timeout, scw.minHealthyTablets, shardInfos[0].Keyspace(), shardInfos[0].ShardName())
	}

	for _, si := range shardInfos {
		wg.Add(1)
		go func(keyspace, shard string) {
			defer wg.Done()
			// We wait for --min_healthy_tablets because we will use several
			// tablets per shard to spread reading the chunks of rows across as many
			// tablets as possible.
			if _, err := waitForHealthyTablets(ctx, scw.wr, scw.tsc, scw.cell, keyspace, shard, scw.minHealthyTablets, timeout, scw.tabletType); err != nil {
				rec.RecordError(err)
			}
		}(si.Keyspace(), si.ShardName())
	}
	wg.Wait()
	return rec.Error()
}

func (scw *SplitCloneWorker) findFirstSourceTablet(ctx context.Context, state StatusWorkerState) (*topodatapb.Tablet, error) {
	if state == WorkerStateCloneOffline {
		// Use the first source tablet which we took offline.
		return scw.sourceTablets[0], nil
	}

	// Pick any healthy serving source tablet.
	si := scw.sourceShards[0]
	tablets := scw.tsc.GetHealthyTabletStats(si.Keyspace(), si.ShardName(), scw.tabletType)
	if len(tablets) == 0 {
		// We fail fast on this problem and don't retry because at the start all tablets should be healthy.
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no healthy %v tablet in source shard (%v) available (required to find out the schema)", topodatapb.TabletType_name[int32(scw.tabletType)], topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName()))
	}
	return tablets[0].Tablet, nil
}

func (scw *SplitCloneWorker) getCounters(state StatusWorkerState) ([]*stats.CountersWithSingleLabel, *tableStatusList) {
	switch state {
	case WorkerStateCloneOnline:
		return []*stats.CountersWithSingleLabel{statsOnlineInsertsCounters, statsOnlineUpdatesCounters, statsOnlineDeletesCounters, statsOnlineEqualRowsCounters},
			scw.tableStatusListOnline
	case WorkerStateCloneOffline:
		return []*stats.CountersWithSingleLabel{statsOfflineInsertsCounters, statsOfflineUpdatesCounters, statsOfflineDeletesCounters, statsOfflineEqualRowsCounters},
			scw.tableStatusListOffline
	default:
		panic("should not happen")
	}
}

func (scw *SplitCloneWorker) startExecutor(ctx context.Context, wg *sync.WaitGroup, keyspace, shard string, insertChannel chan string, threadID int, processError func(string, ...interface{})) {
	defer wg.Done()
	t := scw.getThrottler(keyspace, shard)
	//defer t.ThreadFinished(threadID)

	executor := newExecutor(scw.wr, scw.tsc, t, keyspace, shard, threadID)
	if err := executor.fetchLoop(ctx, insertChannel); err != nil {
		processError("executer.FetchLoop failed: %v", err)
	}
}

func mergeOrSingle(readers []ResultReader, td *tabletmanagerdatapb.TableDefinition) (ResultReader, error) {
	if len(readers) == 1 {
		return readers[0], nil
	}

	sourceReader, err := NewResultMerger(readers, len(td.PrimaryKeyColumns))
	if err != nil {
		return nil, err
	}

	return sourceReader, nil
}

func closeReaders(ctx context.Context, readers []ResultReader) {
	for _, reader := range readers {
		if reader != nil {
			reader.Close(ctx)
		}
	}
}

func (scw *SplitCloneWorker) getSourceResultReader(ctx context.Context, td *tabletmanagerdatapb.TableDefinition, state StatusWorkerState, chunk chunk, txID int64) (ResultReader, error) {
	sourceReaders := make([]ResultReader, len(scw.sourceShards))

	for shardIndex, si := range scw.sourceShards {
		var sourceResultReader ResultReader
		if state == WorkerStateCloneOffline && scw.useConsistentSnapshot {
			var err error
			if txID < 1 {
				return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "tried using consistent snapshot without a valid transaction")
			}
			tp := newShardTabletProvider(scw.tsc, scw.tabletTracker, si.Keyspace(), si.ShardName(), scw.tabletType)
			sourceResultReader, err = NewTransactionalRestartableResultReader(ctx, scw.wr.Logger(), tp, td, chunk, false, txID)
			if err != nil {
				closeReaders(ctx, sourceReaders)
				return nil, vterrors.Wrapf(err, "NewTransactionalRestartableResultReader for source: %v failed", tp.description())
			}
		} else {
			var err error
			var tp tabletProvider
			allowMultipleRetries := true
			if state == WorkerStateCloneOffline {
				tp = newSingleTabletProvider(ctx, scw.wr.TopoServer(), scw.sourceAliases[shardIndex])
				// allowMultipleRetries is false to avoid that we'll keep retrying
				// on the same tablet alias for hours. This guards us against the
				// situation that an offline tablet gets restarted and serves again.
				// In that case we cannot use it because its replication is no
				// longer stopped at the same point as we took it offline initially.
				allowMultipleRetries = false
			} else {
				tp = newShardTabletProvider(scw.tsc, scw.tabletTracker, si.Keyspace(), si.ShardName(), scw.tabletType)
			}
			sourceResultReader, err = NewRestartableResultReader(ctx, scw.wr.Logger(), tp, td, chunk, allowMultipleRetries)
			if err != nil {
				closeReaders(ctx, sourceReaders)
				return nil, vterrors.Wrapf(err, "NewRestartableResultReader for source: %v failed", tp.description())
			}
		}
		sourceReaders[shardIndex] = sourceResultReader
	}
	resultReader, err := mergeOrSingle(sourceReaders, td)
	if err != nil {
		closeReaders(ctx, sourceReaders)
		return nil, err
	}
	return resultReader, err
}

func (scw *SplitCloneWorker) getDestinationResultReader(ctx context.Context, td *tabletmanagerdatapb.TableDefinition, state StatusWorkerState, chunk chunk) (ResultReader, error) {
	destReaders := make([]ResultReader, len(scw.destinationShards))

	for shardIndex, si := range scw.destinationShards {
		tp := newShardTabletProvider(scw.tsc, scw.tabletTracker, si.Keyspace(), si.ShardName(), topodatapb.TabletType_MASTER)
		destResultReader, err := NewRestartableResultReader(ctx, scw.wr.Logger(), tp, td, chunk, true /* allowMultipleRetries */)
		if err != nil {
			closeReaders(ctx, destReaders)
			return nil, vterrors.Wrapf(err, "NewRestartableResultReader for destination: %v failed", tp.description())
		}
		destReaders[shardIndex] = destResultReader
	}
	resultReader, err := mergeOrSingle(destReaders, td)
	if err != nil {
		closeReaders(ctx, destReaders)
		return nil, err
	}
	return resultReader, err
}

func (scw *SplitCloneWorker) cloneAChunk(ctx context.Context, td *tabletmanagerdatapb.TableDefinition, tableIndex int, chunk chunk, processError func(string, ...interface{}), state StatusWorkerState, tableStatusList *tableStatusList, keyResolver keyspaceIDResolver, start time.Time, insertChannels []chan string, txID int64, statsCounters []*stats.CountersWithSingleLabel) {
	errPrefix := fmt.Sprintf("table=%v chunk=%v", td.Name, chunk)

	var err error

	if err := checkDone(ctx); err != nil {
		processError("%v: Context expired while this thread was waiting for its turn. Context error: %v", errPrefix, err)
		return
	}

	tableStatusList.threadStarted(tableIndex)
	defer tableStatusList.threadDone(tableIndex)

	if state == WorkerStateCloneOnline {
		// Wait for enough healthy tablets (they might have become unhealthy
		// and their replication lag might have increased since we started.)
		if err := scw.waitForTablets(ctx, scw.sourceShards, *retryDuration); err != nil {
			processError("%v: No healthy source tablets found (gave up after %v): %v", errPrefix, time.Since(start), err)
			return
		}
	}

	// Set up readers for the diff. There will be one reader for every
	// source and destination shard.
	sourceReader, err := scw.getSourceResultReader(ctx, td, state, chunk, txID)
	if err != nil {
		processError("%v NewResultMerger for source tablets failed: %v", errPrefix, err)
		return
	}
	defer sourceReader.Close(ctx)
	destReader, err := scw.getDestinationResultReader(ctx, td, state, chunk)
	if err != nil {
		processError("%v NewResultMerger for destinations tablets failed: %v", errPrefix, err)
		return
	}
	defer destReader.Close(ctx)
	dbNames := make([]string, len(scw.destinationShards))
	for i, si := range scw.destinationShards {
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		dbNames[i] = scw.destinationDbNames[keyspaceAndShard]
	}
	// Compare the data and reconcile any differences.
	differ, err := NewRowDiffer2(ctx, sourceReader, destReader, td, tableStatusList, tableIndex,
		scw.destinationShards, keyResolver,
		insertChannels, ctx.Done(), dbNames, scw.writeQueryMaxRows, scw.writeQueryMaxSize, statsCounters)
	if err != nil {
		processError("%v: NewRowDiffer2 failed: %v", errPrefix, err)
		return
	}
	// Ignore the diff report because all diffs should get reconciled.
	_ /* DiffReport */, err = differ.Diff()
	if err != nil {
		processError("%v: RowDiffer2 failed: %v", errPrefix, err)
		return
	}
}

type workUnit struct {
	td       *tabletmanagerdatapb.TableDefinition
	chunk    chunk
	threadID int
	resolver keyspaceIDResolver
}

func (scw *SplitCloneWorker) startCloningData(ctx context.Context, state StatusWorkerState, sourceSchemaDefinition *tabletmanagerdatapb.SchemaDefinition,
	processError func(string, ...interface{}), firstSourceTablet *topodatapb.Tablet, tableStatusList *tableStatusList,
	start time.Time, statsCounters []*stats.CountersWithSingleLabel, insertChannels []chan string, wg *sync.WaitGroup) error {

	workPipeline := make(chan workUnit, 10) // We'll use a small buffer so producers do not run too far ahead of consumers
	queryService, err := tabletconn.GetDialer()(firstSourceTablet, true)
	if err != nil {
		return vterrors.Wrap(err, "failed to create queryService")
	}
	defer queryService.Close(ctx)

	// Let's start the work consumers
	for i := 0; i < scw.sourceReaderCount; i++ {
		var txID int64
		if scw.useConsistentSnapshot && state == WorkerStateCloneOffline {
			txID = scw.transactions[i]
		} else {
			txID = -1
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workPipeline {
				scw.cloneAChunk(ctx, work.td, work.threadID, work.chunk, processError, state, tableStatusList, work.resolver, start, insertChannels, txID, statsCounters)
			}
		}()
	}

	// And now let's start producing work units
	for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
		td = reorderColumnsPrimaryKeyFirst(td)

		keyResolver, err := scw.createKeyResolver(td)
		if err != nil {
			return vterrors.Wrapf(err, "cannot resolve sharding keys for keyspace %v", scw.destinationKeyspace)
		}

		// TODO(mberlin): We're going to chunk *all* source shards based on the MIN
		// and MAX values of the *first* source shard. Is this going to be a problem?
		chunks, err := generateChunks(ctx, scw.wr, firstSourceTablet, td, scw.chunkCount, scw.minRowsPerChunk)
		if err != nil {
			return vterrors.Wrap(err, "failed to split table into chunks")
		}
		tableStatusList.setThreadCount(tableIndex, len(chunks))

		for _, c := range chunks {
			workPipeline <- workUnit{td: td, chunk: c, threadID: tableIndex, resolver: keyResolver}
		}
	}

	close(workPipeline)

	return nil
}

// copy phase:
//	- copy the data from source tablets to destination masters (with replication on)
// Assumes that the schema has already been created on each destination tablet
// (probably from vtctl's CopySchemaShard)
func (scw *SplitCloneWorker) clone(ctx context.Context, state StatusWorkerState) error {
	if state != WorkerStateCloneOnline && state != WorkerStateCloneOffline {
		panic(fmt.Sprintf("invalid state passed to clone(): %v", state))
	}
	scw.setState(state)
	start := time.Now()
	defer func() {
		statsStateDurationsNs.Set(string(state), time.Now().Sub(start).Nanoseconds())
	}()

	var firstSourceTablet, err = scw.findFirstSourceTablet(ctx, state)
	if err != nil {
		return err
	}

	statsCounters, tableStatusList := scw.getCounters(state)

	// The throttlers exist only for the duration of this clone() call.
	// That means a SplitClone invocation with both online and offline phases
	// will create throttlers for each phase.
	if err := scw.createThrottlers(); err != nil {
		return err
	}
	defer scw.closeThrottlers()

	sourceSchemaDefinition, err := scw.getSourceSchema(ctx, firstSourceTablet)
	if err != nil {
		return err
	}
	scw.wr.Logger().Infof("Source tablet 0 has %v tables to copy", len(sourceSchemaDefinition.TableDefinitions))
	tableStatusList.initialize(sourceSchemaDefinition)

	var firstError error

	ctx, cancelCopy := context.WithCancel(ctx)
	defer cancelCopy()
	processError := func(format string, args ...interface{}) {
		// in theory we could have two threads see firstError as null and both write to the variable
		// that should not cause any problems though - canceling and logging is concurrently safe,
		// and overwriting the variable will not cause any problems
		scw.wr.Logger().Errorf(format, args...)
		if firstError == nil {
			firstError = vterrors.Errorf(vtrpc.Code_INTERNAL, format, args...)
			cancelCopy()
		}
	}

	// NOTE: Code below this point must *not* use "return" to exit this Go routine
	// early. Instead, "processError" must be called to cancel the context. This
	// way all launched Go routines will terminate as well.
	// (However, "return" in a new Go routine, i.e. not this one, is fine.)
	// If Go routines have already been started, make sure that Wait() on the
	// respective WaitGroup is called as well e.g. "destinationWaitGroup.Wait()"
	// must always be reached. Waiting for the Go routines is important to avoid
	// races between "defer throttler.ThreadFinished()" (must be executed first)
	// and "defer scw.closeThrottlers()". Otherwise, vtworker will panic.

	// In parallel, setup the channels to send SQL data chunks to for each destination tablet:
	insertChannels := make([]chan string, len(scw.destinationShards))
	destinationWaitGroup := sync.WaitGroup{}
	for shardIndex, si := range scw.destinationShards {
		// We create one channel per destination tablet. It is sized to have a
		// buffer of a maximum of destinationWriterCount * 2 items, to hopefully
		// always have data. We then have destinationWriterCount go routines reading
		// from it.
		insertChannels[shardIndex] = make(chan string, scw.destinationWriterCount*2)

		for j := 0; j < scw.destinationWriterCount; j++ {
			destinationWaitGroup.Add(1)
			go scw.startExecutor(ctx, &destinationWaitGroup, si.Keyspace(), si.ShardName(), insertChannels[shardIndex], j, processError)
		}
	}

	// Now for each table, read data chunks and send them to all
	// insertChannels
	readers := sync.WaitGroup{}

	err = scw.startCloningData(ctx, state, sourceSchemaDefinition, processError, firstSourceTablet, tableStatusList, start, statsCounters, insertChannels, &readers)
	if err != nil {
		return vterrors.Wrap(err, "failed to startCloningData")
	}
	readers.Wait()

	for shardIndex := range scw.destinationShards {
		close(insertChannels[shardIndex])
	}
	destinationWaitGroup.Wait()

	return firstError
}

func (scw *SplitCloneWorker) setUpVReplication(ctx context.Context) error {
	wg := sync.WaitGroup{}
	// Create and populate the vreplication table to give filtered replication
	// a starting point.
	queries := make([]string, 0, 4)
	queries = append(queries, binlogplayer.CreateVReplicationTable()...)

	// get the current position from the sources
	sourcePositions := make([]string, len(scw.sourceShards))

	if scw.useConsistentSnapshot {
		sourcePositions[0] = scw.lastPos
	} else {
		for shardIndex := range scw.sourceShards {
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			status, err := scw.wr.TabletManagerClient().SlaveStatus(shortCtx, scw.sourceTablets[shardIndex])
			cancel()
			if err != nil {
				return err
			}
			sourcePositions[shardIndex] = status.Position
		}
	}
	cancelableCtx, cancel := context.WithCancel(ctx)
	rec := concurrency.AllErrorRecorder{}
	handleError := func(e error) {
		rec.RecordError(e)
		cancel()
	}

	for _, si := range scw.destinationShards {
		wg.Add(1)
		go func(keyspace, shard string, kr *topodatapb.KeyRange) {
			defer wg.Done()
			scw.wr.Logger().Infof("Making and populating vreplication table")

			exc := newExecutor(scw.wr, scw.tsc, nil, keyspace, shard, 0)
			for shardIndex, src := range scw.sourceShards {
				// Check if any error occurred in any other gorouties:
				select {
				case <-cancelableCtx.Done():
					return // Error somewhere, terminate
				default:
				}

				bls := &binlogdatapb.BinlogSource{
					Keyspace: src.Keyspace(),
					Shard:    src.ShardName(),
				}
				if scw.tables == nil {
					bls.KeyRange = kr
				} else {
					bls.Tables = scw.tables
				}
				// TODO(mberlin): Fill in scw.maxReplicationLag once the adapative throttler is enabled by default.
				qr, err := exc.vreplicationExec(cancelableCtx, binlogplayer.CreateVReplication("SplitClone", bls, sourcePositions[shardIndex], scw.maxTPS, throttler.ReplicationLagModuleDisabled, time.Now().Unix()))
				if err != nil {
					handleError(vterrors.Wrap(err, "vreplication queries failed"))
					cancel()
					return
				}
				if err := scw.wr.SourceShardAdd(cancelableCtx, keyspace, shard, uint32(qr.InsertID), src.Keyspace(), src.ShardName(), src.Shard.KeyRange, scw.tables); err != nil {
					handleError(vterrors.Wrap(err, "could not add source shard"))
					break
				}
			}
			// refreshState will cause the destination to become non-serving because
			// it's now participating in the resharding workflow.
			if err := exc.refreshState(ctx); err != nil {
				handleError(vterrors.Wrapf(err, "RefreshState failed on tablet %v/%v", keyspace, shard))
			}
		}(si.Keyspace(), si.ShardName(), si.KeyRange)
	}
	wg.Wait()

	return rec.Error()
}

func (scw *SplitCloneWorker) getSourceSchema(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.SchemaDefinition, error) {
	// get source schema from the first shard
	// TODO(alainjobart): for now, we assume the schema is compatible
	// on all source shards. Furthermore, we estimate the number of rows
	// in each source shard for each table to be about the same
	// (rowCount is used to estimate an ETA)
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceSchemaDefinition, err := scw.wr.GetSchema(shortCtx, tablet.Alias, scw.tables, scw.excludeTables, false /* includeViews */)
	cancel()
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot get schema from source %v", topoproto.TabletAliasString(tablet.Alias))
	}
	if len(sourceSchemaDefinition.TableDefinitions) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no tables matching the table filter in tablet %v", topoproto.TabletAliasString(tablet.Alias))
	}
	for _, td := range sourceSchemaDefinition.TableDefinitions {
		if len(td.Columns) == 0 {
			return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "schema for table %v has no columns", td.Name)
		}
	}
	return sourceSchemaDefinition, nil
}

// createKeyResolver is called at the start of each chunk pipeline.
// It creates a keyspaceIDResolver which translates a given row to a
// keyspace ID. This is necessary to route the to be copied rows to the
// different destination shards.
func (scw *SplitCloneWorker) createKeyResolver(td *tabletmanagerdatapb.TableDefinition) (keyspaceIDResolver, error) {
	if scw.cloneType == verticalSplit {
		// VerticalSplitClone currently always has exactly one destination shard
		// and therefore does not require routing between multiple shards.
		return nil, nil
	}

	if *useV3ReshardingMode {
		return newV3ResolverFromTableDefinition(scw.keyspaceSchema, td)
	}
	return newV2Resolver(scw.destinationKeyspaceInfo, td)
}

// StatsUpdate receives replication lag updates for each destination master
// and forwards them to the respective throttler instance.
// It also forwards any update to the TabletStatsCache to keep it up to date.
// It is part of the discovery.HealthCheckStatsListener interface.
func (scw *SplitCloneWorker) StatsUpdate(ts *discovery.TabletStats) {
	scw.tsc.StatsUpdate(ts)

	// Ignore unless REPLICA or RDONLY.
	if ts.Target.TabletType != topodatapb.TabletType_REPLICA && ts.Target.TabletType != topodatapb.TabletType_RDONLY {
		return
	}

	// Lock throttlers mutex to avoid that this method (and the called method
	// Throttler.RecordReplicationLag()) races with closeThrottlers() (which calls
	// Throttler.Close()).
	scw.throttlersMu.Lock()
	defer scw.throttlersMu.Unlock()

	t := scw.getThrottlerLocked(ts.Target.Keyspace, ts.Target.Shard)
	if t != nil {
		t.RecordReplicationLag(time.Now(), ts)
	}
}

func (scw *SplitCloneWorker) createThrottlers() error {
	scw.throttlersMu.Lock()
	defer scw.throttlersMu.Unlock()

	for _, si := range scw.destinationShards {
		// Set up the throttler for each destination shard.
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		t, err := throttler.NewThrottler(keyspaceAndShard, "transactions", scw.destinationWriterCount, scw.maxTPS, scw.maxReplicationLag)
		if err != nil {
			return vterrors.Wrap(err, "cannot instantiate throttler")
		}
		scw.throttlers[keyspaceAndShard] = t
	}
	return nil
}

func (scw *SplitCloneWorker) getThrottler(keyspace, shard string) *throttler.Throttler {
	scw.throttlersMu.Lock()
	defer scw.throttlersMu.Unlock()

	return scw.getThrottlerLocked(keyspace, shard)
}

func (scw *SplitCloneWorker) getThrottlerLocked(keyspace, shard string) *throttler.Throttler {
	keyspaceAndShard := topoproto.KeyspaceShardString(keyspace, shard)
	return scw.throttlers[keyspaceAndShard]
}

func (scw *SplitCloneWorker) closeThrottlers() {
	scw.throttlersMu.Lock()
	defer scw.throttlersMu.Unlock()

	for keyspaceAndShard, t := range scw.throttlers {
		t.Close()
		delete(scw.throttlers, keyspaceAndShard)
	}
}
