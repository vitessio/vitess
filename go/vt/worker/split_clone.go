// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
	"fmt"
	"html/template"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
	"github.com/youtube/vitess/go/vt/worker/events"
	"github.com/youtube/vitess/go/vt/wrangler"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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

	wr                  *wrangler.Wrangler
	cloneType           cloneType
	cell                string
	destinationKeyspace string
	shard               string
	online              bool
	offline             bool
	// verticalSplit only: List of tables which should be split out.
	tables []string
	// horizontalResharding only: List of tables which will be skipped.
	excludeTables     []string
	strategy          *splitStrategy
	chunkCount        int
	minRowsPerChunk   int
	sourceReaderCount int
	writeQueryMaxRows int
	writeQueryMaxSize int
	// TODO(mberlin): Delete this when our testing found out that an extra flag
	// for this is not necessary.
	writeQueryMaxRowsDelete int
	destinationWriterCount  int
	minHealthyRdonlyTablets int
	maxTPS                  int64
	cleaner                 *wrangler.Cleaner
	tabletTracker           *TabletTracker

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
	sourceAliases []*topodatapb.TabletAlias
	sourceTablets []*topodatapb.Tablet
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

	// offlineSourceAliases has the list of tablets (per source shard) we took
	// offline for the WorkerStateCloneOffline phase.
	// Populated shortly before WorkerStateCloneOffline, read-only after that.
	offlineSourceAliases []*topodatapb.TabletAlias

	// formattedOfflineSourcesMu guards all fields in this group.
	formattedOfflineSourcesMu sync.Mutex
	// formattedOfflineSources is a space separated list of
	// "offlineSourceAliases". It is used by the StatusAs* methods to output the
	// used source tablets during the offline clone phase.
	formattedOfflineSources string

	// tableStatusList* holds the status for each table.
	// populated during WorkerStateCloneOnline
	tableStatusListOnline *tableStatusList
	// populated during WorkerStateCloneOffline
	tableStatusListOffline *tableStatusList
	// aliases of tablets that need to have their state refreshed.
	// Only populated once, read-only after that.
	refreshAliases [][]*topodatapb.TabletAlias
	refreshTablets []map[topodatapb.TabletAlias]*topo.TabletInfo

	ev event.Updater
}

// newSplitCloneWorker returns a new worker object for the SplitClone command.
func newSplitCloneWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, online, offline bool, excludeTables []string, strategyStr string, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, writeQueryMaxRowsDelete, destinationWriterCount, minHealthyRdonlyTablets int, maxTPS int64) (Worker, error) {
	return newCloneWorker(wr, horizontalResharding, cell, keyspace, shard, online, offline, nil /* tables */, excludeTables, strategyStr, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, writeQueryMaxRowsDelete, destinationWriterCount, minHealthyRdonlyTablets, maxTPS)
}

// newVerticalSplitCloneWorker returns a new worker object for the
// VerticalSplitClone command.
func newVerticalSplitCloneWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, online, offline bool, tables []string, strategyStr string, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, writeQueryMaxRowsDelete, destinationWriterCount, minHealthyRdonlyTablets int, maxTPS int64) (Worker, error) {
	return newCloneWorker(wr, verticalSplit, cell, keyspace, shard, online, offline, tables, nil /* excludeTables */, strategyStr, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, writeQueryMaxRowsDelete, destinationWriterCount, minHealthyRdonlyTablets, maxTPS)
}

// newCloneWorker returns a new SplitCloneWorker object which is used both by
// the SplitClone and VerticalSplitClone command.
// TODO(mberlin): Rename SplitCloneWorker to cloneWorker.
func newCloneWorker(wr *wrangler.Wrangler, cloneType cloneType, cell, keyspace, shard string, online, offline bool, tables, excludeTables []string, strategyStr string, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, writeQueryMaxRowsDelete, destinationWriterCount, minHealthyRdonlyTablets int, maxTPS int64) (Worker, error) {
	if cloneType != horizontalResharding && cloneType != verticalSplit {
		return nil, fmt.Errorf("unknown cloneType: %v This is a bug. Please report", cloneType)
	}
	if tables != nil && len(tables) == 0 {
		return nil, errors.New("list of tablets to be split out must not be empty")
	}
	strategy, err := newSplitStrategy(wr.Logger(), strategyStr)
	if err != nil {
		return nil, err
	}
	if maxTPS != throttler.MaxRateModuleDisabled {
		wr.Logger().Infof("throttling enabled and set to a max of %v transactions/second", maxTPS)
	}
	if maxTPS != throttler.MaxRateModuleDisabled && maxTPS < int64(destinationWriterCount) {
		return nil, fmt.Errorf("-max_tps must be >= -destination_writer_count: %v >= %v", maxTPS, destinationWriterCount)
	}
	if !online && !offline {
		return nil, errors.New("at least one clone phase (-online, -offline) must be enabled (and not set to false)")
	}
	scw := &SplitCloneWorker{
		StatusWorker:            NewStatusWorker(),
		wr:                      wr,
		cloneType:               cloneType,
		cell:                    cell,
		destinationKeyspace:     keyspace,
		shard:                   shard,
		online:                  online,
		offline:                 offline,
		tables:                  tables,
		excludeTables:           excludeTables,
		strategy:                strategy,
		chunkCount:              chunkCount,
		minRowsPerChunk:         minRowsPerChunk,
		sourceReaderCount:       sourceReaderCount,
		writeQueryMaxRows:       writeQueryMaxRows,
		writeQueryMaxSize:       writeQueryMaxSize,
		writeQueryMaxRowsDelete: writeQueryMaxRowsDelete,
		destinationWriterCount:  destinationWriterCount,
		minHealthyRdonlyTablets: minHealthyRdonlyTablets,
		maxTPS:                  maxTPS,
		cleaner:                 &wrangler.Cleaner{},
		tabletTracker:           NewTabletTracker(),
		throttlers:              make(map[string]*throttler.Throttler),

		destinationDbNames: make(map[string]string),

		tableStatusListOnline:  &tableStatusList{},
		tableStatusListOffline: &tableStatusList{},
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
			Strategy:      scw.strategy.String(),
		}
	case verticalSplit:
		scw.ev = &events.VerticalSplitClone{
			Cell:     scw.cell,
			Keyspace: scw.destinationKeyspace,
			Shard:    scw.shard,
			Tables:   scw.tables,
			Strategy: scw.strategy.String(),
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
		result += "Copying from: " + scw.formatOnlineSources() + "\n"
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
			scw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
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
			scw.wr.Logger().Errorf("HealthCheck.Close() failed: %v", err)
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
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// Phase 2a: Find destination master tablets.
	if err := scw.findDestinationMasters(ctx); err != nil {
		return fmt.Errorf("findDestinationMasters() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}
	// Phase 2b: Wait for minimum number of destination tablets (required for the
	// diff). Note that while we wait for the minimum number, we'll always use
	// *all* available RDONLY tablets from each destination shard.
	if err := scw.waitForTablets(ctx, scw.destinationShards, *waitForHealthyTabletsTimeout); err != nil {
		return fmt.Errorf("waitForDestinationTablets(destinationShards) failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// Phase 3: (optional) online clone.
	if scw.online {
		scw.wr.Logger().Infof("Online clone will be run now.")
		// 3a: Wait for minimum number of source tablets (required for the diff).
		if err := scw.waitForTablets(ctx, scw.sourceShards, *waitForHealthyTabletsTimeout); err != nil {
			return fmt.Errorf("waitForDestinationTablets(sourceShards) failed: %v", err)
		}
		// 3b: Clone the data.
		start := time.Now()
		if err := scw.clone(ctx, WorkerStateCloneOnline); err != nil {
			return fmt.Errorf("online clone() failed: %v", err)
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

		// 4a: Take source tablets out of serving for an exact snapshot.
		if err := scw.findOfflineSourceTablets(ctx); err != nil {
			return fmt.Errorf("findSourceTablets() failed: %v", err)
		}
		if err := checkDone(ctx); err != nil {
			return err
		}

		// 4b: Clone the data.
		start := time.Now()
		if err := scw.clone(ctx, WorkerStateCloneOffline); err != nil {
			return fmt.Errorf("offline clone() failed: %v", err)
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
		return fmt.Errorf("cannot read (destination) keyspace %v: %v", scw.destinationKeyspace, err)
	}

	// Set source and destination shard infos.
	switch scw.cloneType {
	case horizontalResharding:
		if err := scw.initShardsForHorizontalResharding(ctx); err != nil {
			return err
		}
	case verticalSplit:
		if err := scw.initShardsForVerticalSplit(ctx); err != nil {
			return err
		}
	}

	if err := scw.sanityCheckShardInfos(); err != nil {
		return err
	}

	if scw.cloneType == horizontalResharding {
		if err := scw.loadVSchema(ctx); err != nil {
			return err
		}
	}

	// Initialize healthcheck and add destination shards to it.
	scw.healthCheck = discovery.NewHealthCheck(*remoteActionsTimeout, *healthcheckRetryDelay, *healthCheckTimeout)
	scw.tsc = discovery.NewTabletStatsCacheDoNotSetListener(scw.cell)
	// We set sendDownEvents=true because it's required by TabletStatsCache.
	scw.healthCheck.SetListener(scw, true /* sendDownEvents */)

	// Start watchers to get tablets added automatically to healthCheck.
	allShards := append(scw.sourceShards, scw.destinationShards...)
	for _, si := range allShards {
		watcher := discovery.NewShardReplicationWatcher(scw.wr.TopoServer(), scw.healthCheck,
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
		return fmt.Errorf("cannot FindOverlappingShards in %v: %v", scw.destinationKeyspace, err)
	}

	// find the shard we mentioned in there, if any
	os := topotools.OverlappingShardsForShard(osList, scw.shard)
	if os == nil {
		return fmt.Errorf("the specified shard %v/%v is not in any overlapping shard", scw.destinationKeyspace, scw.shard)
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

	return nil
}

func (scw *SplitCloneWorker) initShardsForVerticalSplit(ctx context.Context) error {
	if len(scw.destinationKeyspaceInfo.ServedFroms) == 0 {
		return fmt.Errorf("destination keyspace %v has no KeyspaceServedFrom", scw.destinationKeyspace)
	}

	// Determine the source keyspace.
	servedFrom := ""
	for _, st := range servingTypes {
		sf := scw.destinationKeyspaceInfo.GetServedFrom(st)
		if sf == nil {
			return fmt.Errorf("destination keyspace %v is serving type %v", scw.destinationKeyspace, st)
		}
		if servedFrom == "" {
			servedFrom = sf.Keyspace
		} else {
			if servedFrom != sf.Keyspace {
				return fmt.Errorf("destination keyspace %v is serving from multiple source keyspaces %v and %v", scw.destinationKeyspace, servedFrom, sf.Keyspace)
			}
		}
	}
	sourceKeyspace := servedFrom

	// Init the source and destination shard info.
	sourceShardInfo, err := scw.wr.TopoServer().GetShard(ctx, sourceKeyspace, scw.shard)
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
			return fmt.Errorf("destination shard %v/%v has filtered replication already enabled from a previous resharding (ShardInfo is set)."+
				" This requires manual intervention e.g. use vtctl SourceShardDelete to remove it",
				si.Keyspace(), si.ShardName())
		}
	}
	// Verify that the source is serving all serving types.
	for _, st := range servingTypes {
		for _, si := range scw.sourceShards {
			if si.GetServedType(st) == nil {
				return fmt.Errorf("source shard %v/%v is not serving type %v", si.Keyspace(), si.ShardName(), st)
			}
		}
	}

	switch scw.cloneType {
	case horizontalResharding:
		// Verify that the destination is not serving yet.
		for _, si := range scw.destinationShards {
			if len(si.ServedTypes) > 0 {
				return fmt.Errorf("destination shard %v/%v is serving some types", si.Keyspace(), si.ShardName())
			}
		}
	case verticalSplit:
		// Verify that the destination is serving all types.
		for _, st := range servingTypes {
			for _, si := range scw.destinationShards {
				if si.GetServedType(st) == nil {
					return fmt.Errorf("source shard %v/%v is not serving type %v", si.Keyspace(), si.ShardName(), st)
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
			return fmt.Errorf("cannot load VSchema for keyspace %v: %v", scw.destinationKeyspace, err)
		}
		if kschema == nil {
			return fmt.Errorf("no VSchema for keyspace %v", scw.destinationKeyspace)
		}

		keyspaceSchema, err = vindexes.BuildKeyspaceSchema(kschema, scw.destinationKeyspace)
		if err != nil {
			return fmt.Errorf("cannot build vschema for keyspace %v: %v", scw.destinationKeyspace, err)
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
	scw.offlineSourceAliases = make([]*topodatapb.TabletAlias, len(scw.sourceShards))
	for i, si := range scw.sourceShards {
		var err error
		scw.offlineSourceAliases[i], err = FindWorkerTablet(ctx, scw.wr, scw.cleaner, scw.tsc, scw.cell, si.Keyspace(), si.ShardName(), scw.minHealthyRdonlyTablets)
		if err != nil {
			return fmt.Errorf("FindWorkerTablet() failed for %v/%v/%v: %v", scw.cell, si.Keyspace(), si.ShardName(), err)
		}
		scw.wr.Logger().Infof("Using tablet %v as source for %v/%v", topoproto.TabletAliasString(scw.offlineSourceAliases[i]), si.Keyspace(), si.ShardName())
	}
	scw.setFormattedOfflineSources(scw.offlineSourceAliases)

	// get the tablet info for them, and stop their replication
	scw.sourceTablets = make([]*topodatapb.Tablet, len(scw.offlineSourceAliases))
	for i, alias := range scw.offlineSourceAliases {
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		ti, err := scw.wr.TopoServer().GetTablet(shortCtx, alias)
		cancel()
		if err != nil {
			return fmt.Errorf("cannot read tablet %v: %v", topoproto.TabletAliasString(alias), err)
		}
		scw.sourceTablets[i] = ti.Tablet

		shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
		err = scw.wr.TabletManagerClient().StopSlave(shortCtx, scw.sourceTablets[i])
		cancel()
		if err != nil {
			return fmt.Errorf("cannot stop replication on tablet %v", topoproto.TabletAliasString(alias))
		}

		wrangler.RecordStartSlaveAction(scw.cleaner, scw.sourceTablets[i])
	}

	return nil
}

// findDestinationMasters finds for each destination shard the current master.
func (scw *SplitCloneWorker) findDestinationMasters(ctx context.Context) error {
	scw.setState(WorkerStateFindTargets)

	// Make sure we find a master for each destination shard and log it.
	scw.wr.Logger().Infof("Finding a MASTER tablet for each destination shard...")
	for _, si := range scw.destinationShards {
		waitCtx, waitCancel := context.WithTimeout(ctx, *waitForHealthyTabletsTimeout)
		defer waitCancel()
		if err := scw.tsc.WaitForTablets(waitCtx, scw.cell, si.Keyspace(), si.ShardName(), []topodatapb.TabletType{topodatapb.TabletType_MASTER}); err != nil {
			return fmt.Errorf("cannot find MASTER tablet for destination shard for %v/%v (in cell: %v): %v", si.Keyspace(), si.ShardName(), scw.cell, err)
		}
		masters := scw.tsc.GetHealthyTabletStats(si.Keyspace(), si.ShardName(), topodatapb.TabletType_MASTER)
		if len(masters) == 0 {
			return fmt.Errorf("cannot find MASTER tablet for destination shard for %v/%v (in cell: %v) in HealthCheck: empty TabletStats list", si.Keyspace(), si.ShardName(), scw.cell)
		}
		master := masters[0]

		// Get the MySQL database name of the tablet.
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		scw.destinationDbNames[keyspaceAndShard] = topoproto.TabletDbName(master.Tablet)

		// TODO(mberlin): Verify on the destination master that the
		// _vt.blp_checkpoint table has the latest schema.

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
	for _, si := range shardInfos {
		wg.Add(1)
		go func(keyspace, shard string) {
			defer wg.Done()
			// We wait for --min_healthy_rdonly_tablets because we will use several
			// tablets per shard to spread reading the chunks of rows across as many
			// tablets as possible.
			if _, err := waitForHealthyRdonlyTablets(ctx, scw.wr, scw.tsc, scw.cell, keyspace, shard, scw.minHealthyRdonlyTablets, timeout); err != nil {
				rec.RecordError(err)
			}
		}(si.Keyspace(), si.ShardName())
	}
	wg.Wait()
	return rec.Error()
}

// Find all tablets on all destination shards. This should be done immediately before refreshing
// state on these tablets, to minimize the chances of the topo changing in between.
func (scw *SplitCloneWorker) findRefreshTargets(ctx context.Context) error {
	scw.refreshAliases = make([][]*topodatapb.TabletAlias, len(scw.destinationShards))
	scw.refreshTablets = make([]map[topodatapb.TabletAlias]*topo.TabletInfo, len(scw.destinationShards))

	for shardIndex, si := range scw.destinationShards {
		refreshAliases, refreshTablets, err := resolveRefreshTabletsForShard(ctx, si.Keyspace(), si.ShardName(), scw.wr)
		if err != nil {
			return err
		}
		scw.refreshAliases[shardIndex], scw.refreshTablets[shardIndex] = refreshAliases, refreshTablets
	}

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

	var firstSourceTablet *topodatapb.Tablet
	if state == WorkerStateCloneOffline {
		// Use the first source tablet which we took offline.
		firstSourceTablet = scw.sourceTablets[0]
	} else {
		// Pick any healthy serving source tablet.
		si := scw.sourceShards[0]
		tablets := discovery.RemoveUnhealthyTablets(scw.tsc.GetTabletStats(si.Keyspace(), si.ShardName(), topodatapb.TabletType_RDONLY))
		if len(tablets) == 0 {
			// We fail fast on this problem and don't retry because at the start all tablets should be healthy.
			return fmt.Errorf("no healthy RDONLY tablet in source shard (%v) available (required to find out the schema)", topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName()))
		}
		firstSourceTablet = tablets[0].Tablet
	}
	var statsCounters []*stats.Counters
	var tableStatusList *tableStatusList
	switch state {
	case WorkerStateCloneOnline:
		statsCounters = []*stats.Counters{statsOnlineInsertsCounters, statsOnlineUpdatesCounters, statsOnlineDeletesCounters}
		tableStatusList = scw.tableStatusListOnline
	case WorkerStateCloneOffline:
		statsCounters = []*stats.Counters{statsOfflineInsertsCounters, statsOfflineUpdatesCounters, statsOfflineDeletesCounters}
		tableStatusList = scw.tableStatusListOffline
	}

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

	// In parallel, setup the channels to send SQL data chunks to for each destination tablet:
	//
	// mu protects the context for cancelation, and firstError
	mu := sync.Mutex{}
	var firstError error

	ctx, cancelCopy := context.WithCancel(ctx)
	processError := func(format string, args ...interface{}) {
		scw.wr.Logger().Errorf(format, args...)
		mu.Lock()
		if firstError == nil {
			firstError = fmt.Errorf(format, args...)
			cancelCopy()
		}
		mu.Unlock()
	}

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
			go func(keyspace, shard string, insertChannel chan string, throttler *throttler.Throttler, threadID int) {
				defer destinationWaitGroup.Done()
				defer throttler.ThreadFinished(threadID)

				executor := newExecutor(scw.wr, scw.tsc, throttler, keyspace, shard, threadID)
				if err := executor.fetchLoop(ctx, insertChannel); err != nil {
					processError("executer.FetchLoop failed: %v", err)
				}
			}(si.Keyspace(), si.ShardName(), insertChannels[shardIndex], scw.getThrottler(si.Keyspace(), si.ShardName()), j)
		}
	}

	// Now for each table, read data chunks and send them to all
	// insertChannels
	sourceWaitGroup := sync.WaitGroup{}
	sema := sync2.NewSemaphore(scw.sourceReaderCount, 0)
	for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
		td = reorderColumnsPrimaryKeyFirst(td)

		keyResolver, err := scw.createKeyResolver(td)
		if err != nil {
			return fmt.Errorf("cannot resolve sharding keys for keyspace %v: %v", scw.destinationKeyspace, err)
		}

		// TODO(mberlin): We're going to chunk *all* source shards based on the MIN
		// and MAX values of the *first* source shard. Is this going to be a problem?
		chunks, err := generateChunks(ctx, scw.wr, firstSourceTablet, td, scw.chunkCount, scw.minRowsPerChunk)
		if err != nil {
			return err
		}
		tableStatusList.setThreadCount(tableIndex, len(chunks))

		for _, c := range chunks {
			sourceWaitGroup.Add(1)
			go func(td *tabletmanagerdatapb.TableDefinition, tableIndex int, chunk chunk) {
				defer sourceWaitGroup.Done()

				// We need our own error per Go routine to avoid races.
				var err error

				sema.Acquire()
				defer sema.Release()

				tableStatusList.threadStarted(tableIndex)

				if state == WorkerStateCloneOnline {
					// Wait for enough healthy tablets (they might have become unhealthy
					// and their replication lag might have increased since we started.)
					if err := scw.waitForTablets(ctx, scw.sourceShards, *retryDuration); err != nil {
						processError("table=%v chunk=%v: No healthy source tablets found (gave up after %v): ", td.Name, chunk, *retryDuration, err)
						return
					}
				}

				// Set up readers for the diff. There will be one reader for every
				// source and destination shard.
				sourceReaders := make([]ResultReader, len(scw.sourceShards))
				destReaders := make([]ResultReader, len(scw.destinationShards))
				for shardIndex, si := range scw.sourceShards {
					var sourceAlias *topodatapb.TabletAlias
					if state == WorkerStateCloneOffline {
						// Use the source tablet which we took offline for this phase.
						sourceAlias = scw.offlineSourceAliases[shardIndex]
					} else {
						// Pick any healthy serving source tablet.
						tablets := discovery.RemoveUnhealthyTablets(scw.tsc.GetTabletStats(si.Keyspace(), si.ShardName(), topodatapb.TabletType_RDONLY))
						if len(tablets) == 0 {
							processError("no healthy RDONLY tablets in source shard (%v) available", topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName()))
							return
						}
						sourceAlias = scw.tabletTracker.Track(tablets)
						defer scw.tabletTracker.Untrack(sourceAlias)
					}

					sourceResultReader, err := NewRestartableResultReader(ctx, scw.wr.Logger(), scw.wr.TopoServer(), sourceAlias, td, chunk)
					if err != nil {
						processError("NewRestartableResultReader for source tablet: %v failed: %v", sourceAlias, err)
						return
					}
					defer sourceResultReader.Close()
					sourceReaders[shardIndex] = sourceResultReader
				}
				// Wait for enough healthy tablets (they might have become unhealthy
				// and their replication lag might have increased due to a previous
				// chunk pipeline.)
				if err := scw.waitForTablets(ctx, scw.destinationShards, *retryDuration); err != nil {
					processError("table=%v chunk=%v: No healthy destination tablets found (gave up after %v): ", td.Name, chunk, *retryDuration, err)
					return
				}
				for shardIndex, si := range scw.destinationShards {
					// Pick any healthy serving destination tablet.
					tablets := discovery.RemoveUnhealthyTablets(scw.tsc.GetTabletStats(si.Keyspace(), si.ShardName(), topodatapb.TabletType_RDONLY))
					if len(tablets) == 0 {
						processError("no healthy RDONLY tablets in destination shard (%v) available", topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName()))
						return
					}
					destAlias := scw.tabletTracker.Track(tablets)
					defer scw.tabletTracker.Untrack(destAlias)

					destResultReader, err := NewRestartableResultReader(ctx, scw.wr.Logger(), scw.wr.TopoServer(), destAlias, td, chunk)
					if err != nil {
						processError("NewQueryResultReaderForTablet for destination tablet: %v failed: %v", destAlias, err)
						return
					}
					defer destResultReader.Close()
					destReaders[shardIndex] = destResultReader
				}

				var sourceReader ResultReader
				var destReader ResultReader
				if len(sourceReaders) >= 2 {
					sourceReader, err = NewResultMerger(sourceReaders, len(td.PrimaryKeyColumns))
					if err != nil {
						processError("NewResultMerger for table: %v for source tablets failed: %v", td.Name, err)
						return
					}
				} else {
					sourceReader = sourceReaders[0]
				}
				if len(destReaders) >= 2 {
					destReader, err = NewResultMerger(destReaders, len(td.PrimaryKeyColumns))
					if err != nil {
						processError("NewResultMerger for table: %v for destination tablets failed: %v", td.Name, err)
						return
					}
				} else {
					destReader = destReaders[0]
				}

				dbNames := make([]string, len(scw.destinationShards))
				for i, si := range scw.destinationShards {
					keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
					dbNames[i] = scw.destinationDbNames[keyspaceAndShard]
				}
				// Compare the data and reconcile any differences.
				differ, err := NewRowDiffer2(ctx, sourceReader, destReader, td, tableStatusList, tableIndex,
					scw.destinationShards, keyResolver,
					insertChannels, ctx.Done(), dbNames, scw.writeQueryMaxRows, scw.writeQueryMaxSize, scw.writeQueryMaxRowsDelete, statsCounters)
				if err != nil {
					processError("NewRowDiffer2 failed: %v", err)
					return
				}
				// Ignore the diff report because all diffs should get reconciled.
				_ /* DiffReport */, err = differ.Diff()
				if err != nil {
					processError("RowDiffer2 failed for table: %v, Error: %v", td.Name, err)
					return
				}

				tableStatusList.threadDone(tableIndex)
			}(td, tableIndex, c)
		}
	}
	sourceWaitGroup.Wait()

	for shardIndex := range scw.destinationShards {
		close(insertChannels[shardIndex])
	}
	destinationWaitGroup.Wait()
	if firstError != nil {
		return firstError
	}

	if state == WorkerStateCloneOffline {
		// Create and populate the blp_checkpoint table to give filtered replication
		// a starting point.
		if scw.strategy.skipPopulateBlpCheckpoint {
			scw.wr.Logger().Infof("Skipping populating the blp_checkpoint table")
		} else {
			queries := make([]string, 0, 4)
			queries = append(queries, binlogplayer.CreateBlpCheckpoint()...)
			flags := ""
			if scw.strategy.dontStartBinlogPlayer {
				flags = binlogplayer.BlpFlagDontStart
			}

			// get the current position from the sources
			for shardIndex := range scw.sourceShards {
				shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
				status, err := scw.wr.TabletManagerClient().SlaveStatus(shortCtx, scw.sourceTablets[shardIndex])
				cancel()
				if err != nil {
					return err
				}

				queries = append(queries, binlogplayer.PopulateBlpCheckpoint(uint32(shardIndex), status.Position, scw.maxTPS, throttler.ReplicationLagModuleDisabled, time.Now().Unix(), flags))
			}

			for _, si := range scw.destinationShards {
				destinationWaitGroup.Add(1)
				go func(keyspace, shard string) {
					defer destinationWaitGroup.Done()
					scw.wr.Logger().Infof("Making and populating blp_checkpoint table")
					keyspaceAndShard := topoproto.KeyspaceShardString(keyspace, shard)
					if err := runSQLCommands(ctx, scw.wr, scw.tsc, keyspace, shard, scw.destinationDbNames[keyspaceAndShard], queries); err != nil {
						processError("blp_checkpoint queries failed: %v", err)
					}
				}(si.Keyspace(), si.ShardName())
			}
			destinationWaitGroup.Wait()
			if firstError != nil {
				return firstError
			}
		}

		// Configure filtered replication by setting the SourceShard info.
		// The master tablets won't enable filtered replication (the binlog player)
		//  until they re-read the topology due to a restart or a reload.
		// TODO(alainjobart) this is a superset, some shards may not
		// overlap, have to deal with this better (for N -> M splits
		// where both N>1 and M>1)
		if scw.strategy.skipSetSourceShards {
			scw.wr.Logger().Infof("Skipping setting SourceShard on destination shards.")
		} else {
			for _, si := range scw.destinationShards {
				scw.wr.Logger().Infof("Setting SourceShard on shard %v/%v (tables: %v)", si.Keyspace(), si.ShardName(), scw.tables)
				shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
				err := scw.wr.SetSourceShards(shortCtx, si.Keyspace(), si.ShardName(), scw.offlineSourceAliases, scw.tables)
				cancel()
				if err != nil {
					return fmt.Errorf("failed to set source shards: %v", err)
				}
			}
		}

		// Force a state refresh (re-read topo) on all destination tablets.
		// The master tablet will end up starting filtered replication at this point.
		//
		// Find all tablets first, then refresh the state on each in parallel.
		err = scw.findRefreshTargets(ctx)
		if err != nil {
			return fmt.Errorf("failed before refreshing state on destination tablets: %v", err)
		}
		for shardIndex := range scw.destinationShards {
			for _, tabletAlias := range scw.refreshAliases[shardIndex] {
				destinationWaitGroup.Add(1)
				go func(ti *topo.TabletInfo) {
					defer destinationWaitGroup.Done()
					scw.wr.Logger().Infof("Refreshing state on tablet %v", ti.AliasString())
					shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
					err := scw.wr.TabletManagerClient().RefreshState(shortCtx, ti.Tablet)
					cancel()
					if err != nil {
						processError("RefreshState failed on tablet %v: %v", ti.AliasString(), err)
					}
				}(scw.refreshTablets[shardIndex][*tabletAlias])
			}
		}
	} // clonePhase == offline

	destinationWaitGroup.Wait()
	return firstError
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
		return nil, fmt.Errorf("cannot get schema from source %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
	}
	if len(sourceSchemaDefinition.TableDefinitions) == 0 {
		return nil, fmt.Errorf("no tables matching the table filter in tablet %v", topoproto.TabletAliasString(tablet.Alias))
	}
	for _, td := range sourceSchemaDefinition.TableDefinitions {
		if len(td.Columns) == 0 {
			return nil, fmt.Errorf("schema for table %v has no columns", td.Name)
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
// It is part of the discovery.HealthCheckStatsListener interface.
func (scw *SplitCloneWorker) StatsUpdate(ts *discovery.TabletStats) {
	scw.tsc.StatsUpdate(ts)

	// Ignore if not REPLICA.
	if ts.Target.TabletType != topodatapb.TabletType_REPLICA {
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
		t, err := throttler.NewThrottler(
			keyspaceAndShard, "transactions", scw.destinationWriterCount, scw.maxTPS, throttler.ReplicationLagModuleDisabled)
		if err != nil {
			return fmt.Errorf("cannot instantiate throttler: %v", err)
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
