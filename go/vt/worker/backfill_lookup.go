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

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
	"vitess.io/vitess/go/vt/wrangler"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// BackfillLookupWorker will backfill the data within a keyspace from a
// source set of shards to a destination set of shards.
type BackfillLookupWorker struct {
	StatusWorker

	wr             *wrangler.Wrangler
	cell           string
	sourceKeyspace string
	vindexName     string

	chunkCount             int
	minRowsPerChunk        int
	sourceReaderCount      int
	writeQueryMaxRows      int
	writeQueryMaxSize      int
	destinationWriterCount int
	tabletType             topodatapb.TabletType
	maxTPS                 int64
	maxReplicationLag      int64
	skipNullRows           bool
	cleaner                *wrangler.Cleaner
	tabletTracker          *TabletTracker

	// populated during WorkerStateInit, read-only after that
	vindex                    *vindexes.Vindex
	backfillableVindex        vindexes.Backfillable
	destinationKeyspace       string
	destinationTable          string
	sourceTable               string
	sourceFromColumn          string
	fromColumn                string
	toColumn                  string
	primaryVindexColumn       string
	sourceKeyspaceInfo        *topo.KeyspaceInfo
	destinationKeyspaceInfo   *topo.KeyspaceInfo
	sourceKeyRange            *topodatapb.KeyRange
	sourceShards              []*topo.ShardInfo
	destinationShards         []*topo.ShardInfo
	sequenceShards            []*topo.ShardInfo
	sourceKeyspaceSchema      *vindexes.KeyspaceSchema
	destinationKeyspaceSchema *vindexes.KeyspaceSchema
	destinationAutoIncrement  *vschema.AutoIncrement
	// healthCheck is used for the destination shards to a) find out the current
	// MASTER tablet, b) get the list of healthy RDONLY tablets and c) track the
	// replication lag of all REPLICA tablets.
	// It must be closed at the end of the command.
	healthCheck discovery.HealthCheck
	tsc         *discovery.TabletStatsCache

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
	// Throttlers will be added/removed during WorkerStateBackfilling.
	throttlers map[string]*throttler.Throttler

	// tableStatusList* holds the status for each table.
	// populated during WorkerStateBackfilling
	backfillStatus *backfillStatus
}

// newBackfillLookupWorker returns a new BackfillLookupWorker object which is used both by
// the BackfillLookup and VerticalBackfillLookup command.
func newBackfillLookupWorker(wr *wrangler.Wrangler, cell, keyspace, vindexName string, tabletType topodatapb.TabletType, sourceRange string, chunkCount, minRowsPerChunk, sourceReaderCount, writeQueryMaxRows, writeQueryMaxSize, destinationWriterCount int, maxTPS, maxReplicationLag int64, skipNullRows bool) (Worker, error) {
	// Verify user defined flags.
	if chunkCount <= 0 {
		return nil, fmt.Errorf("chunk_count must be > 0: %v", chunkCount)
	}
	if minRowsPerChunk <= 0 {
		return nil, fmt.Errorf("min_rows_per_chunk must be > 0: %v", minRowsPerChunk)
	}
	if sourceReaderCount <= 0 {
		return nil, fmt.Errorf("source_reader_count must be > 0: %v", sourceReaderCount)
	}
	if writeQueryMaxRows <= 0 {
		return nil, fmt.Errorf("write_query_max_rows must be > 0: %v", writeQueryMaxRows)
	}
	if writeQueryMaxSize <= 0 {
		return nil, fmt.Errorf("write_query_max_size must be > 0: %v", writeQueryMaxSize)
	}
	if destinationWriterCount <= 0 {
		return nil, fmt.Errorf("destination_writer_count must be > 0: %v", destinationWriterCount)
	}
	if maxTPS != throttler.MaxRateModuleDisabled {
		wr.Logger().Infof("throttling enabled and set to a max of %v transactions/second", maxTPS)
	}
	if maxTPS != throttler.MaxRateModuleDisabled && maxTPS < int64(destinationWriterCount) {
		return nil, fmt.Errorf("-max_tps must be >= -destination_writer_count: %v >= %v", maxTPS, destinationWriterCount)
	}
	if maxReplicationLag <= 0 {
		return nil, fmt.Errorf("max_replication_lag must be >= 1s: %v", maxReplicationLag)
	}
	if tabletType != topodatapb.TabletType_MASTER && tabletType != topodatapb.TabletType_RDONLY && tabletType != topodatapb.TabletType_REPLICA {
		return nil, fmt.Errorf("should use MASTER, RDONLY or REPLICA tablets")
	}

	targetKeyranges, err := key.ParseShardingSpec(sourceRange)
	if err != nil {
		return nil, vterrors.Wrapf(err, "unable to parse source range %v: %v", sourceRange)
	}
	if len(targetKeyranges) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "only a single keyrange is currently supported %v", sourceRange)
	}

	scw := &BackfillLookupWorker{
		StatusWorker:           NewStatusWorker(),
		wr:                     wr,
		cell:                   cell,
		sourceKeyspace:         keyspace,
		vindexName:             vindexName,
		tabletType:             tabletType,
		chunkCount:             chunkCount,
		sourceKeyRange:         targetKeyranges[0],
		minRowsPerChunk:        minRowsPerChunk,
		sourceReaderCount:      sourceReaderCount,
		writeQueryMaxRows:      writeQueryMaxRows,
		writeQueryMaxSize:      writeQueryMaxSize,
		destinationWriterCount: destinationWriterCount,
		maxTPS:                 maxTPS,
		maxReplicationLag:      maxReplicationLag,
		skipNullRows:           skipNullRows,
		cleaner:                &wrangler.Cleaner{},
		tabletTracker:          NewTabletTracker(),
		throttlers:             make(map[string]*throttler.Throttler),

		destinationDbNames: make(map[string]string),

		backfillStatus: &backfillStatus{},
	}
	return scw, nil
}

func (blw *BackfillLookupWorker) formatOnlineSources() string {
	aliases := blw.tabletTracker.TabletsInUse()
	if aliases == "" {
		return "no source tablets currently in use"
	}
	return aliases
}

// StatusAsHTML implements the Worker interface
func (blw *BackfillLookupWorker) StatusAsHTML() template.HTML {
	state := blw.State()

	result := "<b>Working on:</b> " + blw.sourceKeyspace + "/" + blw.vindexName + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateBackfilling:
		result += "<b>Running:</b></br>\n"
		result += "<b>Copying from:</b> " + blw.formatOnlineSources() + "</br>\n"
		status, eta := blw.backfillStatus.format()
		result += "<b>ETA:</b> " + eta.String() + "</br>\n"
		result += status + "</br>\n"
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
		result += "</br>\n"
		result += "<b>Backfill Result:</b></br>\n"
		status, _ := blw.backfillStatus.format()
		result += status + "</br>\n"
	}

	if state == WorkerStateBackfilling && (blw.maxTPS != throttler.MaxRateModuleDisabled || blw.maxReplicationLag != throttler.ReplicationLagModuleDisabled) {
		result += "</br>\n"
		result += `<b>Resharding Throttler:</b> <a href="/throttlerz">see /throttlerz for details</a></br>`
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface
func (blw *BackfillLookupWorker) StatusAsText() string {
	state := blw.State()

	result := "Working on: " + blw.sourceKeyspace + "/" + blw.vindexName + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateBackfilling:
		result += "Running:\n"
		result += "Backfilling using: " + blw.formatOnlineSources() + "\n"
		status, eta := blw.backfillStatus.format()
		result += "ETA: " + eta.String() + "\n"
		result += status + "\n"
	case WorkerStateDone:
		result += "Success:"
		result += "\n"
		result += "\n"
		result += "Result:\n"
		status, _ := blw.backfillStatus.format()
		result += status + "\n"
	}
	return result
}

// Run implements the Worker interface
func (blw *BackfillLookupWorker) Run(ctx context.Context) error {
	resetVars()

	// Run the command.
	err := blw.run(ctx)

	// Cleanup.
	blw.SetState(WorkerStateCleanUp)
	// Reverse any changes e.g. setting the tablet type of a source RDONLY tablet.
	cerr := blw.cleaner.CleanUp(blw.wr)
	if cerr != nil {
		if err != nil {
			blw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}

	// Stop watchers to prevent new tablets from getting added to the healthCheck.
	for _, watcher := range blw.shardWatchers {
		watcher.Stop()
	}
	// Stop healthCheck to make sure it stops calling our listener implementation.
	if blw.healthCheck != nil {
		// After Close returned, we can be sure that it won't call our listener
		// implementation (method StatsUpdate) anymore.
		if err := blw.healthCheck.Close(); err != nil {
			blw.wr.Logger().Errorf("HealthCheck.Close() failed: %v", err)
		}
	}

	if err != nil {
		blw.wr.Logger().Errorf("Error running backfill: %v", err)
		blw.SetState(WorkerStateError)
		return err
	}
	blw.SetState(WorkerStateDone)
	return nil
}

func (blw *BackfillLookupWorker) run(ctx context.Context) error {
	// Phase 1: read what we need to do.
	if err := blw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// Phase 2: Find destination master tablets.
	if err := blw.findDestinationMasters(ctx); err != nil {
		return fmt.Errorf("findDestinationMasters() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// Phase 3: backfill
	blw.wr.Logger().Infof("Backfill will be run now.")

	start := time.Now()
	if err := blw.backfill(ctx); err != nil {
		return fmt.Errorf("backfill() failed: %v", err)
	}
	d := time.Since(start)
	if err := checkDone(ctx); err != nil {
		return err
	}
	// TODO(mberlin): Output diff report of the offline backfill.
	// Round duration to second granularity to make it more readable.
	blw.wr.Logger().Infof("Offline backfill finished after %v.", time.Duration(d.Nanoseconds()/time.Second.Nanoseconds()*time.Second.Nanoseconds()))

	return nil
}

// init phase:
// - read the destination keyspace, make sure it has 'servedFrom' values
func (blw *BackfillLookupWorker) init(ctx context.Context) error {
	blw.SetState(WorkerStateInit)

	// read the source keyspace and validate it
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	var err error
	blw.sourceKeyspaceInfo, err = blw.wr.TopoServer().GetKeyspace(shortCtx, blw.sourceKeyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read (destination) keyspace %v: %v", blw.sourceKeyspace, err)
	}

	if err := blw.loadVindex(ctx); err != nil {
		return fmt.Errorf("failed loadVindex: %s", err)
	}

	// read the destination keyspace and validate it
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	blw.destinationKeyspaceInfo, err = blw.wr.TopoServer().GetKeyspace(shortCtx, blw.destinationKeyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read (destination) keyspace %v: %v", blw.sourceKeyspace, err)
	}

	// Set source and destination shard infos.
	if err := blw.initShards(ctx); err != nil {
		return fmt.Errorf("failed initDestinationShards: %s", err)
	}

	if err := blw.sanityCheckShardInfos(); err != nil {
		return fmt.Errorf("failed sanityCheckShardInfos: %s", err)
	}

	// Initialize healthcheck and add destination shards to it.
	blw.healthCheck = discovery.NewHealthCheck(*healthcheckRetryDelay, *healthCheckTimeout)
	blw.tsc = discovery.NewTabletStatsCacheDoNotSetListener(blw.wr.TopoServer(), blw.cell)
	// We set sendDownEvents=true because it's required by TabletStatsCache.
	blw.healthCheck.SetListener(blw, true /* sendDownEvents */)

	// Start watchers to get tablets added automatically to healthCheck.
	allShards := append(blw.sourceShards, blw.destinationShards...)
	if blw.sequenceShards != nil {
		allShards = append(allShards, blw.sequenceShards...)
	}
	for _, si := range allShards {
		watcher := discovery.NewShardReplicationWatcher(ctx, blw.wr.TopoServer(), blw.healthCheck,
			blw.cell, si.Keyspace(), si.ShardName(),
			*healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
		blw.shardWatchers = append(blw.shardWatchers, watcher)
	}

	return nil
}

func (blw *BackfillLookupWorker) initShards(ctx context.Context) error {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceShards, err := blw.wr.TopoServer().FindAllShardsInKeyspace(shortCtx, blw.sourceKeyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot FindAllShardsInKeyspace in %v: %v", blw.sourceKeyspace, err)
	}

	srvKeyspace, err := blw.wr.TopoServer().GetSrvKeyspace(ctx, blw.cell, blw.sourceKeyspace)
	if err != nil {
		return fmt.Errorf("cannot GetSrvKeyspace for %v: %v", blw.sourceKeyspace, err)
	}

	blw.sourceShards = make([]*topo.ShardInfo, 0, len(sourceShards))
	for _, shard := range sourceShards {
		if !key.KeyRangeIncludes(blw.sourceKeyRange, shard.KeyRange) {
			continue
		}

		for _, partition := range srvKeyspace.Partitions {
			if partition.GetServedType() != blw.tabletType {
				continue
			}
			for _, group := range partition.GetShardReferences() {
				if group.KeyRange.String() == shard.KeyRange.String() {
					blw.wr.Logger().Infof("Using shard %v as source for %v/%v", shard.ShardName(), blw.cell, blw.sourceKeyspace)
					blw.sourceShards = append(blw.sourceShards, shard)
					break
				}
			}
			break
		}
	}

	if len(blw.sourceShards) == 0 {
		return fmt.Errorf("no valid shards found for %v", blw.destinationKeyspace)
	}

	if !blw.validateContiguousKeyspace(blw.sourceKeyRange.Start, blw.sourceKeyRange.End) {
		return fmt.Errorf("source shards do not cover the full keyspace")
	}

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	destinationShards, err := blw.wr.TopoServer().FindAllShardsInKeyspace(shortCtx, blw.destinationKeyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot FindAllShardsInKeyspace in %v: %v", blw.destinationKeyspace, err)
	}

	dstKeyspace, err := blw.wr.TopoServer().GetSrvKeyspace(ctx, blw.cell, blw.destinationKeyspace)
	if err != nil {
		return fmt.Errorf("cannot GetSrvKeyspace for %v: %v", blw.destinationKeyspace, err)
	}

	blw.destinationShards = make([]*topo.ShardInfo, 0, len(destinationShards))
	for _, shard := range destinationShards {
		for _, partition := range dstKeyspace.Partitions {
			if partition.GetServedType() != topodatapb.TabletType_MASTER {
				continue
			}
			for _, group := range partition.GetShardReferences() {
				if group.KeyRange.String() == shard.KeyRange.String() {
					blw.wr.Logger().Infof("Using shard %v as destination for %v/%v", shard.ShardName(), blw.cell, blw.destinationKeyspace)
					blw.destinationShards = append(blw.destinationShards, shard)
					break
				}
			}
			break
		}
	}

	if blw.destinationAutoIncrement != nil {
		keyspaces, err := blw.wr.TopoServer().GetKeyspaces(ctx)
		if err != nil {
			return fmt.Errorf("cannot GetKeyspaces: %v", err)
		}
		for _, ks := range keyspaces {
			kschema, err := blw.wr.TopoServer().GetVSchema(ctx, ks)
			if err != nil {
				return fmt.Errorf("cannot load VSchema for keyspace %v: %v", ks, err)
			}
			for table := range kschema.Tables {
				if table == blw.destinationAutoIncrement.Sequence {
					shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
					shards, err := blw.wr.TopoServer().FindAllShardsInKeyspace(shortCtx, ks)
					cancel()
					if err != nil {
						return fmt.Errorf("cannot FindAllShardsInKeyspace in %v: %v", ks, err)
					}
					// NB: If and when sequences on sharded keyspaces become a thing we'll need to revisit this.
					if len(shards) != 1 {
						continue
					}

					blw.sequenceShards = make([]*topo.ShardInfo, 0, len(shards))
					for _, shard := range shards {
						blw.sequenceShards = append(blw.sequenceShards, shard)
					}
					break
				}
			}
		}
	}
	return nil
}

/**
 * validateContiguousKeyspace verifies that the set of shards in blw.sourceShards begins at *start* and ends at *finish*,
 * and the shards cover the entire keyspace between those two spots. Empty arrays for either value represent the start
 * or end of the keyspace as a whole.
 */
func (blw *BackfillLookupWorker) validateContiguousKeyspace(start, finish []byte) bool {
	shard := blw.findStart(start)
	count := 0
	for shard != nil {
		count++
		if byteArraysMatch(shard.KeyRange.End, finish) {
			return count == len(blw.sourceShards)
		}
		shard = blw.findStart(shard.KeyRange.End)
	}
	return false
}

func (blw *BackfillLookupWorker) findStart(position []byte) *topo.ShardInfo {
	for _, shard := range blw.sourceShards {
		if byteArraysMatch(shard.KeyRange.Start, position) {
			return shard
		}
	}
	return nil
}

func byteArraysMatch(a, b []byte) bool {
	if len(a) == len(b) {
		for i, p := range a {
			if p != b[i] {
				return false
			}
		}
		return true
	}
	return false
}

func (blw *BackfillLookupWorker) sanityCheckShardInfos() error {
	// Don't know if we need to check anything here
	return nil
}

func (blw *BackfillLookupWorker) loadVindex(ctx context.Context) error {
	var keyspaceSchema *vindexes.KeyspaceSchema
	kschema, err := blw.wr.TopoServer().GetVSchema(ctx, blw.sourceKeyspace)
	if err != nil {
		return fmt.Errorf("cannot load VSchema for keyspace %v: %v", blw.sourceKeyspace, err)
	}
	if kschema == nil {
		return fmt.Errorf("no VSchema for keyspace %v", blw.sourceKeyspace)
	}

	vindexDef, ok := kschema.Vindexes[blw.vindexName]
	if !ok {
		return fmt.Errorf("no such vindex %v in keyspace %v", blw.vindexName, blw.sourceKeyspace)
	}

	keyspaceSchema, err = vindexes.BuildKeyspaceSchema(kschema, blw.sourceKeyspace)
	if err != nil {
		return fmt.Errorf("cannot build vschema for keyspace %v: %v", blw.sourceKeyspace, err)
	}
	blw.sourceKeyspaceSchema = keyspaceSchema
	vindex, ok := blw.sourceKeyspaceSchema.Vindexes[blw.vindexName]
	if !ok {
		panic("found vindex in Keyspace proto but not in KeyspaceSchema")
	}

	_, ok = vindex.(vindexes.Lookup)
	if !ok {
		return fmt.Errorf("can currently only backfill Lookup")
	}

	blw.backfillableVindex, ok = vindex.(vindexes.Backfillable)
	if !ok {
		return fmt.Errorf("can currently only backfill Backfillable")
	}

	blw.vindex = &vindex
	blw.sourceTable = vindexDef.Owner

	destinationTable := vindexDef.Params["table"]
	split := strings.Split(destinationTable, ".")
	if len(split) == 1 {
		blw.destinationKeyspace = blw.sourceKeyspace
		blw.destinationTable = split[1]
	} else if len(split) == 2 {
		blw.destinationKeyspace = split[0]
		blw.destinationTable = split[1]
	} else {
		return fmt.Errorf("can't parse destination table: %v", destinationTable)
	}

	sourceTableDef, ok := kschema.Tables[blw.sourceTable]
	defaultSourceVindex := sourceTableDef.ColumnVindexes[0]
	if defaultSourceVindex.Column != "" {
		blw.primaryVindexColumn = defaultSourceVindex.Column
	} else {
		if len(defaultSourceVindex.Columns) > 1 {
			return fmt.Errorf("multiple sharding columns on the source table not implemented yet")
		}
		blw.primaryVindexColumn = defaultSourceVindex.Columns[0]
	}

	for _, v := range sourceTableDef.ColumnVindexes {
		if v.Name == blw.vindexName {
			blw.sourceFromColumn = v.Column
			break
		}
	}

	if blw.sourceFromColumn == "" {
		return fmt.Errorf("cannot find sourceFromColumn for table %v with Vindex %v", blw.sourceTable, blw.vindexName)
	}

	blw.fromColumn = vindexDef.Params["from"]
	blw.toColumn = vindexDef.Params["to"]

	kschema, err = blw.wr.TopoServer().GetVSchema(ctx, blw.destinationKeyspace)
	if err != nil {
		return fmt.Errorf("cannot load VSchema for keyspace %v: %v", blw.destinationKeyspace, err)
	}
	if kschema == nil {
		return fmt.Errorf("no VSchema for keyspace %v", blw.destinationKeyspace)
	}
	blw.destinationAutoIncrement = kschema.Tables[blw.destinationTable].AutoIncrement

	keyspaceSchema, err = vindexes.BuildKeyspaceSchema(kschema, blw.destinationKeyspace)
	if err != nil {
		return fmt.Errorf("cannot build vschema for keyspace %v: %v", blw.destinationKeyspace, err)
	}
	blw.destinationKeyspaceSchema = keyspaceSchema

	return nil
}

// findDestinationMasters finds for each destination shard the current master.
func (blw *BackfillLookupWorker) findDestinationMasters(ctx context.Context) error {
	blw.SetState(WorkerStateFindTargets)

	// Make sure we find a master for each destination shard and log it.
	blw.wr.Logger().Infof("Finding a MASTER tablet for each destination shard...")
	for _, si := range blw.destinationShards {
		waitCtx, waitCancel := context.WithTimeout(ctx, *waitForHealthyTabletsTimeout)
		defer waitCancel()
		if err := blw.tsc.WaitForTablets(waitCtx, blw.cell, si.Keyspace(), si.ShardName(), topodatapb.TabletType_MASTER); err != nil {
			return fmt.Errorf("cannot find MASTER tablet for destination shard for %v/%v (in cell: %v): %v", si.Keyspace(), si.ShardName(), blw.cell, err)
		}
		masters := blw.tsc.GetHealthyTabletStats(si.Keyspace(), si.ShardName(), topodatapb.TabletType_MASTER)
		if len(masters) == 0 {
			return fmt.Errorf("cannot find MASTER tablet for destination shard for %v/%v (in cell: %v) in HealthCheck: empty TabletStats list", si.Keyspace(), si.ShardName(), blw.cell)
		}
		master := masters[0]

		// Get the MySQL database name of the tablet.
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		blw.destinationDbNames[keyspaceAndShard] = topoproto.TabletDbName(master.Tablet)

		blw.wr.Logger().Infof("Using tablet %v as destination master for %v/%v", topoproto.TabletAliasString(master.Tablet.Alias), si.Keyspace(), si.ShardName())
	}
	blw.wr.Logger().Infof("NOTE: The used master of a destination shard might change over the course of the copy e.g. due to a reparent. The HealthCheck module will track and log master changes and any error message will always refer the actually used master address.")

	return nil
}

// waitForTablets waits for enough serving tablets in the given
// shard (which can be used as input during the diff).
func (blw *BackfillLookupWorker) waitForTablets(ctx context.Context, shardInfos []*topo.ShardInfo, tabletType topodatapb.TabletType, timeout time.Duration) error {
	var wg sync.WaitGroup
	rec := concurrency.AllErrorRecorder{}

	for _, si := range shardInfos {
		wg.Add(1)
		go func(keyspace, shard string) {
			defer wg.Done()
			if _, err := waitForHealthyTablets(ctx, blw.wr, blw.tsc, blw.cell, keyspace, shard, 1, timeout, tabletType); err != nil {
				rec.RecordError(err)
			}
		}(si.Keyspace(), si.ShardName())
	}
	wg.Wait()
	return rec.Error()
}

func (blw *BackfillLookupWorker) backfill(ctx context.Context) error {
	blw.SetState(WorkerStateBackfilling)
	start := time.Now()
	defer statsStateDurationsNs.Set(string(WorkerStateBackfilling), time.Now().Sub(start).Nanoseconds())

	backfillStatus := blw.backfillStatus

	firstSourceTablet, sourceTableDefinition, err := blw.getTableDefinition(ctx, blw.sourceShards[0], blw.sourceTable)
	if err != nil {
		return err
	}

	// We only need the from and to columns and the primary key columns
	var sourceColumns []string
	sourceTableDefinition = proto.Clone(sourceTableDefinition).(*tabletmanagerdatapb.TableDefinition)
	sourceTableDefinition.Columns = append(
		append(sourceColumns, sourceTableDefinition.PrimaryKeyColumns...),
		blw.sourceFromColumn,
		blw.primaryVindexColumn)
	fromColumnIndex := len(sourceTableDefinition.PrimaryKeyColumns)
	defaultVindexColumnIndex := fromColumnIndex + 1

	backfillStatus.initialize(sourceTableDefinition)

	_, destinationTableDefinition, err := blw.getTableDefinition(ctx, blw.destinationShards[0], blw.destinationTable)
	if err != nil {
		return err
	}

	// We only produce the from and to columns
	destinationTableDefinition = proto.Clone(destinationTableDefinition).(*tabletmanagerdatapb.TableDefinition)
	if blw.destinationAutoIncrement == nil {
		destinationTableDefinition.Columns = []string{
			blw.fromColumn,
			blw.toColumn,
		}
	} else {
		destinationTableDefinition.Columns = []string{
			blw.destinationAutoIncrement.Column,
			blw.fromColumn,
			blw.toColumn,
		}
	}

	keyResolver, err := newV3ResolverFromTableDefinition(blw.destinationKeyspaceSchema, destinationTableDefinition)
	if err != nil {
		return fmt.Errorf("cannot resolve sharding keys for keyspace %v: %v", blw.destinationKeyspaceSchema, err)
	}

	if err := blw.createThrottlers(); err != nil {
		return err
	}
	defer blw.closeThrottlers()

	// In parallel, setup the channels to send SQL data chunks to for each destination tablet:
	//
	// mu protects the context for cancelation, and firstError
	mu := sync.Mutex{}
	var firstError error

	ctx, cancelCopy := context.WithCancel(ctx)
	defer cancelCopy()
	processError := func(format string, args ...interface{}) {
		mu.Lock()
		if firstError == nil {
			blw.wr.Logger().Errorf(format, args...)
			firstError = fmt.Errorf(format, args...)
			cancelCopy()
		}
		mu.Unlock()
	}

	// NOTE: Code below this point must *not* use "return" to exit this Go routine
	// early. Instead, "processError" must be called to cancel the context. This
	// way all launched Go routines will terminate as well.
	// (However, "return" in a new Go routine, i.e. not this one, is fine.)
	// If Go routines have already been started, make sure that Wait() on the
	// respective WaitGroup is called as well e.g. "destinationWaitGroup.Wait()"
	// must always be reached. Waiting for the Go routines is important to avoid
	// races between "defer throttler.ThreadFinished()" (must be executed first)
	// and "defer blw.closeThrottlers()". Otherwise, vtworker will panic.

	insertChannels := make([]chan string, len(blw.destinationShards))
	destinationWaitGroup := sync.WaitGroup{}
	for shardIndex, si := range blw.destinationShards {
		// We create one channel per destination tablet. It is sized to have a
		// buffer of a maximum of destinationWriterCount * 2 items, to hopefully
		// always have data. We then have destinationWriterCount go routines reading
		// from it.
		insertChannels[shardIndex] = make(chan string, blw.destinationWriterCount*2)

		for j := 0; j < blw.destinationWriterCount; j++ {
			destinationWaitGroup.Add(1)
			go func(keyspace, shard string, insertChannel chan string, throttler *throttler.Throttler, threadID int) {
				defer destinationWaitGroup.Done()
				defer throttler.ThreadFinished(threadID)

				executor := newExecutor(blw.wr, blw.tsc, throttler, keyspace, shard, threadID)
				if err := executor.fetchLoop(ctx, insertChannel); err != nil {
					processError("executer.FetchLoop failed: %v", err)
				}
			}(si.Keyspace(), si.ShardName(), insertChannels[shardIndex], blw.getThrottler(si.Keyspace(), si.ShardName()), j)
		}
	}

	// Now for each table, read data chunks and send them to all
	// insertChannels
	sourceWaitGroup := sync.WaitGroup{}
	sema := sync2.NewSemaphore(blw.sourceReaderCount, 0)

	// TODO(mberlin): We're going to chunk *all* source shards based on the MIN
	// and MAX values of the *first* source shard. Is this going to be a problem?
	chunks, err := generateChunks(ctx, blw.wr, firstSourceTablet, sourceTableDefinition, blw.chunkCount, blw.minRowsPerChunk)
	if err != nil {
		processError("failed to split table into chunks: %v", err)
		return err
	}
	backfillStatus.setThreadCount(len(chunks))

	for _, c := range chunks {
		sourceWaitGroup.Add(1)
		go func(chunk chunk) {
			defer sourceWaitGroup.Done()
			errPrefix := fmt.Sprintf("table=%v chunk=%v", sourceTableDefinition.Name, chunk)

			// We need our own error per Go routine to avoid races.
			var err error

			sema.Acquire()
			defer sema.Release()

			if err := checkDone(ctx); err != nil {
				processError("%v: Context expired while this thread was waiting for its turn. Context error: %v", errPrefix, err)
				return
			}

			backfillStatus.threadStarted()
			defer backfillStatus.threadDone()

			// Wait for enough healthy tablets (they might have become unhealthy
			// and their replication lag might have increased since we started.)
			if err := blw.waitForTablets(ctx, blw.sourceShards, topodatapb.TabletType_MASTER, *retryDuration); err != nil {
				processError("%v: No healthy source tablets found (gave up after %v): %v", errPrefix, time.Since(start), err)
				return
			}

			// Set up readers for the diff. There will be one reader for every
			// source and destination shard.
			sourceReaders := make([]ResultReader, len(blw.sourceShards))
			for shardIndex, si := range blw.sourceShards {
				tp := newShardTabletProvider(blw.tsc, blw.tabletTracker, si.Keyspace(), si.ShardName(), blw.tabletType)
				sourceResultReader, err := NewRestartableResultReader(ctx, blw.wr.Logger(), tp, sourceTableDefinition, chunk, true)
				if err != nil {
					processError("%v: NewRestartableResultReader for source: %v failed: %v", errPrefix, tp.description(), err)
					return
				}
				defer sourceResultReader.Close(ctx)
				sourceReaders[shardIndex] = sourceResultReader
			}

			var sourceReader ResultReader
			if len(sourceReaders) >= 2 {
				sourceReader, err = NewResultMerger(sourceReaders, len(sourceTableDefinition.PrimaryKeyColumns))
				if err != nil {
					processError("%v: NewResultMerger for source tablets failed: %v", errPrefix, err)
					return
				}
			} else {
				sourceReader = sourceReaders[0]
			}

			dbNames := make([]string, len(blw.destinationShards))
			for i, si := range blw.destinationShards {
				keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
				dbNames[i] = blw.destinationDbNames[keyspaceAndShard]
			}

			var seqTablet *topodatapb.Tablet
			var seqQueryService queryservice.QueryService
			var target *querypb.Target
			if blw.destinationAutoIncrement != nil {
				si := blw.sequenceShards[0]
				blw.wr.Logger().Infof("Connecting to MASTER for %v/%v", si.Keyspace(), si.ShardName())
				seqTabletProvider := newShardTabletProvider(blw.tsc, blw.tabletTracker, si.Keyspace(), si.ShardName(), topodatapb.TabletType_MASTER)
				seqTablet, err = seqTabletProvider.getTablet()
				if err != nil {
					processError("%v: Connecting to sequence tablet failed: %v", errPrefix, err)
					return
				}
				seqQueryService, err = tabletconn.GetDialer()(seqTablet, true)
				if err != nil {
					processError("%v: Creating QueryService for sequence tablet failed: %v", errPrefix, err)
					return
				}
				defer seqQueryService.Close(ctx)

				target = &querypb.Target{
					Keyspace:   si.Keyspace(),
					Shard:      si.ShardName(),
					TabletType: topodatapb.TabletType_MASTER,
				}
			}
			transform := func(row []sqltypes.Value) ([]sqltypes.Value, bool, error) {
				fromValue, err := blw.backfillableVindex.FromValue(row[fromColumnIndex])
				if err != nil {
					return nil, false, err
				}

				if blw.destinationAutoIncrement != nil {
					bindVars := map[string]*querypb.BindVariable{"n": sqltypes.ValueBindVariable(sqltypes.NewInt64(1))}
					results, err := seqQueryService.Execute(ctx, target, fmt.Sprintf("select next :n values from %s", blw.destinationAutoIncrement.Sequence), bindVars, 0, nil)
					if err != nil {
						return nil, false, err
					}

					return []sqltypes.Value{
						results.Rows[0][0],
						fromValue,
						row[defaultVindexColumnIndex],
					}, row[fromColumnIndex].IsNull(), nil
				}
				return []sqltypes.Value{
					fromValue,
					row[defaultVindexColumnIndex],
				}, row[fromColumnIndex].IsNull(), nil
			}
			backfiller, err := NewBackfiller(ctx, sourceReader, destinationTableDefinition, backfillStatus, blw.skipNullRows,
				transform,
				blw.destinationShards, keyResolver,
				insertChannels, dbNames, blw.writeQueryMaxRows, blw.writeQueryMaxSize, statsBackfillInsertsCounters)
			if err != nil {
				processError("%v: Backfiller failed: %v", errPrefix, err)
				return
			}
			_, err = backfiller.Backfill()
			if err != nil {
				processError("%v: Backfiller failed: %v", errPrefix, err)
				return
			}
		}(c)
	}
	sourceWaitGroup.Wait()

	for shardIndex := range blw.destinationShards {
		close(insertChannels[shardIndex])
	}
	destinationWaitGroup.Wait()
	if firstError != nil {
		return firstError
	}

	return firstError
}

func (blw *BackfillLookupWorker) getSchema(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.SchemaDefinition, error) {
	// get source schema from the first shard
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	schemaDefinition, err := blw.wr.GetSchema(shortCtx, tablet.Alias, []string{}, []string{}, false /* includeViews */)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("cannot get schema from source %v: %v", topoproto.TabletAliasString(tablet.Alias), err)
	}
	if len(schemaDefinition.TableDefinitions) == 0 {
		return nil, fmt.Errorf("no tables found in tablet %v", topoproto.TabletAliasString(tablet.Alias))
	}
	for _, td := range schemaDefinition.TableDefinitions {
		if len(td.Columns) == 0 {
			return nil, fmt.Errorf("schema for table %v has no columns", td.Name)
		}
	}
	return schemaDefinition, nil
}

func (blw *BackfillLookupWorker) getTableDefinition(ctx context.Context, si *topo.ShardInfo, tableName string) (*topodatapb.Tablet, *tabletmanagerdatapb.TableDefinition, error) {
	// Pick any healthy serving source tablet.
	tablets := blw.tsc.GetHealthyTabletStats(si.Keyspace(), si.ShardName(), blw.tabletType)
	if len(tablets) == 0 {
		// We fail fast on this problem and don't retry because at the start all tablets should be healthy.
		return nil, nil, fmt.Errorf("no healthy %s tablet in source shard (%v) available", topodatapb.TabletType_name[int32(blw.tabletType)], topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName()))
	}
	firstTablet := tablets[0].Tablet

	schemaDefinition, err := blw.getSchema(ctx, firstTablet)
	if err != nil {
		return nil, nil, err
	}

	var tableDefinition *tabletmanagerdatapb.TableDefinition
	for _, td := range schemaDefinition.TableDefinitions {
		if td.Name == tableName {
			tableDefinition = td
			break
		}
	}
	if tableDefinition == nil {
		return nil, nil, fmt.Errorf("could not find table: %v in tablet: %v", tableName, topoproto.TabletAliasString(firstTablet.Alias))
	}

	return firstTablet, tableDefinition, nil
}

// StatsUpdate receives replication lag updates for each destination master
// and forwards them to the respective throttler instance.
// It also forwards any update to the TabletStatsCache to keep it up to date.
// It is part of the discovery.HealthCheckStatsListener interface.
func (blw *BackfillLookupWorker) StatsUpdate(ts *discovery.TabletStats) {
	blw.tsc.StatsUpdate(ts)

	// Ignore unless REPLICA or RDONLY.
	if ts.Target.TabletType != topodatapb.TabletType_REPLICA && ts.Target.TabletType != topodatapb.TabletType_RDONLY {
		return
	}

	// Lock throttlers mutex to avoid that this method (and the called method
	// Throttler.RecordReplicationLag()) races with closeThrottlers() (which calls
	// Throttler.Close()).
	blw.throttlersMu.Lock()
	defer blw.throttlersMu.Unlock()

	t := blw.getThrottlerLocked(ts.Target.Keyspace, ts.Target.Shard)
	if t != nil {
		t.RecordReplicationLag(time.Now(), ts)
	}
}

func (blw *BackfillLookupWorker) createThrottlers() error {
	blw.throttlersMu.Lock()
	defer blw.throttlersMu.Unlock()

	for _, si := range blw.destinationShards {
		// Set up the throttler for each destination shard.
		keyspaceAndShard := topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName())
		t, err := throttler.NewThrottler(keyspaceAndShard, "transactions", blw.destinationWriterCount, blw.maxTPS, blw.maxReplicationLag)
		if err != nil {
			return fmt.Errorf("cannot instantiate throttler: %v", err)
		}
		blw.throttlers[keyspaceAndShard] = t
	}
	return nil
}

func (blw *BackfillLookupWorker) getThrottler(keyspace, shard string) *throttler.Throttler {
	blw.throttlersMu.Lock()
	defer blw.throttlersMu.Unlock()

	return blw.getThrottlerLocked(keyspace, shard)
}

func (blw *BackfillLookupWorker) getThrottlerLocked(keyspace, shard string) *throttler.Throttler {
	keyspaceAndShard := topoproto.KeyspaceShardString(keyspace, shard)
	return blw.throttlers[keyspaceAndShard]
}

func (blw *BackfillLookupWorker) closeThrottlers() {
	blw.throttlersMu.Lock()
	defer blw.throttlersMu.Unlock()

	for keyspaceAndShard, t := range blw.throttlers {
		t.Close()
		delete(blw.throttlers, keyspaceAndShard)
	}
}
