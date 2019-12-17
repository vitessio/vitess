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
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/wrangler"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// FindDuplicateIdsWorker finds duplicate ids across shards
type FindDuplicateIdsWorker struct {
	StatusWorker

	wr       *wrangler.Wrangler
	cell     string
	keyspace string

	chunkCount        int
	minRowsPerChunk   int
	sourceReaderCount int
	tabletType        topodatapb.TabletType
	cleaner           *wrangler.Cleaner
	tabletTracker     *TabletTracker

	// populated during WorkerStateInit, read-only after that
	table          string
	shardingColumn string
	token          string
	keyspaceInfo   *topo.KeyspaceInfo
	keyRange       *topodatapb.KeyRange
	shards         []*topo.ShardInfo
	// healthCheck is used for the destination shards to a) find out the current
	// MASTER tablet, b) get the list of healthy RDONLY tablets and c) track the
	// replication lag of all REPLICA tablets.
	// It must be closed at the end of the command.
	healthCheck discovery.LegacyHealthCheck
	tsc         *discovery.LegacyTabletStatsCache

	// shardWatchers contains a TopologyWatcher for each source and destination
	// shard. It updates the list of tablets in the healthcheck if replicas are
	// added/removed.
	// Each watcher must be stopped at the end of the command.
	shardWatchers []*discovery.LegacyTopologyWatcher

	// tableStatusList* holds the status for each table.
	// populated during WorkerStateWorking
	status *backfillStatus
}

func newFindDuplicateIdsWorker(wr *wrangler.Wrangler, cell, keyspace, table, shardingColumn, token string, tabletType topodatapb.TabletType, sourceRange string, chunkCount, minRowsPerChunk, readerCount int) (Worker, error) {
	// Verify user defined flags.
	if chunkCount <= 0 {
		return nil, fmt.Errorf("chunk_count must be > 0: %v", chunkCount)
	}
	if minRowsPerChunk <= 0 {
		return nil, fmt.Errorf("min_rows_per_chunk must be > 0: %v", minRowsPerChunk)
	}
	if readerCount <= 0 {
		return nil, fmt.Errorf("source_reader_count must be > 0: %v", readerCount)
	}
	if tabletType != topodatapb.TabletType_MASTER && tabletType != topodatapb.TabletType_RDONLY && tabletType != topodatapb.TabletType_REPLICA {
		return nil, fmt.Errorf("should use MASTER, RDONLY or REPLICA tablets")
	}

	targetKeyranges, err := key.ParseShardingSpec(sourceRange)
	if err != nil {
		return nil, vterrors.Wrapf(err, "unable to parse source range %v", sourceRange)
	}
	if len(targetKeyranges) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "only a single keyrange is currently supported %v", sourceRange)
	}

	scw := &FindDuplicateIdsWorker{
		StatusWorker:      NewStatusWorker(),
		wr:                wr,
		cell:              cell,
		table:             table,
		token:             token,
		shardingColumn:    shardingColumn,
		keyspace:          keyspace,
		tabletType:        tabletType,
		chunkCount:        chunkCount,
		keyRange:          targetKeyranges[0],
		minRowsPerChunk:   minRowsPerChunk,
		sourceReaderCount: readerCount,
		cleaner:           &wrangler.Cleaner{},
		tabletTracker:     NewTabletTracker(),
		status:            &backfillStatus{},
	}
	return scw, nil
}

func (fdiw *FindDuplicateIdsWorker) formatOnlineSources() string {
	aliases := fdiw.tabletTracker.TabletsInUse()
	if aliases == "" {
		return "no source tablets currently in use"
	}
	return aliases
}

// StatusAsHTML implements the Worker interface
func (fdiw *FindDuplicateIdsWorker) StatusAsHTML() template.HTML {
	state := fdiw.State()

	result := "<b>Working on:</b> " + fdiw.keyspace + "/" + fdiw.table + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateBackfilling:
		result += "<b>Running:</b></br>\n"
		result += "<b>Copying from:</b> " + fdiw.formatOnlineSources() + "</br>\n"
		status, eta := fdiw.status.format()
		result += "<b>ETA:</b> " + eta.String() + "</br>\n"
		result += status + "</br>\n"
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
		result += "</br>\n"
		result += "<b>Result:</b></br>\n"
		status, _ := fdiw.status.format()
		result += status + "</br>\n"
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface
func (fdiw *FindDuplicateIdsWorker) StatusAsText() string {
	state := fdiw.State()

	result := "Working on: " + fdiw.keyspace + "/" + fdiw.table + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateBackfilling:
		result += "Running:\n"
		result += "Running using: " + fdiw.formatOnlineSources() + "\n"
		status, eta := fdiw.status.format()
		result += "ETA: " + eta.String() + "\n"
		result += status + "\n"
	case WorkerStateDone:
		result += "Success:"
		result += "\n"
		result += "\n"
		result += "Result:\n"
		status, _ := fdiw.status.format()
		result += status + "\n"
	}
	return result
}

// Run implements the Worker interface
func (fdiw *FindDuplicateIdsWorker) Run(ctx context.Context) error {
	resetVars()

	// Run the command.
	err := fdiw.run(ctx)

	// Cleanup.
	fdiw.SetState(WorkerStateCleanUp)
	// Reverse any changes e.g. setting the tablet type of a source RDONLY tablet.
	cerr := fdiw.cleaner.CleanUp(fdiw.wr)
	if cerr != nil {
		if err != nil {
			fdiw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}

	// Stop watchers to prevent new tablets from getting added to the healthCheck.
	for _, watcher := range fdiw.shardWatchers {
		watcher.Stop()
	}
	// Stop healthCheck to make sure it stops calling our listener implementation.
	if fdiw.healthCheck != nil {
		// After Close returned, we can be sure that it won't call our listener
		// implementation (method StatsUpdate) anymore.
		if err := fdiw.healthCheck.Close(); err != nil {
			fdiw.wr.Logger().Errorf("HealthCheck.Close() failed: %v", err)
		}
	}

	if err != nil {
		fdiw.wr.Logger().Errorf("Error running: %v", err)
		fdiw.SetState(WorkerStateError)
		return err
	}
	fdiw.SetState(WorkerStateDone)
	return nil
}

func (fdiw *FindDuplicateIdsWorker) run(ctx context.Context) error {
	// Phase 1: read what we need to do.
	if err := fdiw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	err := fdiw.findTargets(ctx)
	if err != nil {
		return err
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// Phase 2: find dupes
	fdiw.wr.Logger().Infof("Finding duplicate ids now.")

	start := time.Now()
	if err := fdiw.findDuplicateIds(ctx); err != nil {
		return fmt.Errorf("findDuplicateIds() failed: %v", err)
	}
	d := time.Since(start)
	if err := checkDone(ctx); err != nil {
		return err
	}
	fdiw.wr.Logger().Infof("Finished after %v.", time.Duration(d.Nanoseconds()/time.Second.Nanoseconds()*time.Second.Nanoseconds()))

	return nil
}

func (fdiw *FindDuplicateIdsWorker) findTargets(ctx context.Context) error {
	fdiw.SetState(WorkerStateFindTargets)

	if err := fdiw.waitForTablets(ctx, fdiw.shards, fdiw.tabletType, *retryDuration); err != nil {
		return err
	}

	return nil
}

// init phase:
// - read the destination keyspace, make sure it has 'servedFrom' values
func (fdiw *FindDuplicateIdsWorker) init(ctx context.Context) error {
	fdiw.SetState(WorkerStateInit)

	// read the source keyspace and validate it
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	var err error
	fdiw.keyspaceInfo, err = fdiw.wr.TopoServer().GetKeyspace(shortCtx, fdiw.keyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read (destination) keyspace %v: %v", fdiw.keyspace, err)
	}

	// Set source shard infos.
	if err := fdiw.initShards(ctx); err != nil {
		return fmt.Errorf("failed initDestinationShards: %s", err)
	}

	if err := fdiw.sanityCheckShardInfos(); err != nil {
		return fmt.Errorf("failed sanityCheckShardInfos: %s", err)
	}

	// Initialize healthcheck and add destination shards to it.
	fdiw.healthCheck = discovery.NewLegacyHealthCheck(*healthcheckRetryDelay, *healthCheckTimeout)
	fdiw.tsc = discovery.NewTabletStatsCacheDoNotSetListener(fdiw.wr.TopoServer(), fdiw.cell)
	// We set sendDownEvents=true because it's required by TabletStatsCache.
	fdiw.healthCheck.SetListener(fdiw, true /* sendDownEvents */)

	// Start watchers to get tablets added automatically to healthCheck.
	allShards := fdiw.shards
	for _, si := range allShards {
		watcher := discovery.NewLegacyShardReplicationWatcher(ctx, fdiw.wr.TopoServer(), fdiw.healthCheck,
			fdiw.cell, si.Keyspace(), si.ShardName(),
			*healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
		fdiw.shardWatchers = append(fdiw.shardWatchers, watcher)
	}

	return nil
}

func (fdiw *FindDuplicateIdsWorker) initShards(ctx context.Context) error {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceShards, err := fdiw.wr.TopoServer().FindAllShardsInKeyspace(shortCtx, fdiw.keyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot FindAllShardsInKeyspace in %v: %v", fdiw.keyspace, err)
	}

	srvKeyspace, err := fdiw.wr.TopoServer().GetSrvKeyspace(ctx, fdiw.cell, fdiw.keyspace)
	if err != nil {
		return fmt.Errorf("cannot GetSrvKeyspace for %v: %v", fdiw.keyspace, err)
	}

	fdiw.shards = make([]*topo.ShardInfo, 0, len(sourceShards))
	for _, shard := range sourceShards {
		if !key.KeyRangeIncludes(fdiw.keyRange, shard.KeyRange) {
			continue
		}

		for _, partition := range srvKeyspace.Partitions {
			if partition.GetServedType() != fdiw.tabletType {
				continue
			}
			for _, group := range partition.GetShardReferences() {
				if group.KeyRange.String() == shard.KeyRange.String() {
					fdiw.wr.Logger().Infof("Using shard %v as source for %v/%v", shard.ShardName(), fdiw.cell, fdiw.keyspace)
					fdiw.shards = append(fdiw.shards, shard)
					break
				}
			}
			break
		}
	}

	if len(fdiw.shards) == 0 {
		return fmt.Errorf("no valid shards found for %v", fdiw.keyspace)
	}

	if !fdiw.validateContiguousKeyspace(fdiw.keyRange.Start, fdiw.keyRange.End) {
		return fmt.Errorf("source shards do not cover the full keyspace")
	}

	return nil
}

/**
 * validateContiguousKeyspace verifies that the set of shards in blw.shards begins at *start* and ends at *finish*,
 * and the shards cover the entire keyspace between those two spots. Empty arrays for either value represent the start
 * or end of the keyspace as a whole.
 */
func (fdiw *FindDuplicateIdsWorker) validateContiguousKeyspace(start, finish []byte) bool {
	shard := fdiw.findStart(start)
	count := 0
	for shard != nil {
		count++
		if byteArraysMatch(shard.KeyRange.End, finish) {
			return count == len(fdiw.shards)
		}
		shard = fdiw.findStart(shard.KeyRange.End)
	}
	return false
}

func (fdiw *FindDuplicateIdsWorker) findStart(position []byte) *topo.ShardInfo {
	for _, shard := range fdiw.shards {
		if byteArraysMatch(shard.KeyRange.Start, position) {
			return shard
		}
	}
	return nil
}

func (fdiw *FindDuplicateIdsWorker) sanityCheckShardInfos() error {
	// Don't know if we need to check anything here
	return nil
}

// waitForTablets waits for enough serving tablets in the given
// shard (which can be used as input during the diff).
func (fdiw *FindDuplicateIdsWorker) waitForTablets(ctx context.Context, shardInfos []*topo.ShardInfo, tabletType topodatapb.TabletType, timeout time.Duration) error {
	var wg sync.WaitGroup
	rec := concurrency.AllErrorRecorder{}

	for _, si := range shardInfos {
		wg.Add(1)
		go func(keyspace, shard string) {
			defer wg.Done()
			if _, err := waitForHealthyTablets(ctx, fdiw.wr, fdiw.tsc, fdiw.cell, keyspace, shard, 1, timeout, tabletType); err != nil {
				rec.RecordError(err)
			}
		}(si.Keyspace(), si.ShardName())
	}
	wg.Wait()
	return rec.Error()
}

func (fdiw *FindDuplicateIdsWorker) findDuplicateIds(ctx context.Context) error {
	fdiw.SetState(WorkerStateBackfilling)
	start := time.Now()
	defer statsStateDurationsNs.Set(string(WorkerStateBackfilling), time.Since(start).Nanoseconds())

	status := fdiw.status

	firstSourceTablet, sourceTableDefinition, err := fdiw.getTableDefinition(ctx, fdiw.shards[0], fdiw.table)
	if err != nil {
		return err
	}

	// We only need the from and to columns and the primary key columns
	if len(sourceTableDefinition.PrimaryKeyColumns) != 1 {
		return fmt.Errorf("can only handle a single PK column of type long currently")
	}
	sourceTableDefinition = proto.Clone(sourceTableDefinition).(*tabletmanagerdatapb.TableDefinition)
	sourceTableDefinition.Columns = append(sourceTableDefinition.PrimaryKeyColumns, fdiw.shardingColumn)
	if fdiw.token != "" {
		sourceTableDefinition.Columns = append(sourceTableDefinition.Columns, fdiw.token)
	}

	status.initialize(sourceTableDefinition)

	// mu protects the context for cancelation, and firstError
	mu := sync.Mutex{}
	var firstError error
	foundDupes := 0

	ctx, cancelCopy := context.WithCancel(ctx)
	defer cancelCopy()
	processError := func(format string, args ...interface{}) {
		mu.Lock()
		if firstError == nil {
			fdiw.wr.Logger().Errorf(format, args...)
			firstError = fmt.Errorf(format, args...)
			cancelCopy()
		}
		mu.Unlock()
	}

	// Now for each table, read data chunks
	sourceWaitGroup := sync.WaitGroup{}
	sema := sync2.NewSemaphore(fdiw.sourceReaderCount, 0)

	// TODO(mberlin): We're going to chunk *all* source shards based on the MIN
	// and MAX values of the *first* source shard. Is this going to be a problem?
	chunks, err := generateChunks(ctx, fdiw.wr, firstSourceTablet, sourceTableDefinition, fdiw.chunkCount, fdiw.minRowsPerChunk)
	if err != nil {
		processError("failed to split table into chunks: %v", err)
		return err
	}
	status.setThreadCount(len(chunks))

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

			status.threadStarted()
			defer status.threadDone()

			// Wait for enough healthy tablets (they might have become unhealthy
			// and their replication lag might have increased since we started.)
			if err = fdiw.waitForTablets(ctx, fdiw.shards, topodatapb.TabletType_RDONLY, *retryDuration); err != nil {
				processError("%v: No healthy source tablets found (gave up after %v): %v", errPrefix, time.Since(start), err)
				return
			}

			// Set up readers. There will be one reader for every shard.
			sourceReaders := make(map[string]ResultReader)
			for _, si := range fdiw.shards {
				tp := newShardTabletProvider(fdiw.tsc, fdiw.tabletTracker, si.Keyspace(), si.ShardName(), fdiw.tabletType)
				sourceResultReader, err := NewRestartableResultReader(ctx, fdiw.wr.Logger(), tp, sourceTableDefinition, chunk, true)
				if err != nil {
					processError("%v: NewRestartableResultReader for source: %v failed: %v", errPrefix, tp.description(), err)
					return
				}
				defer sourceResultReader.Close(ctx)
				sourceReaders[tp.description()] = sourceResultReader
			}

			shardsGroupedById := make(map[uint64][]string)
			customerIdsGroupedById := make(map[uint64][]uint64)
			tokensGroupedById := make(map[uint64][]string)
			for shard, sourceReader := range sourceReaders {
				rowReader := NewRowReader(sourceReader)
				for {
					row, err := rowReader.Next()
					status.addCopiedRows(1)
					if err != nil {
						processError("%v: RowReader.Next() failed: %v", errPrefix, err)
						return
					}
					if row == nil {
						break
					}
					id, err := row[0].ToUint64()
					if err != nil {
						processError("%v: sqltypes.ToUint64 failed: %v", errPrefix, err)
						return
					}
					customerId, _ := row[1].ToUint64()
					shardsGroupedById[id] = append(shardsGroupedById[id], shard)
					customerIdsGroupedById[id] = append(customerIdsGroupedById[id], customerId)

					if fdiw.token != "" {
						token := row[2].String()
						tokensGroupedById[id] = append(tokensGroupedById[id], token)
					}
				}
			}

			localFoundDupes := 0
			for id, shards := range shardsGroupedById {
				tokens := tokensGroupedById[id]
				if len(shards) > 1 {
					if fdiw.token != "" && countUnique(tokens) <= 1 {
						continue
					}
					localFoundDupes++
					customerIds := customerIdsGroupedById[id]
					fdiw.wr.Logger().Errorf("duplicate id found: "+
						"table=%v, id=%v, shards=%v, customer_ids=%v, tokens=%v",
						fdiw.table, id, shards, customerIds, tokens)
				}
			}
			if localFoundDupes > 0 {
				fdiw.wr.Logger().Errorf("dupes in chunk: %v", localFoundDupes)
			}
			mu.Lock()
			foundDupes += localFoundDupes
			mu.Unlock()
		}(c)
	}
	sourceWaitGroup.Wait()

	if firstError != nil {
		return firstError
	}

	fdiw.wr.Logger().Errorf("total duplicate ids table=%v: %v", fdiw.table, foundDupes)

	return firstError
}

func countUnique(tokens []string) int {
	set := make(map[string]bool)
	for _, token := range tokens {
		set[token] = true
	}
	return len(set)
}

func (fdiw *FindDuplicateIdsWorker) getSchema(ctx context.Context, tablet *topodatapb.Tablet) (*tabletmanagerdatapb.SchemaDefinition, error) {
	// get source schema from the first shard
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	schemaDefinition, err := fdiw.wr.GetSchema(shortCtx, tablet.Alias, []string{}, []string{}, false /* includeViews */)
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

func (fdiw *FindDuplicateIdsWorker) getTableDefinition(ctx context.Context, si *topo.ShardInfo, tableName string) (*topodatapb.Tablet, *tabletmanagerdatapb.TableDefinition, error) {
	// Pick any healthy serving source tablet.
	tablets := fdiw.tsc.GetHealthyTabletStats(si.Keyspace(), si.ShardName(), fdiw.tabletType)
	if len(tablets) == 0 {
		// We fail fast on this problem and don't retry because at the start all tablets should be healthy.
		return nil, nil, fmt.Errorf("no healthy %s tablet in source shard (%v) available", topodatapb.TabletType_name[int32(fdiw.tabletType)], topoproto.KeyspaceShardString(si.Keyspace(), si.ShardName()))
	}
	firstTablet := tablets[0].Tablet

	schemaDefinition, err := fdiw.getSchema(ctx, firstTablet)
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
func (fdiw *FindDuplicateIdsWorker) StatsUpdate(ts *discovery.LegacyTabletStats) {
	fdiw.tsc.StatsUpdate(ts)

	// Ignore unless REPLICA or RDONLY.
	if ts.Target.TabletType != topodatapb.TabletType_REPLICA && ts.Target.TabletType != topodatapb.TabletType_RDONLY {
		return
	}
}
