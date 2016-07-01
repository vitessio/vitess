// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"errors"
	"fmt"
	"html/template"
	"io"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"
	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/worker/events"
	"github.com/youtube/vitess/go/vt/wrangler"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// VerticalSplitCloneWorker will clone the data from a source keyspace/shard
// to a destination keyspace/shard.
type VerticalSplitCloneWorker struct {
	StatusWorker

	wr                      *wrangler.Wrangler
	cell                    string
	destinationKeyspace     string
	destinationShard        string
	tables                  []string
	strategy                *splitStrategy
	sourceReaderCount       int
	destinationPackCount    int
	minTableSizeForSplit    uint64
	destinationWriterCount  int
	minHealthyRdonlyTablets int
	maxTPS                  int64
	cleaner                 *wrangler.Cleaner

	// populated during WorkerStateInit, read-only after that
	sourceKeyspace string

	// populated during WorkerStateFindTargets, read-only after that
	sourceAlias  *topodatapb.TabletAlias
	sourceTablet *topodatapb.Tablet
	// healthCheck tracks the health of all MASTER and REPLICA tablets.
	// It must be closed at the end of the command.
	healthCheck discovery.HealthCheck
	// destinationShardWatchers contains a TopologyWatcher for each destination
	// shard. It updates the list of tablets in the healthcheck if replicas are
	// added/removed.
	// Each watcher must be stopped at the end of the command.
	destinationShardWatchers []*discovery.TopologyWatcher
	// destinationDbNames stores for each destination keyspace/shard the MySQL
	// database name.
	// Example Map Entry: test_keyspace/-80 => vt_test_keyspace
	destinationDbNames map[string]string

	// populated during WorkerStateClone
	// tableStatusList holds the status for each table.
	tableStatusList tableStatusList
	// aliases of tablets that need to have their state refreshed.
	// Only populated once, read-only after that.
	refreshAliases []*topodatapb.TabletAlias
	refreshTablets map[topodatapb.TabletAlias]*topo.TabletInfo

	ev *events.VerticalSplitClone
}

// NewVerticalSplitCloneWorker returns a new VerticalSplitCloneWorker object.
func NewVerticalSplitCloneWorker(wr *wrangler.Wrangler, cell, destinationKeyspace, destinationShard string, tables []string, strategyStr string, sourceReaderCount, destinationPackCount int, minTableSizeForSplit uint64, destinationWriterCount, minHealthyRdonlyTablets int, maxTPS int64) (Worker, error) {
	if len(tables) == 0 {
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
	return &VerticalSplitCloneWorker{
		StatusWorker:            NewStatusWorker(),
		wr:                      wr,
		cell:                    cell,
		destinationKeyspace:     destinationKeyspace,
		destinationShard:        destinationShard,
		tables:                  tables,
		strategy:                strategy,
		sourceReaderCount:       sourceReaderCount,
		destinationPackCount:    destinationPackCount,
		minTableSizeForSplit:    minTableSizeForSplit,
		destinationWriterCount:  destinationWriterCount,
		minHealthyRdonlyTablets: minHealthyRdonlyTablets,
		maxTPS:                  maxTPS,
		cleaner:                 &wrangler.Cleaner{},

		destinationDbNames: make(map[string]string),

		ev: &events.VerticalSplitClone{
			Cell:     cell,
			Keyspace: destinationKeyspace,
			Shard:    destinationShard,
			Tables:   tables,
			Strategy: strategy.String(),
		},
	}, nil
}

func (vscw *VerticalSplitCloneWorker) setState(state StatusWorkerState) {
	vscw.SetState(state)
	event.DispatchUpdate(vscw.ev, state.String())
}

func (vscw *VerticalSplitCloneWorker) setErrorState(err error) {
	vscw.SetState(WorkerStateError)
	event.DispatchUpdate(vscw.ev, "error: "+err.Error())
}

// StatusAsHTML implements the Worker interface
func (vscw *VerticalSplitCloneWorker) StatusAsHTML() template.HTML {
	state := vscw.State()

	result := "<b>Working on:</b> " + vscw.destinationKeyspace + "/" + vscw.destinationShard + "</br>\n"
	result += "<b>State:</b> " + state.String() + "</br>\n"
	switch state {
	case WorkerStateCloneOffline:
		result += "<b>Running</b>:</br>\n"
		result += "<b>Copying from</b>: " + topoproto.TabletAliasString(vscw.sourceAlias) + "</br>\n"
		statuses, eta := vscw.tableStatusList.format()
		result += "<b>ETA</b>: " + eta.String() + "</br>\n"
		result += strings.Join(statuses, "</br>\n")
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
		statuses, _ := vscw.tableStatusList.format()
		result += strings.Join(statuses, "</br>\n")
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface
func (vscw *VerticalSplitCloneWorker) StatusAsText() string {
	state := vscw.State()

	result := "Working on: " + vscw.destinationKeyspace + "/" + vscw.destinationShard + "\n"
	result += "State: " + state.String() + "\n"
	switch state {
	case WorkerStateCloneOffline:
		result += "Running:\n"
		result += "Copying from: " + topoproto.TabletAliasString(vscw.sourceAlias) + "\n"
		statuses, eta := vscw.tableStatusList.format()
		result += "ETA: " + eta.String() + "\n"
		result += strings.Join(statuses, "\n")
	case WorkerStateDone:
		result += "Success:\n"
		statuses, _ := vscw.tableStatusList.format()
		result += strings.Join(statuses, "\n")
	}
	return result
}

// Run implements the Worker interface
func (vscw *VerticalSplitCloneWorker) Run(ctx context.Context) error {
	resetVars()

	// Run the command.
	err := vscw.run(ctx)

	// Cleanup.
	vscw.setState(WorkerStateCleanUp)
	// Reverse any changes e.g. setting the tablet type of a source RDONLY tablet.
	cerr := vscw.cleaner.CleanUp(vscw.wr)
	if cerr != nil {
		if err != nil {
			vscw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}

	// Stop healthcheck.
	for _, watcher := range vscw.destinationShardWatchers {
		watcher.Stop()
	}
	if vscw.healthCheck != nil {
		if err := vscw.healthCheck.Close(); err != nil {
			vscw.wr.Logger().Errorf("HealthCheck.Close() failed: %v", err)
		}
	}

	if err != nil {
		vscw.setErrorState(err)
		return err
	}
	vscw.setState(WorkerStateDone)
	return nil
}

func (vscw *VerticalSplitCloneWorker) run(ctx context.Context) error {
	// first state: read what we need to do
	if err := vscw.init(ctx); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// second state: find targets
	if err := vscw.findTargets(ctx); err != nil {
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	// third state: copy data
	if err := vscw.clone(ctx); err != nil {
		return fmt.Errorf("copy() failed: %v", err)
	}
	if err := checkDone(ctx); err != nil {
		return err
	}

	return nil
}

// init phase:
// - read the destination keyspace, make sure it has 'servedFrom' values
func (vscw *VerticalSplitCloneWorker) init(ctx context.Context) error {
	vscw.setState(WorkerStateInit)

	// read the keyspace and validate it
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	destinationKeyspaceInfo, err := vscw.wr.TopoServer().GetKeyspace(shortCtx, vscw.destinationKeyspace)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read destination keyspace %v: %v", vscw.destinationKeyspace, err)
	}
	if len(destinationKeyspaceInfo.ServedFroms) == 0 {
		return fmt.Errorf("destination keyspace %v has no KeyspaceServedFrom", vscw.destinationKeyspace)
	}

	// validate all serving types, find sourceKeyspace
	servingTypes := []topodatapb.TabletType{topodatapb.TabletType_MASTER, topodatapb.TabletType_REPLICA, topodatapb.TabletType_RDONLY}
	servedFrom := ""
	for _, st := range servingTypes {
		sf := destinationKeyspaceInfo.GetServedFrom(st)
		if sf == nil {
			return fmt.Errorf("destination keyspace %v is serving type %v", vscw.destinationKeyspace, st)
		}
		if servedFrom == "" {
			servedFrom = sf.Keyspace
		} else {
			if servedFrom != sf.Keyspace {
				return fmt.Errorf("destination keyspace %v is serving from multiple source keyspaces %v and %v", vscw.destinationKeyspace, servedFrom, sf.Keyspace)
			}
		}
	}
	vscw.sourceKeyspace = servedFrom

	// Verify that filtered replication is not already enabled.
	destShardInfo, err := vscw.wr.TopoServer().GetShard(ctx, vscw.destinationKeyspace, vscw.destinationShard)
	if len(destShardInfo.SourceShards) > 0 {
		return fmt.Errorf("destination shard %v/%v has filtered replication already enabled from a previous resharding (ShardInfo is set)."+
			" This requires manual intervention e.g. use vtctl SourceShardDelete to remove it",
			vscw.destinationKeyspace, vscw.destinationShard)
	}

	return nil
}

// findTargets phase:
// - find one rdonly in the source shard
// - mark it as 'worker' pointing back to us
// - get the aliases of all the targets
func (vscw *VerticalSplitCloneWorker) findTargets(ctx context.Context) error {
	vscw.setState(WorkerStateFindTargets)

	// find an appropriate tablet in the source shard
	var err error
	vscw.sourceAlias, err = FindWorkerTablet(ctx, vscw.wr, vscw.cleaner, nil /* healthCheck */, vscw.cell, vscw.sourceKeyspace, "0", vscw.minHealthyRdonlyTablets)
	if err != nil {
		return fmt.Errorf("FindWorkerTablet() failed for %v/%v/0: %v", vscw.cell, vscw.sourceKeyspace, err)
	}
	vscw.wr.Logger().Infof("Using tablet %v as the source", topoproto.TabletAliasString(vscw.sourceAlias))

	// get the tablet info for it
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	ti, err := vscw.wr.TopoServer().GetTablet(shortCtx, vscw.sourceAlias)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot read tablet %v: %v", topoproto.TabletAliasString(vscw.sourceAlias), err)
	}
	vscw.sourceTablet = ti.Tablet

	// stop replication on it
	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	err = vscw.wr.TabletManagerClient().StopSlave(shortCtx, vscw.sourceTablet)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot stop replication on tablet %v", topoproto.TabletAliasString(vscw.sourceAlias))
	}

	wrangler.RecordStartSlaveAction(vscw.cleaner, vscw.sourceTablet)

	// Initialize healthcheck and add destination shards to it.
	vscw.healthCheck = discovery.NewHealthCheck(*remoteActionsTimeout, *healthcheckRetryDelay, *healthCheckTimeout)
	watcher := discovery.NewShardReplicationWatcher(vscw.wr.TopoServer(), vscw.healthCheck,
		vscw.cell, vscw.destinationKeyspace, vscw.destinationShard,
		*healthCheckTopologyRefresh, discovery.DefaultTopoReadConcurrency)
	vscw.destinationShardWatchers = append(vscw.destinationShardWatchers, watcher)

	// Make sure we find a master for each destination shard and log it.
	vscw.wr.Logger().Infof("Finding a MASTER tablet for each destination shard...")
	waitCtx, waitCancel := context.WithTimeout(ctx, *waitForHealthyTabletsTimeout)
	defer waitCancel()
	if err := discovery.WaitForTablets(waitCtx, vscw.healthCheck,
		vscw.cell, vscw.destinationKeyspace, vscw.destinationShard, []topodatapb.TabletType{topodatapb.TabletType_MASTER}); err != nil {
		return fmt.Errorf("cannot find MASTER tablet for destination shard for %v/%v: %v", vscw.destinationKeyspace, vscw.destinationShard, err)
	}
	masters := discovery.GetCurrentMaster(
		vscw.healthCheck.GetTabletStatsFromTarget(vscw.destinationKeyspace, vscw.destinationShard, topodatapb.TabletType_MASTER))
	if len(masters) == 0 {
		return fmt.Errorf("cannot find MASTER tablet for destination shard for %v/%v in HealthCheck: empty TabletStats list", vscw.destinationKeyspace, vscw.destinationShard)
	}
	master := masters[0]

	// Get the MySQL database name of the tablet.
	keyspaceAndShard := topoproto.KeyspaceShardString(vscw.destinationKeyspace, vscw.destinationShard)
	vscw.destinationDbNames[keyspaceAndShard] = topoproto.TabletDbName(master.Tablet)

	// TODO(mberlin): Verify on the destination master that the
	// _vt.blp_checkpoint table has the latest schema.

	vscw.wr.Logger().Infof("Using tablet %v as destination master for %v/%v", topoproto.TabletAliasString(master.Tablet.Alias), vscw.destinationKeyspace, vscw.destinationShard)
	vscw.wr.Logger().Infof("NOTE: The used master of a destination shard might change over the course of the copy e.g. due to a reparent. The HealthCheck module will track and log master changes and any error message will always refer the actually used master address.")

	return nil
}

// Find all tablets on the destination shard. This should be done immediately before refreshing
// the state on these tablets, to minimize the chances of the topo changing in between.

func (vscw *VerticalSplitCloneWorker) findRefreshTargets(ctx context.Context) error {
	refreshAliases, refreshTablets, err := resolveRefreshTabletsForShard(ctx, vscw.destinationKeyspace, vscw.destinationShard, vscw.wr)
	if err != nil {
		return err
	}
	vscw.refreshAliases, vscw.refreshTablets = refreshAliases, refreshTablets
	return nil
}

// clone phase:
//	- copy the data from source tablets to destination masters (with replication on)
// Assumes that the schema has already been created on each destination tablet
// (probably from vtctl's CopySchemaShard)
func (vscw *VerticalSplitCloneWorker) clone(ctx context.Context) error {
	vscw.setState(WorkerStateCloneOffline)
	start := time.Now()
	defer func() {
		statsStateDurationsNs.Set(string(WorkerStateCloneOffline), time.Now().Sub(start).Nanoseconds())
	}()

	// get source schema
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceSchemaDefinition, err := vscw.wr.GetSchema(shortCtx, vscw.sourceAlias, vscw.tables, nil, true)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot get schema from source %v: %v", topoproto.TabletAliasString(vscw.sourceAlias), err)
	}
	if len(sourceSchemaDefinition.TableDefinitions) == 0 {
		return fmt.Errorf("no tables matching the table filter")
	}
	vscw.wr.Logger().Infof("Source tablet has %v tables to copy", len(sourceSchemaDefinition.TableDefinitions))
	vscw.tableStatusList.initialize(sourceSchemaDefinition)

	// In parallel, setup the channels to send SQL data chunks to
	// for each destination tablet.
	//
	// mu protects firstError
	mu := sync.Mutex{}
	var firstError error

	ctx, cancelCopy := context.WithCancel(ctx)
	processError := func(format string, args ...interface{}) {
		vscw.wr.Logger().Errorf(format, args...)
		mu.Lock()
		if firstError == nil {
			firstError = fmt.Errorf(format, args...)
			cancelCopy()
		}
		mu.Unlock()
	}

	destinationWaitGroup := sync.WaitGroup{}

	// we create one channel for the destination tablet.  It
	// is sized to have a buffer of a maximum of
	// destinationWriterCount * 2 items, to hopefully
	// always have data. We then have
	// destinationWriterCount go routines reading from it.
	insertChannel := make(chan string, vscw.destinationWriterCount*2)
	// Set up the throttler for the destination shard.
	keyspaceAndShard := topoproto.KeyspaceShardString(vscw.destinationKeyspace, vscw.destinationShard)
	destinationThrottler, err := throttler.NewThrottler(
		keyspaceAndShard, "transactions", vscw.destinationWriterCount, vscw.maxTPS, throttler.ReplicationLagModuleDisabled)
	if err != nil {
		return fmt.Errorf("cannot instantiate throttler: %v", err)
	}
	for j := 0; j < vscw.destinationWriterCount; j++ {
		destinationWaitGroup.Add(1)
		go func(threadID int) {
			defer destinationWaitGroup.Done()
			defer destinationThrottler.ThreadFinished(threadID)

			executor := newExecutor(vscw.wr, vscw.healthCheck, destinationThrottler, vscw.destinationKeyspace, vscw.destinationShard, threadID)
			if err := executor.fetchLoop(ctx, insertChannel); err != nil {
				processError("executer.FetchLoop failed: %v", err)
			}
		}(j)
	}

	// Now for each table, read data chunks and send them to insertChannel
	sourceWaitGroup := sync.WaitGroup{}
	sema := sync2.NewSemaphore(vscw.sourceReaderCount, 0)
	dbName := vscw.destinationDbNames[topoproto.KeyspaceShardString(vscw.destinationKeyspace, vscw.destinationShard)]
	for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
		if td.Type == tmutils.TableView {
			continue
		}

		chunks, err := FindChunks(ctx, vscw.wr, vscw.sourceTablet, td, vscw.minTableSizeForSplit, vscw.sourceReaderCount)
		if err != nil {
			return err
		}
		vscw.tableStatusList.setThreadCount(tableIndex, len(chunks)-1)

		for chunkIndex := 0; chunkIndex < len(chunks)-1; chunkIndex++ {
			sourceWaitGroup.Add(1)
			go func(td *tabletmanagerdatapb.TableDefinition, tableIndex, chunkIndex int) {
				defer sourceWaitGroup.Done()

				sema.Acquire()
				defer sema.Release()

				vscw.tableStatusList.threadStarted(tableIndex)

				// build the query, and start the streaming
				selectSQL := buildSQLFromChunks(vscw.wr, td, chunks, chunkIndex, topoproto.TabletAliasString(vscw.sourceAlias))
				qrr, err := NewQueryResultReaderForTablet(ctx, vscw.wr.TopoServer(), vscw.sourceAlias, selectSQL)
				if err != nil {
					processError("NewQueryResultReaderForTablet failed: %v", err)
					return
				}
				defer qrr.Close()

				// process the data
				if err := vscw.processData(ctx, dbName, td, tableIndex, qrr, insertChannel, vscw.destinationPackCount); err != nil {
					processError("QueryResultReader failed: %v", err)
				}
				vscw.tableStatusList.threadDone(tableIndex)
			}(td, tableIndex, chunkIndex)
		}
	}
	sourceWaitGroup.Wait()

	close(insertChannel)
	destinationWaitGroup.Wait()
	// Stop Throttler.
	destinationThrottler.Close()
	if firstError != nil {
		return firstError
	}

	// then create and populate the blp_checkpoint table
	if vscw.strategy.skipPopulateBlpCheckpoint {
		vscw.wr.Logger().Infof("Skipping populating the blp_checkpoint table")
	} else {
		// get the current position from the source
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		status, err := vscw.wr.TabletManagerClient().SlaveStatus(shortCtx, vscw.sourceTablet)
		cancel()
		if err != nil {
			return err
		}

		queries := make([]string, 0, 4)
		queries = append(queries, binlogplayer.CreateBlpCheckpoint()...)
		flags := ""
		if vscw.strategy.dontStartBinlogPlayer {
			flags = binlogplayer.BlpFlagDontStart
		}
		queries = append(queries, binlogplayer.PopulateBlpCheckpoint(0, status.Position, vscw.maxTPS, throttler.ReplicationLagModuleDisabled, time.Now().Unix(), flags))
		vscw.wr.Logger().Infof("Making and populating blp_checkpoint table")
		if err := runSQLCommands(ctx, vscw.wr, vscw.healthCheck, vscw.destinationKeyspace, vscw.destinationShard, dbName, queries); err != nil {
			processError("blp_checkpoint queries failed: %v", err)
		}
		if firstError != nil {
			return firstError
		}
	}

	// Now we're done with data copy, update the shard's source info.
	if vscw.strategy.skipSetSourceShards {
		vscw.wr.Logger().Infof("Skipping setting SourceShard on destination shard.")
	} else {
		vscw.wr.Logger().Infof("Setting SourceShard on shard %v/%v", vscw.destinationKeyspace, vscw.destinationShard)
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		err := vscw.wr.SetSourceShards(shortCtx, vscw.destinationKeyspace, vscw.destinationShard, []*topodatapb.TabletAlias{vscw.sourceAlias}, vscw.tables)
		cancel()
		if err != nil {
			return fmt.Errorf("Failed to set source shards: %v", err)
		}
	}

	err = vscw.findRefreshTargets(ctx)
	if err != nil {
		return fmt.Errorf("failed before refreshing state on destination tablets: %v", err)
	}
	// And force a state refresh (re-read topo) on all destination tablets.
	// The master tablet will end up starting filtered replication
	// at this point.
	for _, tabletAlias := range vscw.refreshAliases {
		destinationWaitGroup.Add(1)
		go func(ti *topo.TabletInfo) {
			defer destinationWaitGroup.Done()
			vscw.wr.Logger().Infof("Refreshing state on tablet %v", ti.AliasString())
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			err := vscw.wr.TabletManagerClient().RefreshState(shortCtx, ti.Tablet)
			cancel()
			if err != nil {
				processError("RefreshState failed on tablet %v: %v", ti.AliasString(), err)
			}
		}(vscw.refreshTablets[*tabletAlias])
	}
	destinationWaitGroup.Wait()
	return firstError
}

// processData pumps the data out of the provided QueryResultReader.
// It returns any error the source encounters.
func (vscw *VerticalSplitCloneWorker) processData(ctx context.Context, dbName string, td *tabletmanagerdatapb.TableDefinition, tableIndex int, qrr *QueryResultReader, insertChannel chan string, destinationPackCount int) error {
	// process the data
	baseCmd := "INSERT INTO `" + dbName + "`." + td.Name + "(" + strings.Join(td.Columns, ", ") + ") VALUES "
	var rows [][]sqltypes.Value
	packCount := 0

	fields := qrr.Fields()
	for {
		r, err := qrr.Next()
		if err != nil {
			// we are done, see if there was an error
			if err != io.EOF {
				return err
			}

			// send the remainder if any
			if packCount > 0 {
				cmd := baseCmd + makeValueString(fields, rows)
				select {
				case insertChannel <- cmd:
				case <-ctx.Done():
					return nil
				}
			}
			return nil
		}

		// add the rows to our current result
		rows = append(rows, r.Rows...)
		vscw.tableStatusList.addCopiedRows(tableIndex, len(r.Rows))

		// see if we reach the destination pack count
		packCount++
		if packCount < destinationPackCount {
			continue
		}

		// send the rows to be inserted
		cmd := baseCmd + makeValueString(fields, rows)
		select {
		case insertChannel <- cmd:
		case <-ctx.Done():
			return nil
		}

		// and reset our row buffer
		rows = nil
		packCount = 0
	}
}
