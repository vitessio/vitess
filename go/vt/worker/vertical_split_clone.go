// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"html/template"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/worker/events"
	"github.com/youtube/vitess/go/vt/wrangler"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// VerticalSplitCloneWorker will clone the data from a source keyspace/shard
// to a destination keyspace/shard.
type VerticalSplitCloneWorker struct {
	StatusWorker

	wr                     *wrangler.Wrangler
	cell                   string
	destinationKeyspace    string
	destinationShard       string
	tables                 []string
	strategy               *mysqlctl.SplitStrategy
	sourceReaderCount      int
	destinationPackCount   int
	minTableSizeForSplit   uint64
	destinationWriterCount int
	cleaner                *wrangler.Cleaner

	// all subsequent fields are protected by the StatusWorker mutex

	// populated during WorkerStateInit, read-only after that
	sourceKeyspace string

	// populated during WorkerStateFindTargets, read-only after that
	sourceAlias  topo.TabletAlias
	sourceTablet *topo.TabletInfo

	// populated during WorkerStateCopy
	tableStatus []*tableStatus
	startTime   time.Time
	// aliases of tablets that need to have their schema reloaded.
	// Only populated once, read-only after that.
	reloadAliases []topo.TabletAlias
	reloadTablets map[topo.TabletAlias]*topo.TabletInfo

	ev *events.VerticalSplitClone

	// Mutex to protect fields that might change when (re)resolving topology.
	resolveMu                  sync.Mutex
	destinationShardsToTablets map[string]*topo.TabletInfo
	resolveTime                time.Time
}

// NewVerticalSplitCloneWorker returns a new VerticalSplitCloneWorker object.
func NewVerticalSplitCloneWorker(wr *wrangler.Wrangler, cell, destinationKeyspace, destinationShard string, tables []string, strategyStr string, sourceReaderCount, destinationPackCount int, minTableSizeForSplit uint64, destinationWriterCount int) (Worker, error) {
	strategy, err := mysqlctl.NewSplitStrategy(wr.Logger(), strategyStr)
	if err != nil {
		return nil, err
	}
	return &VerticalSplitCloneWorker{
		StatusWorker:           NewStatusWorker(),
		wr:                     wr,
		cell:                   cell,
		destinationKeyspace:    destinationKeyspace,
		destinationShard:       destinationShard,
		tables:                 tables,
		strategy:               strategy,
		sourceReaderCount:      sourceReaderCount,
		destinationPackCount:   destinationPackCount,
		minTableSizeForSplit:   minTableSizeForSplit,
		destinationWriterCount: destinationWriterCount,
		cleaner:                &wrangler.Cleaner{},

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
	vscw.Mu.Lock()
	defer vscw.Mu.Unlock()
	result := "<b>Working on:</b> " + vscw.destinationKeyspace + "/" + vscw.destinationShard + "</br>\n"
	result += "<b>State:</b> " + vscw.State.String() + "</br>\n"
	switch vscw.State {
	case WorkerStateCopy:
		result += "<b>Running</b>:</br>\n"
		result += "<b>Copying from</b>: " + vscw.sourceAlias.String() + "</br>\n"
		statuses, eta := formatTableStatuses(vscw.tableStatus, vscw.startTime)
		result += "<b>ETA</b>: " + eta.String() + "</br>\n"
		result += strings.Join(statuses, "</br>\n")
	case WorkerStateDone:
		result += "<b>Success</b>:</br>\n"
		statuses, _ := formatTableStatuses(vscw.tableStatus, vscw.startTime)
		result += strings.Join(statuses, "</br>\n")
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface
func (vscw *VerticalSplitCloneWorker) StatusAsText() string {
	vscw.Mu.Lock()
	defer vscw.Mu.Unlock()
	result := "Working on: " + vscw.destinationKeyspace + "/" + vscw.destinationShard + "\n"
	result += "State: " + vscw.State.String() + "\n"
	switch vscw.State {
	case WorkerStateCopy:
		result += "Running:\n"
		result += "Copying from: " + vscw.sourceAlias.String() + "\n"
		statuses, eta := formatTableStatuses(vscw.tableStatus, vscw.startTime)
		result += "ETA: " + eta.String() + "\n"
		result += strings.Join(statuses, "\n")
	case WorkerStateDone:
		result += "Success:\n"
		statuses, _ := formatTableStatuses(vscw.tableStatus, vscw.startTime)
		result += strings.Join(statuses, "\n")
	}
	return result
}

// Run implements the Worker interface
func (vscw *VerticalSplitCloneWorker) Run(ctx context.Context) error {
	resetVars()
	err := vscw.run(ctx)

	vscw.setState(WorkerStateCleanUp)
	cerr := vscw.cleaner.CleanUp(vscw.wr)
	if cerr != nil {
		if err != nil {
			vscw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
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
	if err := vscw.copy(ctx); err != nil {
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
	destinationKeyspaceInfo, err := vscw.wr.TopoServer().GetKeyspace(ctx, vscw.destinationKeyspace)
	if err != nil {
		return fmt.Errorf("cannot read destination keyspace %v: %v", vscw.destinationKeyspace, err)
	}
	if len(destinationKeyspaceInfo.ServedFroms) == 0 {
		return fmt.Errorf("destination keyspace %v has no KeyspaceServedFrom", vscw.destinationKeyspace)
	}

	// validate all serving types, find sourceKeyspace
	servingTypes := []pb.TabletType{pb.TabletType_MASTER, pb.TabletType_REPLICA, pb.TabletType_RDONLY}
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

	return nil
}

// findTargets phase:
// - find one rdonly in the source shard
// - mark it as 'worker' pointing back to us
// - get the aliases of all the targets
func (vscw *VerticalSplitCloneWorker) findTargets(ctx context.Context) error {
	vscw.setState(WorkerStateFindTargets)

	// find an appropriate endpoint in the source shard
	var err error
	vscw.sourceAlias, err = FindWorkerTablet(ctx, vscw.wr, vscw.cleaner, vscw.cell, vscw.sourceKeyspace, "0")
	if err != nil {
		return fmt.Errorf("FindWorkerTablet() failed for %v/%v/0: %v", vscw.cell, vscw.sourceKeyspace, err)
	}
	vscw.wr.Logger().Infof("Using tablet %v as the source", vscw.sourceAlias)

	// get the tablet info for it
	vscw.sourceTablet, err = vscw.wr.TopoServer().GetTablet(ctx, vscw.sourceAlias)
	if err != nil {
		return fmt.Errorf("cannot read tablet %v: %v", vscw.sourceTablet, err)
	}

	// stop replication on it
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	err = vscw.wr.TabletManagerClient().StopSlave(shortCtx, vscw.sourceTablet)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot stop replication on tablet %v", vscw.sourceAlias)
	}

	wrangler.RecordStartSlaveAction(vscw.cleaner, vscw.sourceTablet)
	action, err := wrangler.FindChangeSlaveTypeActionByTarget(vscw.cleaner, vscw.sourceAlias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", vscw.sourceAlias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	return vscw.ResolveDestinationMasters(ctx)
}

// ResolveDestinationMasters implements the Resolver interface.
// It will attempt to resolve all shards and update vscw.destinationShardsToTablets;
// if it is unable to do so, it will not modify vscw.destinationShardsToTablets at all.
func (vscw *VerticalSplitCloneWorker) ResolveDestinationMasters(ctx context.Context) error {
	statsDestinationAttemptedResolves.Add(1)
	// Allow at most one resolution request at a time; if there are concurrent requests, only
	// one of them will actualy hit the topo server.
	vscw.resolveMu.Lock()
	defer vscw.resolveMu.Unlock()

	// If the last resolution was fresh enough, return it.
	if time.Now().Sub(vscw.resolveTime) < *resolveTTL {
		return nil
	}

	ti, err := resolveDestinationShardMaster(ctx, vscw.destinationKeyspace, vscw.destinationShard, vscw.wr)
	if err != nil {
		return err
	}
	vscw.destinationShardsToTablets = map[string]*topo.TabletInfo{vscw.destinationShard: ti}
	// save the time of the last successful resolution
	vscw.resolveTime = time.Now()
	statsDestinationActualResolves.Add(1)
	return nil
}

// GetDestinationMaster implements the Resolver interface
func (vscw *VerticalSplitCloneWorker) GetDestinationMaster(shardName string) (*topo.TabletInfo, error) {
	vscw.resolveMu.Lock()
	defer vscw.resolveMu.Unlock()
	ti, ok := vscw.destinationShardsToTablets[shardName]
	if !ok {
		return nil, fmt.Errorf("no tablet found for destination shard %v", shardName)
	}
	return ti, nil
}

// Find all tablets on the destination shard. This should be done immediately before reloading
// the schema on these tablets, to minimize the chances of the topo changing in between.

func (vscw *VerticalSplitCloneWorker) findReloadTargets(ctx context.Context) error {
	reloadAliases, reloadTablets, err := resolveReloadTabletsForShard(ctx, vscw.destinationKeyspace, vscw.destinationShard, vscw.wr)
	if err != nil {
		return err
	}
	vscw.reloadAliases, vscw.reloadTablets = reloadAliases, reloadTablets
	return nil
}

// copy phase:
//	- copy the data from source tablets to destination masters (wtih replication on)
// Assumes that the schema has already been created on each destination tablet
// (probably from vtctl's CopySchemaShard)
func (vscw *VerticalSplitCloneWorker) copy(ctx context.Context) error {
	vscw.setState(WorkerStateCopy)

	// get source schema
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	sourceSchemaDefinition, err := vscw.wr.GetSchema(shortCtx, vscw.sourceAlias, vscw.tables, nil, true)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot get schema from source %v: %v", vscw.sourceAlias, err)
	}
	if len(sourceSchemaDefinition.TableDefinitions) == 0 {
		return fmt.Errorf("no tables matching the table filter")
	}
	vscw.wr.Logger().Infof("Source tablet has %v tables to copy", len(sourceSchemaDefinition.TableDefinitions))
	vscw.Mu.Lock()
	vscw.tableStatus = make([]*tableStatus, len(sourceSchemaDefinition.TableDefinitions))
	for i, td := range sourceSchemaDefinition.TableDefinitions {
		vscw.tableStatus[i] = &tableStatus{
			name:     td.Name,
			rowCount: td.RowCount,
		}
	}
	vscw.startTime = time.Now()
	vscw.Mu.Unlock()

	// Count rows
	for i, td := range sourceSchemaDefinition.TableDefinitions {
		vscw.tableStatus[i].mu.Lock()
		if td.Type == myproto.TableBaseTable {
			vscw.tableStatus[i].rowCount = td.RowCount
		} else {
			vscw.tableStatus[i].isView = true
		}
		vscw.tableStatus[i].mu.Unlock()
	}

	// In parallel, setup the channels to send SQL data chunks to
	// for each destination tablet.
	//
	// mu protects firstError
	mu := sync.Mutex{}
	var firstError error

	ctx, cancel = context.WithCancel(ctx)
	processError := func(format string, args ...interface{}) {
		vscw.wr.Logger().Errorf(format, args...)
		mu.Lock()
		if firstError == nil {
			firstError = fmt.Errorf(format, args...)
			cancel()
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

	go func(shardName string, insertChannel chan string) {
		for j := 0; j < vscw.destinationWriterCount; j++ {
			destinationWaitGroup.Add(1)
			go func() {
				defer destinationWaitGroup.Done()

				if err := executeFetchLoop(ctx, vscw.wr, vscw, shardName, insertChannel); err != nil {
					processError("executeFetchLoop failed: %v", err)
				}
			}()
		}
	}(vscw.destinationShard, insertChannel)

	// Now for each table, read data chunks and send them to insertChannel
	sourceWaitGroup := sync.WaitGroup{}
	sema := sync2.NewSemaphore(vscw.sourceReaderCount, 0)
	for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
		if td.Type == myproto.TableView {
			continue
		}

		chunks, err := FindChunks(ctx, vscw.wr, vscw.sourceTablet, td, vscw.minTableSizeForSplit, vscw.sourceReaderCount)
		if err != nil {
			return err
		}
		vscw.tableStatus[tableIndex].setThreadCount(len(chunks) - 1)

		for chunkIndex := 0; chunkIndex < len(chunks)-1; chunkIndex++ {
			sourceWaitGroup.Add(1)
			go func(td *myproto.TableDefinition, tableIndex, chunkIndex int) {
				defer sourceWaitGroup.Done()

				sema.Acquire()
				defer sema.Release()

				vscw.tableStatus[tableIndex].threadStarted()

				// build the query, and start the streaming
				selectSQL := buildSQLFromChunks(vscw.wr, td, chunks, chunkIndex, vscw.sourceAlias.String())
				qrr, err := NewQueryResultReaderForTablet(ctx, vscw.wr.TopoServer(), vscw.sourceAlias, selectSQL)
				if err != nil {
					processError("NewQueryResultReaderForTablet failed: %v", err)
					return
				}
				defer qrr.Close()

				// process the data
				if err := vscw.processData(td, tableIndex, qrr, insertChannel, vscw.destinationPackCount, ctx.Done()); err != nil {
					processError("QueryResultReader failed: %v", err)
				}
				vscw.tableStatus[tableIndex].threadDone()
			}(td, tableIndex, chunkIndex)
		}
	}
	sourceWaitGroup.Wait()

	close(insertChannel)
	destinationWaitGroup.Wait()
	if firstError != nil {
		return firstError
	}

	// then create and populate the blp_checkpoint table
	if vscw.strategy.PopulateBlpCheckpoint {
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
		if vscw.strategy.DontStartBinlogPlayer {
			flags = binlogplayer.BlpFlagDontStart
		}
		queries = append(queries, binlogplayer.PopulateBlpCheckpoint(0, status.Position, time.Now().Unix(), flags))
		destinationWaitGroup.Add(1)
		go func(shardName string) {
			defer destinationWaitGroup.Done()
			vscw.wr.Logger().Infof("Making and populating blp_checkpoint table")
			if err := runSqlCommands(ctx, vscw.wr, vscw, shardName, queries); err != nil {
				processError("blp_checkpoint queries failed: %v", err)
			}
		}(vscw.destinationShard)
		destinationWaitGroup.Wait()
		if firstError != nil {
			return firstError
		}
	}

	// Now we're done with data copy, update the shard's source info.
	if vscw.strategy.SkipSetSourceShards {
		vscw.wr.Logger().Infof("Skipping setting SourceShard on destination shard.")
	} else {
		vscw.wr.Logger().Infof("Setting SourceShard on shard %v/%v", vscw.destinationKeyspace, vscw.destinationShard)
		shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
		err := vscw.wr.SetSourceShards(shortCtx, vscw.destinationKeyspace, vscw.destinationShard, []topo.TabletAlias{vscw.sourceAlias}, vscw.tables)
		cancel()
		if err != nil {
			return fmt.Errorf("Failed to set source shards: %v", err)
		}
	}

	err = vscw.findReloadTargets(ctx)
	if err != nil {
		return fmt.Errorf("failed before reloading schema on destination tablets: %v", err)
	}
	// And force a schema reload on all destination tablets.
	// The master tablet will end up starting filtered replication
	// at this point.
	for _, tabletAlias := range vscw.reloadAliases {
		destinationWaitGroup.Add(1)
		go func(ti *topo.TabletInfo) {
			defer destinationWaitGroup.Done()
			vscw.wr.Logger().Infof("Reloading schema on tablet %v", ti.Alias)
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			err := vscw.wr.TabletManagerClient().ReloadSchema(shortCtx, ti)
			cancel()
			if err != nil {
				processError("ReloadSchema failed on tablet %v: %v", ti.Alias, err)
			}
		}(vscw.reloadTablets[tabletAlias])
	}
	destinationWaitGroup.Wait()
	return firstError
}

// processData pumps the data out of the provided QueryResultReader.
// It returns any error the source encounters.
func (vscw *VerticalSplitCloneWorker) processData(td *myproto.TableDefinition, tableIndex int, qrr *QueryResultReader, insertChannel chan string, destinationPackCount int, abort <-chan struct{}) error {
	// process the data
	baseCmd := td.Name + "(" + strings.Join(td.Columns, ", ") + ") VALUES "
	var rows [][]sqltypes.Value
	packCount := 0

	for {
		select {
		case r, ok := <-qrr.Output:
			if !ok {
				// we are done, see if there was an error
				err := qrr.Error()
				if err != nil {
					return err
				}

				// send the remainder if any
				if packCount > 0 {
					cmd := baseCmd + makeValueString(qrr.Fields, rows)
					select {
					case insertChannel <- cmd:
					case <-abort:
						return nil
					}
				}
				return nil
			}

			// add the rows to our current result
			rows = append(rows, r.Rows...)
			vscw.tableStatus[tableIndex].addCopiedRows(len(r.Rows))

			// see if we reach the destination pack count
			packCount++
			if packCount < destinationPackCount {
				continue
			}

			// send the rows to be inserted
			cmd := baseCmd + makeValueString(qrr.Fields, rows)
			select {
			case insertChannel <- cmd:
			case <-abort:
				return nil
			}

			// and reset our row buffer
			rows = nil
			packCount = 0

		case <-abort:
			return nil
		}
		// the abort case might be starved above, so we check again; this means that the loop
		// will run at most once before the abort case is triggered.
		select {
		case <-abort:
			return nil
		default:
		}
	}
}
