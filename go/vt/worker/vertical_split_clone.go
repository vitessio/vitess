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
)

const (
	// all the states for the worker
	stateVSCNotSarted = "not started"
	stateVSCDone      = "done"
	stateVSCError     = "error"

	stateVSCInit        = "initializing"
	stateVSCFindTargets = "finding target instances"
	stateVSCCopy        = "copying the data"
	stateVSCCleanUp     = "cleaning up"
)

// VerticalSplitCloneWorker will clone the data from a source keyspace/shard
// to a destination keyspace/shard.
type VerticalSplitCloneWorker struct {
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
	ctx                    context.Context
	ctxCancel              context.CancelFunc

	// all subsequent fields are protected by the mutex
	mu    sync.Mutex
	state string

	// populated if state == stateVSCError
	err error

	// populated during stateVSCInit, read-only after that
	sourceKeyspace string

	// populated during stateVSCFindTargets, read-only after that
	sourceAlias  topo.TabletAlias
	sourceTablet *topo.TabletInfo

	// populated during stateVSCCopy
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
}

// NewVerticalSplitCloneWorker returns a new VerticalSplitCloneWorker object.
func NewVerticalSplitCloneWorker(wr *wrangler.Wrangler, cell, destinationKeyspace, destinationShard string, tables []string, strategyStr string, sourceReaderCount, destinationPackCount int, minTableSizeForSplit uint64, destinationWriterCount int) (Worker, error) {
	strategy, err := mysqlctl.NewSplitStrategy(wr.Logger(), strategyStr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &VerticalSplitCloneWorker{
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
		ctx:                    ctx,
		ctxCancel:              cancel,

		state: stateVSCNotSarted,
		ev: &events.VerticalSplitClone{
			Cell:     cell,
			Keyspace: destinationKeyspace,
			Shard:    destinationShard,
			Tables:   tables,
			Strategy: strategy.String(),
		},
	}, nil
}

func (vscw *VerticalSplitCloneWorker) setState(state string) {
	vscw.mu.Lock()
	vscw.state = state
	vscw.mu.Unlock()

	event.DispatchUpdate(vscw.ev, state)
}

func (vscw *VerticalSplitCloneWorker) recordError(err error) {
	vscw.mu.Lock()
	vscw.state = stateVSCError
	vscw.err = err
	vscw.mu.Unlock()

	event.DispatchUpdate(vscw.ev, "error: "+err.Error())
}

// StatusAsHTML implements the Worker interface
func (vscw *VerticalSplitCloneWorker) StatusAsHTML() template.HTML {
	vscw.mu.Lock()
	defer vscw.mu.Unlock()
	result := "<b>Working on:</b> " + vscw.destinationKeyspace + "/" + vscw.destinationShard + "</br>\n"
	result += "<b>State:</b> " + vscw.state + "</br>\n"
	switch vscw.state {
	case stateVSCError:
		result += "<b>Error</b>: " + vscw.err.Error() + "</br>\n"
	case stateVSCCopy:
		result += "<b>Running</b>:</br>\n"
		result += "<b>Copying from</b>: " + vscw.sourceAlias.String() + "</br>\n"
		statuses, eta := formatTableStatuses(vscw.tableStatus, vscw.startTime)
		result += "<b>ETA</b>: " + eta.String() + "</br>\n"
		result += strings.Join(statuses, "</br>\n")
	case stateVSCDone:
		result += "<b>Success</b>:</br>\n"
		statuses, _ := formatTableStatuses(vscw.tableStatus, vscw.startTime)
		result += strings.Join(statuses, "</br>\n")
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface
func (vscw *VerticalSplitCloneWorker) StatusAsText() string {
	vscw.mu.Lock()
	defer vscw.mu.Unlock()
	result := "Working on: " + vscw.destinationKeyspace + "/" + vscw.destinationShard + "\n"
	result += "State: " + vscw.state + "\n"
	switch vscw.state {
	case stateVSCError:
		result += "Error: " + vscw.err.Error() + "\n"
	case stateVSCCopy:
		result += "Running:\n"
		result += "Copying from: " + vscw.sourceAlias.String() + "\n"
		statuses, eta := formatTableStatuses(vscw.tableStatus, vscw.startTime)
		result += "ETA: " + eta.String() + "\n"
		result += strings.Join(statuses, "\n")
	case stateVSCDone:
		result += "Success:\n"
		statuses, _ := formatTableStatuses(vscw.tableStatus, vscw.startTime)
		result += strings.Join(statuses, "\n")
	}
	return result
}

// Cancel is part of the Worker interface
func (vscw *VerticalSplitCloneWorker) Cancel() {
	vscw.ctxCancel()
}

func (vscw *VerticalSplitCloneWorker) checkInterrupted() bool {
	select {
	case <-vscw.ctx.Done():
		if vscw.ctx.Err() == context.DeadlineExceeded {
			return false
		}
		vscw.recordError(topo.ErrInterrupted)
		return true
	default:
	}
	return false
}

// Run implements the Worker interface
func (vscw *VerticalSplitCloneWorker) Run() {
	err := vscw.run()

	vscw.setState(stateVSCCleanUp)
	cerr := vscw.cleaner.CleanUp(vscw.wr)
	if cerr != nil {
		if err != nil {
			vscw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		vscw.recordError(err)
		return
	}
	vscw.setState(stateVSCDone)
}

func (vscw *VerticalSplitCloneWorker) Error() error {
	return vscw.err
}

func (vscw *VerticalSplitCloneWorker) run() error {
	// first state: read what we need to do
	if err := vscw.init(); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if vscw.checkInterrupted() {
		return topo.ErrInterrupted
	}

	// second state: find targets
	if err := vscw.findTargets(); err != nil {
		// A canceled context can appear to cause an application error
		if vscw.checkInterrupted() {
			return topo.ErrInterrupted
		}
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if vscw.checkInterrupted() {
		return topo.ErrInterrupted
	}

	// third state: copy data
	if err := vscw.copy(); err != nil {
		// A canceled context can appear to cause an application error
		if vscw.checkInterrupted() {
			return topo.ErrInterrupted
		}
		return fmt.Errorf("copy() failed: %v", err)
	}
	if vscw.checkInterrupted() {
		return topo.ErrInterrupted
	}

	return nil
}

// init phase:
// - read the destination keyspace, make sure it has 'servedFrom' values
func (vscw *VerticalSplitCloneWorker) init() error {
	vscw.setState(stateVSCInit)

	// read the keyspace and validate it
	destinationKeyspaceInfo, err := vscw.wr.TopoServer().GetKeyspace(vscw.destinationKeyspace)
	if err != nil {
		return fmt.Errorf("cannot read destination keyspace %v: %v", vscw.destinationKeyspace, err)
	}
	if len(destinationKeyspaceInfo.ServedFromMap) == 0 {
		return fmt.Errorf("destination keyspace %v has no KeyspaceServedFrom", vscw.destinationKeyspace)
	}

	// validate all serving types, find sourceKeyspace
	servingTypes := []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY}
	servedFrom := ""
	for _, st := range servingTypes {
		sf, ok := destinationKeyspaceInfo.ServedFromMap[st]
		if !ok {
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
// - mark it as 'checker' pointing back to us
// - get the aliases of all the targets
func (vscw *VerticalSplitCloneWorker) findTargets() error {
	vscw.setState(stateVSCFindTargets)

	// find an appropriate endpoint in the source shard
	var err error
	vscw.sourceAlias, err = findChecker(vscw.ctx, vscw.wr, vscw.cleaner, vscw.cell, vscw.sourceKeyspace, "0")
	if err != nil {
		return fmt.Errorf("cannot find checker for %v/%v/0: %v", vscw.cell, vscw.sourceKeyspace, err)
	}
	vscw.wr.Logger().Infof("Using tablet %v as the source", vscw.sourceAlias)

	// get the tablet info for it
	vscw.sourceTablet, err = vscw.wr.TopoServer().GetTablet(vscw.sourceAlias)
	if err != nil {
		return fmt.Errorf("cannot read tablet %v: %v", vscw.sourceTablet, err)
	}

	// stop replication on it
	ctx, cancel := context.WithTimeout(vscw.ctx, 60*time.Second)
	err = vscw.wr.TabletManagerClient().StopSlave(ctx, vscw.sourceTablet)
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

	return vscw.ResolveDestinationMasters()
}

// ResolveDestinationMasters implements the Resolver interface.
func (vscw *VerticalSplitCloneWorker) ResolveDestinationMasters() error {
	vscw.resolveMu.Lock()
	defer vscw.resolveMu.Unlock()

	ti, err := resolveDestinationShardMaster(vscw.ctx, vscw.destinationKeyspace, vscw.destinationShard, vscw.wr)
	if err != nil {
		return err
	}
	vscw.destinationShardsToTablets = map[string]*topo.TabletInfo{vscw.destinationShard: ti}
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

func (vscw *VerticalSplitCloneWorker) findReloadTargets() error {
	reloadAliases, reloadTablets, err := resolveReloadTabletsForShard(vscw.ctx, vscw.destinationKeyspace, vscw.destinationShard, vscw.wr)
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
func (vscw *VerticalSplitCloneWorker) copy() error {
	vscw.setState(stateVSCCopy)

	// get source schema
	ctx, cancel := context.WithTimeout(vscw.ctx, 60*time.Second)
	sourceSchemaDefinition, err := vscw.wr.GetSchema(ctx, vscw.sourceAlias, vscw.tables, nil, true)
	cancel()
	if err != nil {
		return fmt.Errorf("cannot get schema from source %v: %v", vscw.sourceAlias, err)
	}
	if len(sourceSchemaDefinition.TableDefinitions) == 0 {
		return fmt.Errorf("no tables matching the table filter")
	}
	vscw.wr.Logger().Infof("Source tablet has %v tables to copy", len(sourceSchemaDefinition.TableDefinitions))
	vscw.mu.Lock()
	vscw.tableStatus = make([]*tableStatus, len(sourceSchemaDefinition.TableDefinitions))
	for i, td := range sourceSchemaDefinition.TableDefinitions {
		vscw.tableStatus[i] = &tableStatus{
			name:     td.Name,
			rowCount: td.RowCount,
		}
	}
	vscw.startTime = time.Now()
	vscw.mu.Unlock()

	// Count rows
	for i, td := range sourceSchemaDefinition.TableDefinitions {
		vscw.tableStatus[i].mu.Lock()
		if td.Type == myproto.TABLE_BASE_TABLE {
			vscw.tableStatus[i].rowCount = td.RowCount
		} else {
			vscw.tableStatus[i].isView = true
		}
		vscw.tableStatus[i].mu.Unlock()
	}

	// In parallel, setup the channels to send SQL data chunks to for each destination tablet.
	//
	// mu protects the context for cancelation, and firstError
	mu := sync.Mutex{}
	var firstError error

	processError := func(format string, args ...interface{}) {
		vscw.wr.Logger().Errorf(format, args...)
		mu.Lock()
		if !vscw.checkInterrupted() {
			vscw.Cancel()
			firstError = fmt.Errorf(format, args...)
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

				if err := executeFetchLoop(vscw.ctx, vscw.wr, vscw, shardName, insertChannel); err != nil {
					processError("executeFetchLoop failed: %v", err)
				}
			}()
		}
	}(vscw.destinationShard, insertChannel)

	// Now for each table, read data chunks and send them to insertChannel
	sourceWaitGroup := sync.WaitGroup{}
	sema := sync2.NewSemaphore(vscw.sourceReaderCount, 0)
	for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
		if td.Type == myproto.TABLE_VIEW {
			continue
		}

		chunks, err := findChunks(vscw.ctx, vscw.wr, vscw.sourceTablet, td, vscw.minTableSizeForSplit, vscw.sourceReaderCount)
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
				qrr, err := NewQueryResultReaderForTablet(vscw.ctx, vscw.wr.TopoServer(), vscw.sourceAlias, selectSQL)
				if err != nil {
					processError("NewQueryResultReaderForTablet failed: %v", err)
					return
				}
				defer qrr.Close()

				// process the data
				if err := vscw.processData(td, tableIndex, qrr, insertChannel, vscw.destinationPackCount, vscw.ctx.Done()); err != nil {
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
		ctx, cancel := context.WithTimeout(vscw.ctx, 60*time.Second)
		status, err := vscw.wr.TabletManagerClient().SlaveStatus(ctx, vscw.sourceTablet)
		cancel()
		if err != nil {
			return err
		}

		queries := make([]string, 0, 4)
		queries = append(queries, binlogplayer.CreateBlpCheckpoint()...)
		flags := ""
		if vscw.strategy.DontStartBinlogPlayer {
			flags = binlogplayer.BLP_FLAG_DONT_START
		}
		queries = append(queries, binlogplayer.PopulateBlpCheckpoint(0, status.Position, time.Now().Unix(), flags))
		destinationWaitGroup.Add(1)
		go func(shardName string) {
			defer destinationWaitGroup.Done()
			vscw.wr.Logger().Infof("Making and populating blp_checkpoint table")
			if err := runSqlCommands(vscw.ctx, vscw.wr, vscw, shardName, queries); err != nil {
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
		ctx, cancel := context.WithTimeout(vscw.ctx, 60*time.Second)
		err := vscw.wr.SetSourceShards(ctx, vscw.destinationKeyspace, vscw.destinationShard, []topo.TabletAlias{vscw.sourceAlias}, vscw.tables)
		cancel()
		if err != nil {
			return fmt.Errorf("Failed to set source shards: %v", err)
		}
	}

	err = vscw.findReloadTargets()
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
			ctx, cancel := context.WithTimeout(vscw.ctx, 30*time.Second)
			err := vscw.wr.TabletManagerClient().ReloadSchema(ctx, ti)
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
