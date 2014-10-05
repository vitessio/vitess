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

	"github.com/youtube/vitess/go/event"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/worker/events"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const (
	// all the states for the worker
	stateSCNotSarted = "not started"
	stateSCDone      = "done"
	stateSCError     = "error"

	stateSCInit        = "initializing"
	stateSCFindTargets = "finding target instances"
	stateSCCopy        = "copying the data"
	stateSCCleanUp     = "cleaning up"
)

// SplitCloneWorker will clone the data within a keyspace from a
// source set of shards to a destination set of shards.
type SplitCloneWorker struct {
	wr                     *wrangler.Wrangler
	cell                   string
	keyspace               string
	shard                  string
	excludeTables          []string
	strategy               string
	sourceReaderCount      int
	minTableSizeForSplit   uint64
	destinationWriterCount int
	cleaner                *wrangler.Cleaner

	// all subsequent fields are protected by the mutex
	mu    sync.Mutex
	state string

	// populated if state == stateSCError
	err error

	// populated during stateSCInit, read-only after that
	keyspaceInfo      *topo.KeyspaceInfo
	sourceShards      []*topo.ShardInfo
	destinationShards []*topo.ShardInfo

	// populated during stateSCFindTargets, read-only after that
	sourceAliases            []topo.TabletAlias
	sourceTablets            []*topo.TabletInfo
	destinationAliases       [][]topo.TabletAlias
	destinationTablets       []map[topo.TabletAlias]*topo.TabletInfo
	destinationMasterAliases []topo.TabletAlias

	// populated during stateSCCopy
	tableStatus []tableStatus
	startTime   time.Time

	ev *events.SplitClone
}

// NewSplitCloneWorker returns a new SplitCloneWorker object.
func NewSplitCloneWorker(wr *wrangler.Wrangler, cell, keyspace, shard string, excludeTables []string, strategy string, sourceReaderCount int, minTableSizeForSplit uint64, destinationWriterCount int) Worker {
	return &SplitCloneWorker{
		wr:                     wr,
		cell:                   cell,
		keyspace:               keyspace,
		shard:                  shard,
		excludeTables:          excludeTables,
		strategy:               strategy,
		sourceReaderCount:      sourceReaderCount,
		minTableSizeForSplit:   minTableSizeForSplit,
		destinationWriterCount: destinationWriterCount,
		cleaner:                &wrangler.Cleaner{},

		state: stateSCNotSarted,
		ev: &events.SplitClone{
			Cell:          cell,
			Keyspace:      keyspace,
			Shard:         shard,
			ExcludeTables: excludeTables,
			Strategy:      strategy,
		},
	}
}

func (scw *SplitCloneWorker) setState(state string) {
	scw.mu.Lock()
	scw.state = state
	scw.mu.Unlock()

	event.DispatchUpdate(scw.ev, state)
}

func (scw *SplitCloneWorker) recordError(err error) {
	scw.mu.Lock()
	scw.state = stateSCError
	scw.err = err
	scw.mu.Unlock()

	event.DispatchUpdate(scw.ev, "error: "+err.Error())
}

func (scw *SplitCloneWorker) formatSources() string {
	result := ""
	for _, alias := range scw.sourceAliases {
		result += " " + alias.String()
	}
	return result
}

// StatusAsHTML implements the Worker interface
func (scw *SplitCloneWorker) StatusAsHTML() template.HTML {
	scw.mu.Lock()
	defer scw.mu.Unlock()
	result := "<b>Working on:</b> " + scw.keyspace + "/" + scw.shard + "</br>\n"
	result += "<b>State:</b> " + scw.state + "</br>\n"
	switch scw.state {
	case stateSCError:
		result += "<b>Error</b>: " + scw.err.Error() + "</br>\n"
	case stateSCCopy:
		result += "<b>Running</b>:</br>\n"
		result += "<b>Copying from</b>: " + scw.formatSources() + "</br>\n"
		statuses, eta := formatTableStatuses(scw.tableStatus, scw.startTime)
		result += "<b>ETA</b>: " + eta.String() + "</br>\n"
		result += strings.Join(statuses, "</br>\n")
	case stateSCDone:
		result += "<b>Success</b>:</br>\n"
		statuses, _ := formatTableStatuses(scw.tableStatus, scw.startTime)
		result += strings.Join(statuses, "</br>\n")
	}

	return template.HTML(result)
}

// StatusAsText implements the Worker interface
func (scw *SplitCloneWorker) StatusAsText() string {
	scw.mu.Lock()
	defer scw.mu.Unlock()
	result := "Working on: " + scw.keyspace + "/" + scw.shard + "\n"
	result += "State: " + scw.state + "\n"
	switch scw.state {
	case stateSCError:
		result += "Error: " + scw.err.Error() + "\n"
	case stateSCCopy:
		result += "Running:\n"
		result += "Copying from: " + scw.formatSources() + "\n"
		statuses, eta := formatTableStatuses(scw.tableStatus, scw.startTime)
		result += "ETA: " + eta.String() + "\n"
		result += strings.Join(statuses, "\n")
	case stateSCDone:
		result += "Success:\n"
		statuses, _ := formatTableStatuses(scw.tableStatus, scw.startTime)
		result += strings.Join(statuses, "\n")
	}
	return result
}

func (scw *SplitCloneWorker) CheckInterrupted() bool {
	select {
	case <-interrupted:
		scw.recordError(topo.ErrInterrupted)
		return true
	default:
	}
	return false
}

// Run implements the Worker interface
func (scw *SplitCloneWorker) Run() {
	err := scw.run()

	scw.setState(stateSCCleanUp)
	cerr := scw.cleaner.CleanUp(scw.wr)
	if cerr != nil {
		if err != nil {
			scw.wr.Logger().Errorf("CleanUp failed in addition to job error: %v", cerr)
		} else {
			err = cerr
		}
	}
	if err != nil {
		scw.recordError(err)
		return
	}
	scw.setState(stateSCDone)
}

func (scw *SplitCloneWorker) Error() error {
	return scw.err
}

func (scw *SplitCloneWorker) run() error {
	// first state: read what we need to do
	if err := scw.init(); err != nil {
		return fmt.Errorf("init() failed: %v", err)
	}
	if scw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// second state: find targets
	if err := scw.findTargets(); err != nil {
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if scw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// third state: copy data
	if err := scw.copy(); err != nil {
		return fmt.Errorf("copy() failed: %v", err)
	}
	if scw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	return nil
}

// init phase:
// - read the destination keyspace, make sure it has 'servedFrom' values
func (scw *SplitCloneWorker) init() error {
	scw.setState(stateSCInit)
	var err error

	// read the keyspace and validate it
	scw.keyspaceInfo, err = scw.wr.TopoServer().GetKeyspace(scw.keyspace)
	if err != nil {
		return fmt.Errorf("cannot read keyspace %v: %v", scw.keyspace, err)
	}

	// find the OverlappingShards in the keyspace
	osList, err := topotools.FindOverlappingShards(scw.wr.TopoServer(), scw.keyspace)
	if err != nil {
		return fmt.Errorf("cannot FindOverlappingShards in %v: %v", scw.keyspace, err)
	}

	// find the shard we mentioned in there, if any
	os := topotools.OverlappingShardsForShard(osList, scw.shard)
	if os == nil {
		return fmt.Errorf("the specified shard %v/%v is not in any overlapping shard", scw.keyspace, scw.shard)
	}

	// one side should have served types, the other one none,
	// figure out wich is which, then double check them all
	if len(os.Left[0].ServedTypes) > 0 {
		scw.sourceShards = os.Left
		scw.destinationShards = os.Right
	} else {
		scw.sourceShards = os.Right
		scw.destinationShards = os.Left
	}

	// validate all serving types
	servingTypes := []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY}
	for _, st := range servingTypes {
		for _, si := range scw.sourceShards {
			if !topo.IsTypeInList(st, si.ServedTypes) {
				return fmt.Errorf("source shard %v/%v is not serving type %v", si.Keyspace(), si.ShardName(), st)
			}
		}
	}
	for _, si := range scw.destinationShards {
		if len(si.ServedTypes) > 0 {
			return fmt.Errorf("destination shard %v/%v is serving some types", si.Keyspace(), si.ShardName())
		}
	}

	return nil
}

// findTargets phase:
// - find one rdonly in the source shard
// - mark it as 'checker' pointing back to us
// - get the aliases of all the targets
func (scw *SplitCloneWorker) findTargets() error {
	scw.setState(stateSCFindTargets)
	var err error

	// find an appropriate endpoint in the source shards
	scw.sourceAliases = make([]topo.TabletAlias, len(scw.sourceShards))
	for i, si := range scw.sourceShards {
		scw.sourceAliases[i], err = findChecker(scw.wr, scw.cleaner, scw.cell, si.Keyspace(), si.ShardName())
		if err != nil {
			return fmt.Errorf("cannot find checker for %v/%v/%v: %v", scw.cell, si.Keyspace(), si.ShardName(), err)
		}
		scw.wr.Logger().Infof("Using tablet %v as source for %v/%v", scw.sourceAliases[i], si.Keyspace(), si.ShardName())
	}

	// get the tablet info for them
	scw.sourceTablets = make([]*topo.TabletInfo, len(scw.sourceAliases))
	for i, alias := range scw.sourceAliases {
		scw.sourceTablets[i], err = scw.wr.TopoServer().GetTablet(alias)
		if err != nil {
			return fmt.Errorf("cannot read tablet %v: %v", alias, err)
		}
	}

	// find all the targets in the destination shards
	scw.destinationAliases = make([][]topo.TabletAlias, len(scw.destinationShards))
	scw.destinationTablets = make([]map[topo.TabletAlias]*topo.TabletInfo, len(scw.destinationShards))
	scw.destinationMasterAliases = make([]topo.TabletAlias, len(scw.destinationShards))
	for shardIndex, si := range scw.destinationShards {
		scw.destinationAliases[shardIndex], err = topo.FindAllTabletAliasesInShard(scw.wr.TopoServer(), si.Keyspace(), si.ShardName())
		if err != nil {
			return fmt.Errorf("cannot find all target tablets in %v/%v: %v", si.Keyspace(), si.ShardName(), err)
		}
		scw.wr.Logger().Infof("Found %v target aliases in shard %v/%v", len(scw.destinationAliases[shardIndex]), si.Keyspace(), si.ShardName())

		// get the TabletInfo for all targets
		scw.destinationTablets[shardIndex], err = topo.GetTabletMap(scw.wr.TopoServer(), scw.destinationAliases[shardIndex])
		if err != nil {
			return fmt.Errorf("cannot read all target tablets in %v/%v: %v", si.Keyspace(), si.ShardName(), err)
		}

		// find and validate the master
		for tabletAlias, ti := range scw.destinationTablets[shardIndex] {
			if ti.Type == topo.TYPE_MASTER {
				if scw.destinationMasterAliases[shardIndex].IsZero() {
					scw.destinationMasterAliases[shardIndex] = tabletAlias
				} else {
					return fmt.Errorf("multiple masters in destination shard: %v and %v at least", scw.destinationMasterAliases[shardIndex], tabletAlias)
				}
			}
		}
		if scw.destinationMasterAliases[shardIndex].IsZero() {
			return fmt.Errorf("no master in destination shard")
		}
	}

	return nil
}

// copy phase:
// - get schema on the sources, filter tables
// - create tables on all destinations
// - copy the data
func (scw *SplitCloneWorker) copy() error {
	scw.setState(stateSCCopy)

	// get source schema from the first shard
	// TODO(alainjobart): for now, we assume the schema is compatible
	// on all source shards. Furthermore, we estimate the number of rows
	// in each source shard for each table to be about the same
	// (rowCount is used to estimate an ETA)
	sourceSchemaDefinition, err := scw.wr.GetSchema(scw.sourceAliases[0], nil, scw.excludeTables, true)
	if err != nil {
		return fmt.Errorf("cannot get schema from source %v: %v", scw.sourceAliases[0], err)
	}
	if len(sourceSchemaDefinition.TableDefinitions) == 0 {
		return fmt.Errorf("no tables matching the table filter in tablet %v", scw.sourceAliases[0])
	}
	scw.wr.Logger().Infof("Source tablet 0 has %v tables to copy", len(sourceSchemaDefinition.TableDefinitions))
	scw.mu.Lock()
	scw.tableStatus = make([]tableStatus, len(sourceSchemaDefinition.TableDefinitions))
	for i, td := range sourceSchemaDefinition.TableDefinitions {
		scw.tableStatus[i].name = td.Name
		scw.tableStatus[i].rowCount = td.RowCount * uint64(len(scw.sourceAliases))
	}
	scw.startTime = time.Now()
	scw.mu.Unlock()

	// Create all the commands to create the destination schema:
	// - createDbCmds will create the database and the tables
	// - createViewCmds will create the views
	// - alterTablesCmds will modify the tables at the end if needed
	// (all need template substitution for {{.DatabaseName}})
	createDbCmds := make([]string, 0, len(sourceSchemaDefinition.TableDefinitions)+1)
	createDbCmds = append(createDbCmds, sourceSchemaDefinition.DatabaseSchema)
	createViewCmds := make([]string, 0, 16)
	alterTablesCmds := make([]string, 0, 16)
	columnIndexes := make([]int, len(sourceSchemaDefinition.TableDefinitions))
	for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
		if td.Type == myproto.TABLE_BASE_TABLE {
			// build the create and alter statements
			create, alter, err := mysqlctl.MakeSplitCreateTableSql(td.Schema, "{{.DatabaseName}}", td.Name, scw.strategy)
			if err != nil {
				return fmt.Errorf("MakeSplitCreateTableSql(%v) returned: %v", td.Name, err)
			}
			createDbCmds = append(createDbCmds, create)
			if alter != "" {
				alterTablesCmds = append(alterTablesCmds, alter)
			}

			// find the column to split on
			columnIndexes[tableIndex] = -1
			for i, name := range td.Columns {
				if name == scw.keyspaceInfo.ShardingColumnName {
					columnIndexes[tableIndex] = i
					break
				}
			}
			if columnIndexes[tableIndex] == -1 {
				return fmt.Errorf("table %v doesn't have a column named '%v'", td.Name, scw.keyspaceInfo.ShardingColumnName)
			}

			scw.tableStatus[tableIndex].mu.Lock()
			scw.tableStatus[tableIndex].state = "before table creation"
			scw.tableStatus[tableIndex].rowCount = td.RowCount
			scw.tableStatus[tableIndex].mu.Unlock()
		} else {
			scw.tableStatus[tableIndex].mu.Lock()
			createViewCmds = append(createViewCmds, td.Schema)
			scw.tableStatus[tableIndex].state = "before view creation"
			scw.tableStatus[tableIndex].rowCount = 0
			scw.tableStatus[tableIndex].mu.Unlock()
		}
	}

	// For each destination tablet (in parallel):
	// - create the schema
	// - setup the channels to send SQL data chunks
	//
	// mu protects the abort channel for closing, and firstError
	mu := sync.Mutex{}
	abort := make(chan struct{})
	var firstError error

	processError := func(format string, args ...interface{}) {
		scw.wr.Logger().Errorf(format, args...)
		mu.Lock()
		if abort != nil {
			close(abort)
			abort = nil
			firstError = fmt.Errorf(format, args...)
		}
		mu.Unlock()
	}

	insertChannels := make([][]chan string, len(scw.destinationShards))
	destinationWaitGroup := sync.WaitGroup{}
	for shardIndex, _ := range scw.destinationShards {
		insertChannels[shardIndex] = make([]chan string, len(scw.destinationAliases[shardIndex]))
		for i, tabletAlias := range scw.destinationAliases[shardIndex] {
			// we create one channel per destination tablet.  It
			// is sized to have a buffer of a maximum of
			// destinationWriterCount * 2 items, to hopefully
			// always have data. We then have
			// destinationWriterCount go routines reading from it.
			insertChannels[shardIndex][i] = make(chan string, scw.destinationWriterCount*2)

			destinationWaitGroup.Add(1)
			go func(ti *topo.TabletInfo, insertChannel chan string) {
				defer destinationWaitGroup.Done()
				scw.wr.Logger().Infof("Creating tables on tablet %v", ti.Alias)
				if err := runSqlCommands(scw.wr, ti, createDbCmds, abort); err != nil {
					processError("createDbCmds failed: %v", err)
					return
				}
				if len(createViewCmds) > 0 {
					scw.wr.Logger().Infof("Creating views on tablet %v", ti.Alias)
					if err := runSqlCommands(scw.wr, ti, createViewCmds, abort); err != nil {
						processError("createViewCmds failed: %v", err)
						return
					}
				}
				for j := 0; j < scw.destinationWriterCount; j++ {
					destinationWaitGroup.Add(1)
					go func() {
						defer destinationWaitGroup.Done()
						if err := executeFetchLoop(scw.wr, ti, insertChannel, abort); err != nil {
							processError("executeFetchLoop failed: %v", err)
						}
					}()
				}
			}(scw.destinationTablets[shardIndex][tabletAlias], insertChannels[shardIndex][i])
		}
	}

	// Now for each table, read data chunks and send them to all
	// insertChannels
	sourceWaitGroup := sync.WaitGroup{}
	for shardIndex, _ := range scw.sourceShards {
		sema := sync2.NewSemaphore(scw.sourceReaderCount, 0)
		for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
			if td.Type == myproto.TABLE_VIEW {
				continue
			}

			rowSplitter := NewRowSplitter(scw.destinationShards, scw.keyspaceInfo.ShardingColumnType, columnIndexes[tableIndex])

			chunks, err := findChunks(scw.wr, scw.sourceTablets[shardIndex], td, scw.minTableSizeForSplit, scw.sourceReaderCount)
			if err != nil {
				return err
			}

			for chunkIndex := 0; chunkIndex < len(chunks)-1; chunkIndex++ {
				sourceWaitGroup.Add(1)
				go func(td *myproto.TableDefinition, tableIndex, chunkIndex int) {
					defer sourceWaitGroup.Done()

					sema.Acquire()
					defer sema.Release()

					// build the query, and start the streaming
					selectSQL := buildSQLFromChunks(scw.wr, td, chunks, chunkIndex, scw.sourceAliases[shardIndex].String())
					qrr, err := NewQueryResultReaderForTablet(scw.wr.TopoServer(), scw.sourceAliases[shardIndex], selectSQL)
					if err != nil {
						processError("NewQueryResultReaderForTablet failed: %v", err)
						return
					}

					// process the data
					if err := scw.processData(td, tableIndex, qrr, rowSplitter, insertChannels, abort); err != nil {
						processError("processData failed: %v", err)
					}
				}(td, tableIndex, chunkIndex)
			}
		}
	}
	sourceWaitGroup.Wait()

	for shardIndex, _ := range scw.destinationShards {
		for _, c := range insertChannels[shardIndex] {
			close(c)
		}
	}
	destinationWaitGroup.Wait()
	if firstError != nil {
		return firstError
	}

	// do the post-copy alters if any
	if len(alterTablesCmds) > 0 {
		for shardIndex, _ := range scw.destinationShards {
			for _, tabletAlias := range scw.destinationAliases[shardIndex] {
				destinationWaitGroup.Add(1)
				go func(ti *topo.TabletInfo) {
					defer destinationWaitGroup.Done()
					scw.wr.Logger().Infof("Altering tables on tablet %v", ti.Alias)
					if err := runSqlCommands(scw.wr, ti, alterTablesCmds, abort); err != nil {
						processError("alterTablesCmds failed on tablet %v: %v", ti.Alias, err)
					}
				}(scw.destinationTablets[shardIndex][tabletAlias])
			}
		}
		destinationWaitGroup.Wait()
		if firstError != nil {
			return firstError
		}
	}

	// then create and populate the blp_checkpoint table
	if strings.Index(scw.strategy, "populateBlpCheckpoint") != -1 {
		queries := make([]string, 0, 4)
		queries = append(queries, binlogplayer.CreateBlpCheckpoint()...)
		flags := ""
		if strings.Index(scw.strategy, "dontStartBinlogPlayer") != -1 {
			flags = binlogplayer.BLP_FLAG_DONT_START
		}

		// get the current position from the sources
		for shardIndex, _ := range scw.sourceShards {
			status, err := scw.wr.ActionInitiator().SlaveStatus(scw.sourceTablets[shardIndex], 30*time.Second)
			if err != nil {
				return err
			}

			queries = append(queries, binlogplayer.PopulateBlpCheckpoint(0, status.Position, time.Now().Unix(), flags))
		}

		for shardIndex, _ := range scw.destinationShards {
			for _, tabletAlias := range scw.destinationAliases[shardIndex] {
				destinationWaitGroup.Add(1)
				go func(ti *topo.TabletInfo) {
					defer destinationWaitGroup.Done()
					scw.wr.Logger().Infof("Making and populating blp_checkpoint table on tablet %v", ti.Alias)
					if err := runSqlCommands(scw.wr, ti, queries, abort); err != nil {
						processError("blp_checkpoint queries failed on tablet %v: %v", ti.Alias, err)
					}
				}(scw.destinationTablets[shardIndex][tabletAlias])
			}
		}
		destinationWaitGroup.Wait()
		if firstError != nil {
			return firstError
		}
	}

	// Now we're done with data copy, update the shard's source info.
	// TODO(alainjobart) this is a superset, some shards may not
	// overlap, have to deal with this better (for N -> M splits
	// where both N>1 and M>1)
	if strings.Index(scw.strategy, "skipSetSourceShards") != -1 {
		scw.wr.Logger().Infof("Skipping setting SourceShard on destination shards.")
	} else {
		for _, si := range scw.destinationShards {
			scw.wr.Logger().Infof("Setting SourceShard on shard %v/%v", si.Keyspace(), si.ShardName())
			if err := scw.wr.SetSourceShards(si.Keyspace(), si.ShardName(), scw.sourceAliases, nil); err != nil {
				return fmt.Errorf("Failed to set source shards: %v", err)
			}
		}
	}

	// And force a schema reload on all destination tablets.
	// The master tablet will end up starting filtered replication
	// at this point.
	for shardIndex, _ := range scw.destinationShards {
		for _, tabletAlias := range scw.destinationAliases[shardIndex] {
			destinationWaitGroup.Add(1)
			go func(ti *topo.TabletInfo) {
				defer destinationWaitGroup.Done()
				scw.wr.Logger().Infof("Reloading schema on tablet %v", ti.Alias)
				if err := scw.wr.ActionInitiator().ReloadSchema(ti, 30*time.Second); err != nil {
					processError("ReloadSchema failed on tablet %v: %v", ti.Alias, err)
				}
			}(scw.destinationTablets[shardIndex][tabletAlias])
		}
	}
	destinationWaitGroup.Wait()
	return firstError
}

// processData pumps the data out of the provided QueryResultReader.
// It returns any error the source encounters.
func (scw *SplitCloneWorker) processData(td *myproto.TableDefinition, tableIndex int, qrr *QueryResultReader, rowSplitter *RowSplitter, insertChannels [][]chan string, abort chan struct{}) error {
	baseCmd := td.Name + "(" + strings.Join(td.Columns, ", ") + ") VALUES "

	for {
		select {
		case r, ok := <-qrr.Output:
			if !ok {
				return qrr.Error()
			}

			// Split the rows by keyspace_id, and insert
			// each chunk into each destination
			sr, err := rowSplitter.Split(r.Rows)
			if err != nil {
				return fmt.Errorf("RowSplitter failed for table %v: %v", td.Name, err)
			}

			// send the rows to be inserted
			scw.tableStatus[tableIndex].addCopiedRows(len(r.Rows))
			for i, cs := range insertChannels {
				// one of the chunks might be empty, so no need
				// to send data in that case
				if len(sr[i]) > 0 {
					cmd := baseCmd + makeValueString(qrr.Fields, sr[i])
					for _, c := range cs {
						c <- cmd
					}
				}
			}
		case <-abort:
			// FIXME(alainjobart): note this select case
			// could be starved here, and we might miss
			// the abort in some corner cases.
			return nil
		}
	}
}
