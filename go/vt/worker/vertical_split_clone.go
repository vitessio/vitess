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

	"code.google.com/p/go.net/context"

	"github.com/youtube/vitess/go/event"
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
	strategy               string
	sourceReaderCount      int
	minTableSizeForSplit   uint64
	destinationWriterCount int
	cleaner                *wrangler.Cleaner

	// all subsequent fields are protected by the mutex
	mu    sync.Mutex
	state string

	// populated if state == stateVSCError
	err error

	// populated during stateVSCInit, read-only after that
	sourceKeyspace string

	// populated during stateVSCFindTargets, read-only after that
	sourceAlias            topo.TabletAlias
	sourceTablet           *topo.TabletInfo
	destinationAliases     []topo.TabletAlias
	destinationTablets     map[topo.TabletAlias]*topo.TabletInfo
	destinationMasterAlias topo.TabletAlias

	// populated during stateVSCCopy
	tableStatus []tableStatus
	startTime   time.Time

	ev *events.VerticalSplitClone
}

// NewVerticalSplitCloneWorker returns a new VerticalSplitCloneWorker object.
func NewVerticalSplitCloneWorker(wr *wrangler.Wrangler, cell, destinationKeyspace, destinationShard string, tables []string, strategy string, sourceReaderCount int, minTableSizeForSplit uint64, destinationWriterCount int) Worker {
	return &VerticalSplitCloneWorker{
		wr:                     wr,
		cell:                   cell,
		destinationKeyspace:    destinationKeyspace,
		destinationShard:       destinationShard,
		tables:                 tables,
		strategy:               strategy,
		sourceReaderCount:      sourceReaderCount,
		minTableSizeForSplit:   minTableSizeForSplit,
		destinationWriterCount: destinationWriterCount,
		cleaner:                &wrangler.Cleaner{},

		state: stateVSCNotSarted,
		ev: &events.VerticalSplitClone{
			Cell:     cell,
			Keyspace: destinationKeyspace,
			Shard:    destinationShard,
			Tables:   tables,
			Strategy: strategy,
		},
	}
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

func (vscw *VerticalSplitCloneWorker) CheckInterrupted() bool {
	select {
	case <-interrupted:
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
	if vscw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// second state: find targets
	if err := vscw.findTargets(); err != nil {
		return fmt.Errorf("findTargets() failed: %v", err)
	}
	if vscw.CheckInterrupted() {
		return topo.ErrInterrupted
	}

	// third state: copy data
	if err := vscw.copy(); err != nil {
		return fmt.Errorf("copy() failed: %v", err)
	}
	if vscw.CheckInterrupted() {
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
		if sf, ok := destinationKeyspaceInfo.ServedFromMap[st]; !ok {
			return fmt.Errorf("destination keyspace %v is serving type %v", vscw.destinationKeyspace, st)
		} else {
			if servedFrom == "" {
				servedFrom = sf.Keyspace
			} else {
				if servedFrom != sf.Keyspace {
					return fmt.Errorf("destination keyspace %v is serving from multiple source keyspaces %v and %v", vscw.destinationKeyspace, servedFrom, sf.Keyspace)
				}
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
	vscw.sourceAlias, err = findChecker(vscw.wr, vscw.cleaner, vscw.cell, vscw.sourceKeyspace, "0")
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
	if err := vscw.wr.TabletManagerClient().StopSlave(context.TODO(), vscw.sourceTablet, 30*time.Second); err != nil {
		return fmt.Errorf("cannot stop replication on tablet %v", vscw.sourceAlias)
	}

	wrangler.RecordStartSlaveAction(vscw.cleaner, vscw.sourceTablet, 30*time.Second)
	action, err := wrangler.FindChangeSlaveTypeActionByTarget(vscw.cleaner, vscw.sourceAlias)
	if err != nil {
		return fmt.Errorf("cannot find ChangeSlaveType action for %v: %v", vscw.sourceAlias, err)
	}
	action.TabletType = topo.TYPE_SPARE

	// find all the targets in the destination keyspace / shard
	vscw.destinationAliases, err = topo.FindAllTabletAliasesInShard(vscw.wr.TopoServer(), vscw.destinationKeyspace, vscw.destinationShard)
	if err != nil {
		return fmt.Errorf("cannot find all target tablets in %v/%v: %v", vscw.destinationKeyspace, vscw.destinationShard, err)
	}
	vscw.wr.Logger().Infof("Found %v target aliases", len(vscw.destinationAliases))

	// get the TabletInfo for all targets
	vscw.destinationTablets, err = topo.GetTabletMap(vscw.wr.TopoServer(), vscw.destinationAliases)
	if err != nil {
		return fmt.Errorf("cannot read all target tablets in %v/%v: %v", vscw.destinationKeyspace, vscw.destinationShard, err)
	}

	// find and validate the master
	for tabletAlias, ti := range vscw.destinationTablets {
		if ti.Type == topo.TYPE_MASTER {
			if vscw.destinationMasterAlias.IsZero() {
				vscw.destinationMasterAlias = tabletAlias
			} else {
				return fmt.Errorf("multiple masters in destination shard: %v and %v at least", vscw.destinationMasterAlias, tabletAlias)
			}
		}
	}
	if vscw.destinationMasterAlias.IsZero() {
		return fmt.Errorf("no master in destination shard")
	}

	return nil
}

// copy phase:
// - get schema on the source, filter tables
// - create tables on all destinations
// - copy the data
func (vscw *VerticalSplitCloneWorker) copy() error {
	vscw.setState(stateVSCCopy)

	// get source schema
	sourceSchemaDefinition, err := vscw.wr.GetSchema(vscw.sourceAlias, vscw.tables, nil, true)
	if err != nil {
		return fmt.Errorf("cannot get schema from source %v: %v", vscw.sourceAlias, err)
	}
	if len(sourceSchemaDefinition.TableDefinitions) == 0 {
		return fmt.Errorf("no tables matching the table filter")
	}
	vscw.wr.Logger().Infof("Source tablet has %v tables to copy", len(sourceSchemaDefinition.TableDefinitions))
	vscw.mu.Lock()
	vscw.tableStatus = make([]tableStatus, len(sourceSchemaDefinition.TableDefinitions))
	for i, td := range sourceSchemaDefinition.TableDefinitions {
		vscw.tableStatus[i].name = td.Name
		vscw.tableStatus[i].rowCount = td.RowCount
	}
	vscw.startTime = time.Now()
	vscw.mu.Unlock()

	// Create all the commands to create the destination schema:
	// - createDbCmds will create the database and the tables
	// - createViewCmds will create the views
	// - alterTablesCmds will modify the tables at the end if needed
	// (all need template substitution for {{.DatabaseName}})
	createDbCmds := make([]string, 0, len(sourceSchemaDefinition.TableDefinitions)+1)
	createDbCmds = append(createDbCmds, sourceSchemaDefinition.DatabaseSchema)
	createViewCmds := make([]string, 0, 16)
	alterTablesCmds := make([]string, 0, 16)
	for i, td := range sourceSchemaDefinition.TableDefinitions {
		vscw.tableStatus[i].mu.Lock()
		if td.Type == myproto.TABLE_BASE_TABLE {
			create, alter, err := mysqlctl.MakeSplitCreateTableSql(vscw.wr.Logger(), td.Schema, "{{.DatabaseName}}", td.Name, vscw.strategy)
			if err != nil {
				return fmt.Errorf("MakeSplitCreateTableSql(%v) returned: %v", td.Name, err)
			}
			createDbCmds = append(createDbCmds, create)
			if alter != "" {
				alterTablesCmds = append(alterTablesCmds, alter)
			}
			vscw.tableStatus[i].state = "before table creation"
			vscw.tableStatus[i].rowCount = td.RowCount
		} else {
			createViewCmds = append(createViewCmds, td.Schema)
			vscw.tableStatus[i].state = "before view creation"
			vscw.tableStatus[i].rowCount = 0
		}
		vscw.tableStatus[i].mu.Unlock()
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
		vscw.wr.Logger().Errorf(format, args...)
		mu.Lock()
		if abort != nil {
			close(abort)
			abort = nil
			firstError = fmt.Errorf(format, args...)
		}
		mu.Unlock()
	}

	insertChannels := make([]chan string, len(vscw.destinationAliases))
	destinationWaitGroup := sync.WaitGroup{}
	for i, tabletAlias := range vscw.destinationAliases {
		// we create one channel per destination tablet.  It
		// is sized to have a buffer of a maximum of
		// destinationWriterCount * 2 items, to hopefully
		// always have data. We then have
		// destinationWriterCount go routines reading from it.
		insertChannels[i] = make(chan string, vscw.destinationWriterCount*2)

		destinationWaitGroup.Add(1)
		go func(ti *topo.TabletInfo, insertChannel chan string) {
			defer destinationWaitGroup.Done()
			vscw.wr.Logger().Infof("Creating tables on tablet %v", ti.Alias)
			if err := runSqlCommands(vscw.wr, ti, createDbCmds, abort); err != nil {
				processError("createDbCmds failed: %v", err)
				return
			}
			if len(createViewCmds) > 0 {
				vscw.wr.Logger().Infof("Creating views on tablet %v", ti.Alias)
				if err := runSqlCommands(vscw.wr, ti, createViewCmds, abort); err != nil {
					processError("createViewCmds failed: %v", err)
					return
				}
			}
			for j := 0; j < vscw.destinationWriterCount; j++ {
				destinationWaitGroup.Add(1)
				go func() {
					defer destinationWaitGroup.Done()

					if err := executeFetchLoop(vscw.wr, ti, insertChannel, abort); err != nil {
						processError("executeFetchLoop failed: %v", err)
					}
				}()
			}
		}(vscw.destinationTablets[tabletAlias], insertChannels[i])
	}

	// Now for each table, read data chunks and send them to all
	// insertChannels
	sourceWaitGroup := sync.WaitGroup{}
	sema := sync2.NewSemaphore(vscw.sourceReaderCount, 0)
	for tableIndex, td := range sourceSchemaDefinition.TableDefinitions {
		if td.Type == myproto.TABLE_VIEW {
			vscw.tableStatus[tableIndex].setState("view created")
			continue
		}

		vscw.tableStatus[tableIndex].setState("before copy")
		chunks, err := findChunks(vscw.wr, vscw.sourceTablet, td, vscw.minTableSizeForSplit, vscw.sourceReaderCount)
		if err != nil {
			return err
		}

		for chunkIndex := 0; chunkIndex < len(chunks)-1; chunkIndex++ {
			sourceWaitGroup.Add(1)
			go func(td *myproto.TableDefinition, tableIndex, chunkIndex int) {
				defer sourceWaitGroup.Done()

				sema.Acquire()
				defer sema.Release()

				vscw.tableStatus[tableIndex].setState("started the copy")

				// build the query, and start the streaming
				selectSQL := buildSQLFromChunks(vscw.wr, td, chunks, chunkIndex, vscw.sourceAlias.String())
				qrr, err := NewQueryResultReaderForTablet(vscw.wr.TopoServer(), vscw.sourceAlias, selectSQL)
				if err != nil {
					processError("NewQueryResultReaderForTablet failed: %v", err)
					return
				}

				// process the data
				if err := vscw.processData(td, tableIndex, qrr, insertChannels, abort); err != nil {
					processError("QueryResultReader failed: %v", err)
				}
			}(td, tableIndex, chunkIndex)
		}
	}
	sourceWaitGroup.Wait()

	for _, c := range insertChannels {
		close(c)
	}
	destinationWaitGroup.Wait()
	if firstError != nil {
		return firstError
	}

	// do the post-copy alters if any
	if len(alterTablesCmds) > 0 {
		for _, tabletAlias := range vscw.destinationAliases {
			destinationWaitGroup.Add(1)
			go func(ti *topo.TabletInfo) {
				defer destinationWaitGroup.Done()
				vscw.wr.Logger().Infof("Altering tables on tablet %v", ti.Alias)
				if err := runSqlCommands(vscw.wr, ti, alterTablesCmds, abort); err != nil {
					processError("alterTablesCmds failed on tablet %v: %v", ti.Alias, err)
				}
			}(vscw.destinationTablets[tabletAlias])
		}
		destinationWaitGroup.Wait()
		if firstError != nil {
			return firstError
		}
	}

	// then create and populate the blp_checkpoint table
	if strings.Index(vscw.strategy, "populateBlpCheckpoint") != -1 {
		// get the current position from the source
		status, err := vscw.wr.TabletManagerClient().SlaveStatus(context.TODO(), vscw.sourceTablet, 30*time.Second)
		if err != nil {
			return err
		}

		queries := make([]string, 0, 4)
		queries = append(queries, binlogplayer.CreateBlpCheckpoint()...)
		flags := ""
		if strings.Index(vscw.strategy, "dontStartBinlogPlayer") != -1 {
			flags = binlogplayer.BLP_FLAG_DONT_START
		}
		queries = append(queries, binlogplayer.PopulateBlpCheckpoint(0, status.Position, time.Now().Unix(), flags))
		for _, tabletAlias := range vscw.destinationAliases {
			destinationWaitGroup.Add(1)
			go func(ti *topo.TabletInfo) {
				defer destinationWaitGroup.Done()
				vscw.wr.Logger().Infof("Making and populating blp_checkpoint table on tablet %v", ti.Alias)
				if err := runSqlCommands(vscw.wr, ti, queries, abort); err != nil {
					processError("blp_checkpoint queries failed on tablet %v: %v", ti.Alias, err)
				}
			}(vscw.destinationTablets[tabletAlias])
		}
		destinationWaitGroup.Wait()
		if firstError != nil {
			return firstError
		}
	}

	// Now we're done with data copy, update the shard's source info.
	if strings.Index(vscw.strategy, "skipSetSourceShards") != -1 {
		vscw.wr.Logger().Infof("Skipping setting SourceShard on destination shard.")
	} else {
		vscw.wr.Logger().Infof("Setting SourceShard on shard %v/%v", vscw.destinationKeyspace, vscw.destinationShard)
		if err := vscw.wr.SetSourceShards(vscw.destinationKeyspace, vscw.destinationShard, []topo.TabletAlias{vscw.sourceAlias}, vscw.tables); err != nil {
			return fmt.Errorf("Failed to set source shards: %v", err)
		}
	}

	// And force a schema reload on all destination tablets.
	// The master tablet will end up starting filtered replication
	// at this point.
	for _, tabletAlias := range vscw.destinationAliases {
		destinationWaitGroup.Add(1)
		go func(ti *topo.TabletInfo) {
			defer destinationWaitGroup.Done()
			vscw.wr.Logger().Infof("Reloading schema on tablet %v", ti.Alias)
			if err := vscw.wr.TabletManagerClient().ReloadSchema(context.TODO(), ti, 30*time.Second); err != nil {
				processError("ReloadSchema failed on tablet %v: %v", ti.Alias, err)
			}
		}(vscw.destinationTablets[tabletAlias])
	}
	destinationWaitGroup.Wait()
	return firstError
}

// processData pumps the data out of the provided QueryResultReader.
// It returns any error the source encounters.
func (vscw *VerticalSplitCloneWorker) processData(td *myproto.TableDefinition, tableIndex int, qrr *QueryResultReader, insertChannels []chan string, abort chan struct{}) error {
	// process the data
	baseCmd := td.Name + "(" + strings.Join(td.Columns, ", ") + ") VALUES "
	for {
		select {
		case r, ok := <-qrr.Output:
			if !ok {
				return qrr.Error()
			}

			// send the rows to be inserted
			vscw.tableStatus[tableIndex].addCopiedRows(len(r.Rows))
			cmd := baseCmd + makeValueString(qrr.Fields, r.Rows)
			for _, c := range insertChannels {
				select {
				case c <- cmd:
				case <-abort:
					return nil
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
