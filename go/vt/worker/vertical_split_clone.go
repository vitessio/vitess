// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"html/template"
	"strconv"
	"strings"
	"sync"
	ttemplate "text/template"
	"time"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
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

type tableStatus struct {
	name     string
	rowCount uint64

	// all subsequent fields are protected by the mutex
	mu         sync.Mutex
	state      string
	copiedRows uint64
}

func (ts *tableStatus) setState(state string) {
	ts.mu.Lock()
	ts.state = state
	ts.mu.Unlock()
}

func (ts *tableStatus) addCopiedRows(copiedRows int) {
	ts.mu.Lock()
	ts.copiedRows += uint64(copiedRows)
	if ts.copiedRows == ts.rowCount {
		ts.state = "finished the copy"
	}
	ts.mu.Unlock()
}

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
	}
}

func (vscw *VerticalSplitCloneWorker) setState(state string) {
	vscw.mu.Lock()
	vscw.state = state
	vscw.mu.Unlock()
}

func (vscw *VerticalSplitCloneWorker) recordError(err error) {
	vscw.mu.Lock()
	vscw.state = stateVSCError
	vscw.err = err
	vscw.mu.Unlock()
}

func (vscw *VerticalSplitCloneWorker) tableStatuses() []string {
	result := make([]string, len(vscw.tableStatus))
	for i, ts := range vscw.tableStatus {
		ts.mu.Lock()
		if ts.rowCount > 0 {
			result[i] = fmt.Sprintf("%v: %v (%v/%v)", ts.name, ts.state, ts.copiedRows, ts.rowCount)
		} else {
			result[i] = fmt.Sprintf("%v: %v", ts.name, ts.state)
		}
		ts.mu.Unlock()
	}
	return result
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
		result += strings.Join(vscw.tableStatuses(), "</br>\n")
	case stateVSCDone:
		result += "<b>Success</b>:</br>\n"
		result += strings.Join(vscw.tableStatuses(), "</br>\n")
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
		result += strings.Join(vscw.tableStatuses(), "\n")
	case stateVSCDone:
		result += "Success:\n"
		result += strings.Join(vscw.tableStatuses(), "\n")
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
			log.Errorf("CleanUp failed in addition to job error: %v", cerr)
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
	if len(destinationKeyspaceInfo.ServedFrom) == 0 {
		return fmt.Errorf("destination keyspace %v has no ServedFrom", vscw.destinationKeyspace)
	}

	// validate all serving types, find sourceKeyspace
	servingTypes := []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY}
	servedFrom := ""
	for _, st := range servingTypes {
		if sf, ok := destinationKeyspaceInfo.ServedFrom[st]; !ok {
			return fmt.Errorf("destination keyspace %v is serving type %v", vscw.destinationKeyspace, st)
		} else {
			if servedFrom == "" {
				servedFrom = sf
			} else {
				if servedFrom != sf {
					return fmt.Errorf("destination keyspace %v is serving from multiple source keyspaces %v and %v", vscw.destinationKeyspace, servedFrom, sf)
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
	log.Infof("Using tablet %v as the source", vscw.sourceAlias)

	// get the tablet info for it
	vscw.sourceTablet, err = vscw.wr.TopoServer().GetTablet(vscw.sourceAlias)
	if err != nil {
		return fmt.Errorf("cannot read tablet %v: %v", vscw.sourceTablet, err)
	}

	// find all the targets in the destination keyspace / shard
	vscw.destinationAliases, err = topo.FindAllTabletAliasesInShard(vscw.wr.TopoServer(), vscw.destinationKeyspace, vscw.destinationShard)
	if err != nil {
		return fmt.Errorf("cannot find all target tablets in %v/%v: %v", vscw.destinationKeyspace, vscw.destinationShard, err)
	}
	log.Infof("Found %v target aliases", len(vscw.destinationAliases))

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
	log.Infof("Source tablet has %v tables to copy", len(sourceSchemaDefinition.TableDefinitions))
	vscw.mu.Lock()
	vscw.tableStatus = make([]tableStatus, len(sourceSchemaDefinition.TableDefinitions))
	for i, td := range sourceSchemaDefinition.TableDefinitions {
		vscw.tableStatus[i].name = td.Name
		vscw.tableStatus[i].rowCount = td.RowCount
	}
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
			create, alter, err := mysqlctl.MakeSplitCreateTableSql(td.Schema, "{{.DatabaseName}}", td.Name, vscw.strategy)
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
		log.Errorf(format, args...)
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
			log.Infof("Creating tables on tablet %v", ti.Alias)
			if err := vscw.runSqlCommands(ti, createDbCmds, abort); err != nil {
				processError("createDbCmds failed: %v", err)
				return
			}
			if len(createViewCmds) > 0 {
				log.Infof("Creating views on tablet %v", ti.Alias)
				if err := vscw.runSqlCommands(ti, createViewCmds, abort); err != nil {
					processError("createViewCmds failed: %v", err)
					return
				}
			}
			for j := 0; j < vscw.destinationWriterCount; j++ {
				destinationWaitGroup.Add(1)
				go func() {
					defer destinationWaitGroup.Done()
					for {
						select {
						case cmd, ok := <-insertChannel:
							if !ok {
								return
							}
							cmd = "INSERT INTO `" + ti.DbName() + "`." + cmd
							_, err := vscw.wr.ActionInitiator().ExecuteFetch(ti, cmd, 0, false, true, 30*time.Second)
							if err != nil {
								processError("ExecuteFetch failed: %v", err)
								return
							}
						case <-abort:
							return
						}
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
		chunks, err := vscw.findChunks(vscw.sourceTablet, td)
		if err != nil {
			return err
		}

		for chunkIndex := 0; chunkIndex < len(chunks)-1; chunkIndex++ {
			sourceWaitGroup.Add(1)
			go func(td myproto.TableDefinition, tableIndex, chunkIndex int) {
				defer sourceWaitGroup.Done()

				sema.Acquire()
				defer sema.Release()

				vscw.tableStatus[tableIndex].setState("started the copy")

				// build the query, and start the streaming
				selectSQL := "SELECT " + strings.Join(td.Columns, ", ") + " FROM " + td.Name
				if chunks[chunkIndex] != "" || chunks[chunkIndex+1] != "" {
					log.Infof("Starting to stream all data from table %v between '%v' and '%v'", td.Name, chunks[chunkIndex], chunks[chunkIndex+1])
					clauses := make([]string, 0, 2)
					if chunks[chunkIndex] != "" {
						clauses = append(clauses, td.PrimaryKeyColumns[0]+">="+chunks[chunkIndex])
					}
					if chunks[chunkIndex+1] != "" {
						clauses = append(clauses, td.PrimaryKeyColumns[0]+"<"+chunks[chunkIndex+1])
					}
					selectSQL += " WHERE " + strings.Join(clauses, " AND ")
				} else {
					log.Infof("Starting to stream all data from table %v", td.Name)
				}
				if len(td.PrimaryKeyColumns) > 0 {
					selectSQL += " ORDER BY " + strings.Join(td.PrimaryKeyColumns, ", ")
				}
				qrr, err := NewQueryResultReaderForTablet(vscw.wr.TopoServer(), vscw.sourceAlias, selectSQL)
				if err != nil {
					processError("NewQueryResultReaderForTablet failed: %v", err)
					return
				}

				// process the data
				baseCmd := td.Name + "(" + strings.Join(td.Columns, ", ") + ") VALUES "
			loop:
				for {
					select {
					case r, ok := <-qrr.Output:
						if !ok {
							if err := qrr.Error(); err != nil {
								// error case
								processError("QueryResultReader failed: %v", err)
								return
							}

							// we're done with the data
							break loop
						}

						// send the rows to be inserted
						vscw.tableStatus[tableIndex].addCopiedRows(len(r.Rows))
						cmd := baseCmd + makeValueString(qrr.Fields, r)
						for _, c := range insertChannels {
							c <- cmd
						}
					case <-abort:
						return
					}
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
				log.Infof("Altering tables on tablet %v", ti.Alias)
				if err := vscw.runSqlCommands(ti, alterTablesCmds, abort); err != nil {
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
		pos, err := vscw.wr.ActionInitiator().SlavePosition(vscw.sourceTablet, 30*time.Second)
		if err != nil {
			return err
		}

		queries := make([]string, 0, 4)
		queries = append(queries, binlogplayer.CreateBlpCheckpoint()...)
		queries = append(queries, binlogplayer.PopulateBlpCheckpoint(0, pos.MasterLogGroupId, time.Now().Unix()))
		for _, tabletAlias := range vscw.destinationAliases {
			destinationWaitGroup.Add(1)
			go func(ti *topo.TabletInfo) {
				defer destinationWaitGroup.Done()
				log.Infof("Making and populating blp_checkpoint table on tablet %v", ti.Alias)
				if err := vscw.runSqlCommands(ti, queries, abort); err != nil {
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
	log.Infof("Setting SourceShard on shard %v/%v", vscw.destinationKeyspace, vscw.destinationShard)
	if err := vscw.wr.SetSourceShards(vscw.destinationKeyspace, vscw.destinationShard, []topo.TabletAlias{vscw.sourceAlias}, vscw.tables); err != nil {
		return fmt.Errorf("Failed to set source shards: %v", err)
	}

	// And force a schema reload on all destination tablets.
	// The master tablet will end up starting filtered replication
	// at this point.
	for _, tabletAlias := range vscw.destinationAliases {
		destinationWaitGroup.Add(1)
		go func(ti *topo.TabletInfo) {
			defer destinationWaitGroup.Done()
			log.Infof("Reloading schema on tablet %v", ti.Alias)
			if err := vscw.wr.ActionInitiator().ReloadSchema(ti, 30*time.Second); err != nil {
				processError("ReloadSchema failed on tablet %v: %v", ti.Alias, err)
			}
		}(vscw.destinationTablets[tabletAlias])
	}
	destinationWaitGroup.Wait()
	return firstError
}

func (vscw *VerticalSplitCloneWorker) runSqlCommands(ti *topo.TabletInfo, commands []string, abort chan struct{}) error {
	for _, command := range commands {
		command, err := fillStringTemplate(command, map[string]string{"DatabaseName": ti.DbName()})
		if err != nil {
			return fmt.Errorf("fillStringTemplate failed: %v", err)
		}

		_, err = vscw.wr.ActionInitiator().ExecuteFetch(ti, command, 0, false, true, 30*time.Second)
		if err != nil {
			return err
		}

		// check on abort
		select {
		case <-abort:
			return nil
		default:
			break
		}
	}

	return nil
}

// findChunks returns an array of chunks to use for splitting up a table
// into multiple data chunks. It only works for tables with a primary key
// (and the primary key first column is an integer type).
// The array will always look like:
// "", "value1", "value2", ""
// A non-split tablet will just return:
// "", ""
func (vscw *VerticalSplitCloneWorker) findChunks(ti *topo.TabletInfo, td myproto.TableDefinition) ([]string, error) {
	result := []string{"", ""}

	// eliminate a few cases we don't split tables for
	if len(td.PrimaryKeyColumns) == 0 {
		// no primary key, what can we do?
		return result, nil
	}
	if td.DataLength < vscw.minTableSizeForSplit {
		// table is too small to split up
		return result, nil
	}

	// get the min and max of the leading column of the primary key
	query := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v.%v", td.PrimaryKeyColumns[0], td.PrimaryKeyColumns[0], ti.DbName(), td.Name)
	qr, err := vscw.wr.ActionInitiator().ExecuteFetch(ti, query, 1, true, false, 30*time.Second)
	if err != nil {
		log.Infof("Not splitting table %v into multiple chunks: %v", td.Name, err)
		return result, nil
	}
	if len(qr.Rows) != 1 {
		log.Infof("Not splitting table %v into multiple chunks, cannot get min and max", td.Name)
		return result, nil
	}
	if qr.Rows[0][0].IsNull() || qr.Rows[0][1].IsNull() {
		log.Infof("Not splitting table %v into multiple chunks, min or max is NULL: %v %v", td.Name, qr.Rows[0][0], qr.Rows[0][1])
		return result, nil
	}
	switch qr.Fields[0].Type {
	case mproto.VT_TINY, mproto.VT_SHORT, mproto.VT_LONG, mproto.VT_LONGLONG, mproto.VT_INT24:
		minNumeric := sqltypes.MakeNumeric(qr.Rows[0][0].Raw())
		maxNumeric := sqltypes.MakeNumeric(qr.Rows[0][1].Raw())
		if qr.Rows[0][0].Raw()[0] == '-' {
			// signed values, use int64
			min, err := minNumeric.ParseInt64()
			if err != nil {
				log.Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, minNumeric, err)
				return result, nil
			}
			max, err := maxNumeric.ParseInt64()
			if err != nil {
				log.Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, maxNumeric, err)
				return result, nil
			}
			interval := (max - min) / int64(vscw.sourceReaderCount)
			if interval == 0 {
				log.Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", max, min)
				return result, nil
			}

			result = make([]string, vscw.sourceReaderCount+1)
			result[0] = ""
			result[vscw.sourceReaderCount] = ""
			for i := int64(1); i < int64(vscw.sourceReaderCount); i++ {
				result[i] = fmt.Sprintf("%v", min+interval*i)
			}
			return result, nil
		}

		// unsigned values, use uint64
		min, err := minNumeric.ParseUint64()
		if err != nil {
			log.Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, minNumeric, err)
			return result, nil
		}
		max, err := maxNumeric.ParseUint64()
		if err != nil {
			log.Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, maxNumeric, err)
			return result, nil
		}
		interval := (max - min) / uint64(vscw.sourceReaderCount)
		if interval == 0 {
			log.Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", max, min)
			return result, nil
		}

		result = make([]string, vscw.sourceReaderCount+1)
		result[0] = ""
		result[vscw.sourceReaderCount] = ""
		for i := uint64(1); i < uint64(vscw.sourceReaderCount); i++ {
			result[i] = fmt.Sprintf("%v", min+interval*i)
		}
		return result, nil

	case mproto.VT_FLOAT, mproto.VT_DOUBLE:
		min, err := strconv.ParseFloat(qr.Rows[0][0].String(), 64)
		if err != nil {
			log.Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, qr.Rows[0][0], err)
			return result, nil
		}
		max, err := strconv.ParseFloat(qr.Rows[0][1].String(), 64)
		if err != nil {
			log.Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, qr.Rows[0][1].String(), err)
			return result, nil
		}
		interval := (max - min) / float64(vscw.sourceReaderCount)
		if interval == 0 {
			log.Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", max, min)
			return result, nil
		}

		result = make([]string, vscw.sourceReaderCount+1)
		result[0] = ""
		result[vscw.sourceReaderCount] = ""
		for i := 1; i < vscw.sourceReaderCount; i++ {
			result[i] = fmt.Sprintf("%v", min+interval*float64(i))
		}
		return result, nil
	}

	log.Infof("Not splitting table %v into multiple chunks, primary key not numeric", td.Name)
	return result, nil
}

func fillStringTemplate(tmpl string, vars interface{}) (string, error) {
	myTemplate := ttemplate.Must(ttemplate.New("").Parse(tmpl))
	data := new(bytes.Buffer)
	if err := myTemplate.Execute(data, vars); err != nil {
		return "", err
	}
	return data.String(), nil
}

func makeValueString(fields []mproto.Field, qr *mproto.QueryResult) string {
	buf := bytes.Buffer{}
	for i, row := range qr.Rows {
		if i > 0 {
			buf.Write([]byte(",("))
		} else {
			buf.WriteByte('(')
		}
		for j, value := range row {
			if j > 0 {
				buf.WriteByte(',')
			}
			// convert value back to its original type
			if !value.IsNull() {
				switch fields[j].Type {
				case mproto.VT_TINY, mproto.VT_SHORT, mproto.VT_LONG, mproto.VT_LONGLONG, mproto.VT_INT24:
					value = sqltypes.MakeNumeric(value.Raw())
				case mproto.VT_FLOAT, mproto.VT_DOUBLE:
					value = sqltypes.MakeFractional(value.Raw())
				}
			}
			value.EncodeSql(&buf)
		}
		buf.WriteByte(')')
	}
	return buf.String()
}
