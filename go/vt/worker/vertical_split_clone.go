// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"
	"sync"
	ttemplate "text/template"
	"time"

	log "github.com/golang/glog"
	//	"github.com/youtube/vitess/go/sync2"
	//	"github.com/youtube/vitess/go/vt/concurrency"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
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

// VerticalSplitCloneWorker will clone the data from a source keyspace/shard
// to a destination keyspace/shard.
type VerticalSplitCloneWorker struct {
	wr                  *wrangler.Wrangler
	cell                string
	destinationKeyspace string
	destinationShard    string
	tables              []string
	strategy            string
	cleaner             *wrangler.Cleaner

	// all subsequent fields are protected by the mutex
	mu    sync.Mutex
	state string

	// populated if state == stateVSCError
	err error

	// populated during stateVSCInit, read-only after that
	destinationKeyspaceInfo *topo.KeyspaceInfo
	sourceKeyspace          string

	// populated during stateVSCFindTargets, read-only after that
	sourceAlias        topo.TabletAlias
	destinationAliases []topo.TabletAlias
	destinationTablets map[topo.TabletAlias]*topo.TabletInfo

	// populated during stateVSCCopy
	copyLogs               []string
	sourceSchemaDefinition *myproto.SchemaDefinition
}

// NewVerticalSplitCloneWorker returns a new VerticalSplitCloneWorker object.
func NewVerticalSplitCloneWorker(wr *wrangler.Wrangler, cell, destinationKeyspace, destinationShard string, tables []string, strategy string) Worker {
	return &VerticalSplitCloneWorker{
		wr:                  wr,
		cell:                cell,
		destinationKeyspace: destinationKeyspace,
		destinationShard:    destinationShard,
		tables:              tables,
		strategy:            strategy,
		cleaner:             &wrangler.Cleaner{},

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
		result += strings.Join(vscw.copyLogs, "</br>\n")
	case stateVSCDone:
		result += "<b>Success</b>:</br>\n"
		result += strings.Join(vscw.copyLogs, "</br>\n")
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
		result += strings.Join(vscw.copyLogs, "\n")
	case stateVSCDone:
		result += "Success:\n"
		result += strings.Join(vscw.copyLogs, "\n")
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

	var err error

	// read the keyspace and validate it
	vscw.destinationKeyspaceInfo, err = vscw.wr.TopoServer().GetKeyspace(vscw.destinationKeyspace)
	if err != nil {
		return fmt.Errorf("cannot read destination keyspace %v: %v", vscw.destinationKeyspace, err)
	}
	if len(vscw.destinationKeyspaceInfo.ServedFrom) == 0 {
		return fmt.Errorf("destination keyspace %v has no ServedFrom", vscw.destinationKeyspace)
	}

	// validate all serving types, find sourceKeyspace
	servingTypes := []topo.TabletType{topo.TYPE_MASTER, topo.TYPE_REPLICA, topo.TYPE_RDONLY}
	servedFrom := ""
	for _, st := range servingTypes {
		if sf, ok := vscw.destinationKeyspaceInfo.ServedFrom[st]; !ok {
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

	return nil
}

// copy phase:
// - get schema on the source, filter tables
// - create tables on all destinations
// - copy the data
func (vscw *VerticalSplitCloneWorker) copy() error {
	vscw.setState(stateVSCCopy)

	// get source schema
	var err error
	vscw.sourceSchemaDefinition, err = vscw.wr.GetSchema(vscw.sourceAlias, vscw.tables, nil, true)
	if err != nil {
		return fmt.Errorf("cannot get schema from source %v: %v", vscw.sourceAlias, err)
	}
	if len(vscw.sourceSchemaDefinition.TableDefinitions) == 0 {
		return fmt.Errorf("no tables matching the table filter")
	}
	log.Infof("Source tablet has %v tables to copy", len(vscw.sourceSchemaDefinition.TableDefinitions))

	// Create all the commands to create the destination schema:
	// - createDbCmds will create the database and the tables
	// - createViewCmds will create the views
	// - alterTablesCmds will modify the tables at the end if needed
	// (all need template substitution for {{.DatabaseName}})
	createDbCmds := make([]string, 0, len(vscw.sourceSchemaDefinition.TableDefinitions)+1)
	createDbCmds = append(createDbCmds, vscw.sourceSchemaDefinition.DatabaseSchema)
	createViewCmds := make([]string, 0, 16)
	alterTablesCmds := make([]string, 0, 16)
	for _, td := range vscw.sourceSchemaDefinition.TableDefinitions {
		if td.Type == myproto.TABLE_BASE_TABLE {
			create, alter, err := mysqlctl.MakeSplitCreateTableSql(td.Schema, "{{.DatabaseName}}", td.Name, vscw.strategy)
			if err != nil {
				return fmt.Errorf("MakeSplitCreateTableSql(%v) returned: %v", td.Name, err)
			}
			createDbCmds = append(createDbCmds, create)
			if alter != "" {
				alterTablesCmds = append(alterTablesCmds, alter)
			}
		} else {
			createViewCmds = append(createViewCmds, td.Schema)
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
		insertChannels[i] = make(chan string, 20)

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
			for j := 0; j < 20; j++ {
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
	for _, td := range vscw.sourceSchemaDefinition.TableDefinitions {
		sourceWaitGroup.Add(1)
		go func(td myproto.TableDefinition) {
			defer sourceWaitGroup.Done()

			// build the query, and start the streaming
			log.Infof("Starting to stream data from table %v", td.Name)
			selectSQL := "SELECT " + strings.Join(td.Columns, ", ") + " FROM " + td.Name
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
					cmd := baseCmd + makeValueString(qrr.Fields, r)
					for _, c := range insertChannels {
						c <- cmd
					}
				case <-abort:
					return
				}
			}
		}(td)
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
	if len(alterTablesCmds) == 0 {
		return nil
	}
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
