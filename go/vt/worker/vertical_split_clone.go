// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"html/template"
	"strings"
	"sync"
	//	"time"

	log "github.com/golang/glog"
	//	"github.com/youtube/vitess/go/sync2"
	//	"github.com/youtube/vitess/go/vt/concurrency"
	//	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const (
	// all the states for the worker
	stateVSCNotSarted = "not started"
	stateVSCDone      = "done"
	stateVSCError     = "error"

	stateVSCInit = "initializing"
	//	stateVSCFindTargets            = "finding target instances"
	//	stateVSCSynchronizeReplication = "synchronizing replication"
	stateVSCCopy    = "copying the data"
	stateVSCCleanUp = "cleaning up"
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

	// populated during stateVSCFindTargets, read-only after that
	//	sourceAlias      topo.TabletAlias
	//	destinationAlias topo.TabletAlias

	// populated during stateVSCCopy
	copyLogs []string
	//	sourceSchemaDefinition      *myproto.SchemaDefinition
	//	destinationSchemaDefinition *myproto.SchemaDefinition
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

	// validate all serving types
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

	return nil
}
