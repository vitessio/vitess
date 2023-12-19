/*
Copyright 2019 The Vitess Authors.

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

package binlog

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/tb"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

/* API and config for UpdateStream Service */

const (
	usDisabled int64 = iota
	usEnabled
)

var usStateNames = map[int64]string{
	usEnabled:  "Enabled",
	usDisabled: "Disabled",
}

var (
	streamCount          = stats.NewCountersWithSingleLabel("UpdateStreamStreamCount", "update stream count", "type")
	updateStreamErrors   = stats.NewCountersWithSingleLabel("UpdateStreamErrors", "update stream error count", "type")
	keyrangeStatements   = stats.NewCounter("UpdateStreamKeyRangeStatements", "update stream key range statement count")
	keyrangeTransactions = stats.NewCounter("UpdateStreamKeyRangeTransactions", "update stream key range transaction count")
	tablesStatements     = stats.NewCounter("UpdateStreamTablesStatements", "update stream table statement count")
	tablesTransactions   = stats.NewCounter("UpdateStreamTablesTransactions", "update stream table transaction count")
)

// UpdateStreamControl is the interface an UpdateStream service implements
// to bring it up or down.
type UpdateStreamControl interface {
	// InitDBConfigs is called after the db name is computed.
	InitDBConfig(*dbconfigs.DBConfigs)
	// RegisterService registers the UpdateStream service.
	RegisterService()
	// Enable will allow any new RPC calls
	Enable()
	// Disable will interrupt all current calls, and disallow any new call
	Disable()
	// IsEnabled returns true iff the service is enabled
	IsEnabled() bool
}

// UpdateStreamImpl is the real implementation of UpdateStream
// and UpdateStreamControl
type UpdateStreamImpl struct {
	// the following variables are set at construction time
	ts       *topo.Server
	keyspace string
	cell     string
	cp       dbconfigs.Connector
	se       *schema.Engine

	// actionLock protects the following variables
	actionLock     sync.Mutex
	state          atomic.Int64
	stateWaitGroup sync.WaitGroup
	streams        StreamList
	parser         *sqlparser.Parser
}

// StreamList is a map of context.CancelFunc to mass-interrupt ongoing
// calls.
type StreamList struct {
	sync.Mutex
	currentIndex int
	streams      map[int]context.CancelFunc
}

// Init must be called before using the list.
func (sl *StreamList) Init() {
	sl.Lock()
	sl.streams = make(map[int]context.CancelFunc)
	sl.currentIndex = 0
	sl.Unlock()
}

// Add adds a CancelFunc to the map.
func (sl *StreamList) Add(c context.CancelFunc) int {
	sl.Lock()
	defer sl.Unlock()

	sl.currentIndex++
	sl.streams[sl.currentIndex] = c
	return sl.currentIndex
}

// Delete removes a CancelFunc from the list.
func (sl *StreamList) Delete(i int) {
	sl.Lock()
	delete(sl.streams, i)
	sl.Unlock()
}

// Stop stops all the current streams.
func (sl *StreamList) Stop() {
	sl.Lock()
	for _, c := range sl.streams {
		c()
	}
	sl.Unlock()
}

// RegisterUpdateStreamServiceFunc is the type to use for delayed
// registration of RPC servers until we have all the objects
type RegisterUpdateStreamServiceFunc func(UpdateStream)

// RegisterUpdateStreamServices is the list of all registration
// callbacks to invoke
var RegisterUpdateStreamServices []RegisterUpdateStreamServiceFunc

// NewUpdateStream returns a new UpdateStreamImpl object
func NewUpdateStream(ts *topo.Server, keyspace string, cell string, se *schema.Engine, parser *sqlparser.Parser) *UpdateStreamImpl {
	return &UpdateStreamImpl{
		ts:       ts,
		keyspace: keyspace,
		cell:     cell,
		se:       se,
		parser:   parser,
	}
}

// InitDBConfig should be invoked after the db name is computed.
func (updateStream *UpdateStreamImpl) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	updateStream.cp = dbcfgs.DbaWithDB()
}

// RegisterService needs to be called to publish stats, and to start listening
// to clients. Only once instance can call this in a process.
func (updateStream *UpdateStreamImpl) RegisterService() {
	// publish the stats
	stats.Publish("UpdateStreamState", stats.StringFunc(func() string {
		return usStateNames[updateStream.state.Load()]
	}))

	// and register all the RPC protocols
	for _, f := range RegisterUpdateStreamServices {
		f(updateStream)
	}
}

func logError() {
	if x := recover(); x != nil {
		log.Errorf("%s at\n%s", x.(error).Error(), tb.Stack(4))
	}
}

// Enable will allow connections to the service
func (updateStream *UpdateStreamImpl) Enable() {
	defer logError()
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if updateStream.IsEnabled() {
		return
	}

	updateStream.state.Store(usEnabled)
	updateStream.streams.Init()
	log.Infof("Enabling update stream, dbname: %s", updateStream.cp.DBName())
}

// Disable will disallow any connection to the service
func (updateStream *UpdateStreamImpl) Disable() {
	defer logError()
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if !updateStream.IsEnabled() {
		return
	}

	updateStream.state.Store(usDisabled)
	updateStream.streams.Stop()
	updateStream.stateWaitGroup.Wait()
	log.Infof("Update Stream Disabled")
}

// IsEnabled returns true if UpdateStreamImpl is enabled
func (updateStream *UpdateStreamImpl) IsEnabled() bool {
	return updateStream.state.Load() == usEnabled
}

// StreamKeyRange is part of the UpdateStream interface
func (updateStream *UpdateStreamImpl) StreamKeyRange(ctx context.Context, position string, keyRange *topodatapb.KeyRange, charset *binlogdatapb.Charset, callback func(trans *binlogdatapb.BinlogTransaction) error) (err error) {
	pos, err := replication.DecodePosition(position)
	if err != nil {
		return err
	}

	updateStream.actionLock.Lock()
	if !updateStream.IsEnabled() {
		updateStream.actionLock.Unlock()
		log.Errorf("Unable to serve client request: Update stream service is not enabled")
		return fmt.Errorf("update stream service is not enabled")
	}
	updateStream.stateWaitGroup.Add(1)
	updateStream.actionLock.Unlock()
	defer updateStream.stateWaitGroup.Done()

	streamCount.Add("KeyRange", 1)
	defer streamCount.Add("KeyRange", -1)
	log.Infof("ServeUpdateStream starting @ %#v", pos)

	// Calls cascade like this: binlog.Streamer->keyRangeFilterFunc->func(*binlogdatapb.BinlogTransaction)->callback
	f := keyRangeFilterFunc(keyRange, func(trans *binlogdatapb.BinlogTransaction) error {
		keyrangeStatements.Add(int64(len(trans.Statements)))
		keyrangeTransactions.Add(1)
		return callback(trans)
	})
	bls := NewStreamer(updateStream.cp, updateStream.se, charset, pos, 0, f)
	bls.resolverFactory, err = newKeyspaceIDResolverFactory(ctx, updateStream.ts, updateStream.keyspace, updateStream.cell, updateStream.parser)
	if err != nil {
		return fmt.Errorf("newKeyspaceIDResolverFactory failed: %v", err)
	}

	streamCtx, cancel := context.WithCancel(ctx)
	i := updateStream.streams.Add(cancel)
	defer updateStream.streams.Delete(i)

	return bls.Stream(streamCtx)
}

// StreamTables is part of the UpdateStream interface
func (updateStream *UpdateStreamImpl) StreamTables(ctx context.Context, position string, tables []string, charset *binlogdatapb.Charset, callback func(trans *binlogdatapb.BinlogTransaction) error) (err error) {
	pos, err := replication.DecodePosition(position)
	if err != nil {
		return err
	}

	updateStream.actionLock.Lock()
	if !updateStream.IsEnabled() {
		updateStream.actionLock.Unlock()
		log.Errorf("Unable to serve client request: Update stream service is not enabled")
		return fmt.Errorf("update stream service is not enabled")
	}
	updateStream.stateWaitGroup.Add(1)
	updateStream.actionLock.Unlock()
	defer updateStream.stateWaitGroup.Done()

	streamCount.Add("Tables", 1)
	defer streamCount.Add("Tables", -1)
	log.Infof("ServeUpdateStream starting @ %#v", pos)

	// Calls cascade like this: binlog.Streamer->tablesFilterFunc->func(*binlogdatapb.BinlogTransaction)->callback
	f := tablesFilterFunc(tables, func(trans *binlogdatapb.BinlogTransaction) error {
		tablesStatements.Add(int64(len(trans.Statements)))
		tablesTransactions.Add(1)
		return callback(trans)
	})
	bls := NewStreamer(updateStream.cp, updateStream.se, charset, pos, 0, f)

	streamCtx, cancel := context.WithCancel(ctx)
	i := updateStream.streams.Add(cancel)
	defer updateStream.streams.Delete(i)

	return bls.Stream(streamCtx)
}

// HandlePanic is part of the UpdateStream interface
func (updateStream *UpdateStreamImpl) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}
