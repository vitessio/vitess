// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	streamCount          = stats.NewCounters("UpdateStreamStreamCount")
	updateStreamErrors   = stats.NewCounters("UpdateStreamErrors")
	keyrangeStatements   = stats.NewInt("UpdateStreamKeyRangeStatements")
	keyrangeTransactions = stats.NewInt("UpdateStreamKeyRangeTransactions")
	tablesStatements     = stats.NewInt("UpdateStreamTablesStatements")
	tablesTransactions   = stats.NewInt("UpdateStreamTablesTransactions")
)

// UpdateStreamControl is the interface an UpdateStream service implements
// to bring it up or down.
type UpdateStreamControl interface {
	// Enable will allow any new RPC calls
	Enable()

	// Disable will interrupt all current calls, and disallow any new call
	Disable()

	// IsEnabled returns true iff the service is enabled
	IsEnabled() bool
}

// UpdateStreamControlMock is an implementation of UpdateStreamControl
// to be used in tests
type UpdateStreamControlMock struct {
	enabled bool
	sync.Mutex
}

// NewUpdateStreamControlMock creates a new UpdateStreamControlMock
func NewUpdateStreamControlMock() *UpdateStreamControlMock {
	return &UpdateStreamControlMock{}
}

// Enable is part of UpdateStreamControl
func (m *UpdateStreamControlMock) Enable() {
	m.Lock()
	m.enabled = true
	m.Unlock()
}

// Disable is part of UpdateStreamControl
func (m *UpdateStreamControlMock) Disable() {
	m.Lock()
	m.enabled = false
	m.Unlock()
}

// IsEnabled is part of UpdateStreamControl
func (m *UpdateStreamControlMock) IsEnabled() bool {
	m.Lock()
	defer m.Unlock()
	return m.enabled
}

// UpdateStreamImpl is the real implementation of UpdateStream
// and UpdateStreamControl
type UpdateStreamImpl struct {
	// the following variables are set at construction time
	ts       topo.Server
	keyspace string
	cell     string
	mysqld   mysqlctl.MysqlDaemon
	dbname   string
	se       *schema.Engine

	// actionLock protects the following variables
	actionLock     sync.Mutex
	state          sync2.AtomicInt64
	stateWaitGroup sync.WaitGroup
	streams        StreamList
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
func NewUpdateStream(ts topo.Server, keyspace string, cell string, mysqld mysqlctl.MysqlDaemon, se *schema.Engine, dbname string) *UpdateStreamImpl {
	return &UpdateStreamImpl{
		ts:       ts,
		keyspace: keyspace,
		cell:     cell,
		mysqld:   mysqld,
		se:       se,
		dbname:   dbname,
	}
}

// RegisterService needs to be called to publish stats, and to start listening
// to clients. Only once instance can call this in a process.
func (updateStream *UpdateStreamImpl) RegisterService() {
	// publish the stats
	stats.Publish("UpdateStreamState", stats.StringFunc(func() string {
		return usStateNames[updateStream.state.Get()]
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

	updateStream.state.Set(usEnabled)
	updateStream.streams.Init()
	log.Infof("Enabling update stream, dbname: %s", updateStream.dbname)
}

// Disable will disallow any connection to the service
func (updateStream *UpdateStreamImpl) Disable() {
	defer logError()
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if !updateStream.IsEnabled() {
		return
	}

	updateStream.state.Set(usDisabled)
	updateStream.streams.Stop()
	updateStream.stateWaitGroup.Wait()
	log.Infof("Update Stream Disabled")
}

// IsEnabled returns true if UpdateStreamImpl is enabled
func (updateStream *UpdateStreamImpl) IsEnabled() bool {
	return updateStream.state.Get() == usEnabled
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

	// Calls cascade like this: binlog.Streamer->KeyRangeFilterFunc->func(*binlogdatapb.BinlogTransaction)->callback
	f := KeyRangeFilterFunc(keyRange, func(trans *binlogdatapb.BinlogTransaction) error {
		keyrangeStatements.Add(int64(len(trans.Statements)))
		keyrangeTransactions.Add(1)
		return callback(trans)
	})
	bls := NewStreamer(updateStream.dbname, updateStream.mysqld, updateStream.se, charset, pos, 0, f)
	bls.resolverFactory, err = newKeyspaceIDResolverFactory(ctx, updateStream.ts, updateStream.keyspace, updateStream.cell)
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

	// Calls cascade like this: binlog.Streamer->TablesFilterFunc->func(*binlogdatapb.BinlogTransaction)->callback
	f := TablesFilterFunc(tables, func(trans *binlogdatapb.BinlogTransaction) error {
		tablesStatements.Add(int64(len(trans.Statements)))
		tablesTransactions.Add(1)
		return callback(trans)
	})
	bls := NewStreamer(updateStream.dbname, updateStream.mysqld, updateStream.se, charset, pos, 0, f)

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
