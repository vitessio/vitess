// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	updateStreamEvents   = stats.NewCounters("UpdateStreamEvents")
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

// UpdateStream is the real implementation of proto.UpdateStream
// and UpdateStreamControl
type UpdateStream struct {
	// the following variables are set at construction time

	mysqld mysqlctl.MysqlDaemon
	dbname string

	// actionLock protects the following variables
	actionLock     sync.Mutex
	state          sync2.AtomicInt64
	stateWaitGroup sync.WaitGroup
	streams        streamList
}

type streamList struct {
	sync.Mutex
	streams map[*sync2.ServiceManager]bool
}

func (sl *streamList) Init() {
	sl.Lock()
	sl.streams = make(map[*sync2.ServiceManager]bool)
	sl.Unlock()
}

func (sl *streamList) Add(e *sync2.ServiceManager) {
	sl.Lock()
	sl.streams[e] = true
	sl.Unlock()
}

func (sl *streamList) Delete(e *sync2.ServiceManager) {
	sl.Lock()
	delete(sl.streams, e)
	sl.Unlock()
}

func (sl *streamList) Stop() {
	sl.Lock()
	for stream := range sl.streams {
		stream.Stop()
	}
	sl.Unlock()
}

// RegisterUpdateStreamServiceFunc is the type to use for delayed
// registration of RPC servers until we have all the objects
type RegisterUpdateStreamServiceFunc func(proto.UpdateStream)

// RegisterUpdateStreamServices is the list of all registration
// callbacks to invoke
var RegisterUpdateStreamServices []RegisterUpdateStreamServiceFunc

// NewUpdateStream returns a new UpdateStream object
func NewUpdateStream(mysqld mysqlctl.MysqlDaemon, dbname string) *UpdateStream {
	return &UpdateStream{
		mysqld: mysqld,
		dbname: dbname,
	}
}

// RegisterService needs to be called to publish stats, and to start listening
// to clients. Only once instance can call this in a process.
func (updateStream *UpdateStream) RegisterService() {
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
func (updateStream *UpdateStream) Enable() {
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
func (updateStream *UpdateStream) Disable() {
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

// IsEnabled returns true if UpdateStream is enabled
func (updateStream *UpdateStream) IsEnabled() bool {
	return updateStream.state.Get() == usEnabled
}

// ServeUpdateStream is part of the proto.UpdateStream interface
func (updateStream *UpdateStream) ServeUpdateStream(position string, sendReply func(reply *proto.StreamEvent) error) (err error) {
	pos, err := myproto.DecodeReplicationPosition(position)
	if err != nil {
		return err
	}

	updateStream.actionLock.Lock()
	if !updateStream.IsEnabled() {
		updateStream.actionLock.Unlock()
		log.Errorf("Unable to serve client request: update stream service is not enabled")
		return fmt.Errorf("update stream service is not enabled")
	}
	updateStream.stateWaitGroup.Add(1)
	updateStream.actionLock.Unlock()
	defer updateStream.stateWaitGroup.Done()

	streamCount.Add("Updates", 1)
	defer streamCount.Add("Updates", -1)
	log.Infof("ServeUpdateStream starting @ %#v", pos)

	evs := NewEventStreamer(updateStream.dbname, updateStream.mysqld, pos, func(reply *proto.StreamEvent) error {
		if reply.Category == "ERR" {
			updateStreamErrors.Add("UpdateStream", 1)
		} else {
			updateStreamEvents.Add(reply.Category, 1)
		}
		return sendReply(reply)
	})

	svm := &sync2.ServiceManager{}
	svm.Go(evs.Stream)
	updateStream.streams.Add(svm)
	defer updateStream.streams.Delete(svm)
	return svm.Join()
}

// StreamKeyRange is part of the proto.UpdateStream interface
func (updateStream *UpdateStream) StreamKeyRange(position string, keyspaceIDType key.KeyspaceIdType, keyRange *pb.KeyRange, charset *mproto.Charset, sendReply func(reply *proto.BinlogTransaction) error) (err error) {
	pos, err := myproto.DecodeReplicationPosition(position)
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

	// Calls cascade like this: BinlogStreamer->KeyRangeFilterFunc->func(*proto.BinlogTransaction)->sendReply
	f := KeyRangeFilterFunc(keyspaceIDType, keyRange, func(reply *proto.BinlogTransaction) error {
		keyrangeStatements.Add(int64(len(reply.Statements)))
		keyrangeTransactions.Add(1)
		return sendReply(reply)
	})
	bls := NewBinlogStreamer(updateStream.dbname, updateStream.mysqld, charset, pos, f)

	svm := &sync2.ServiceManager{}
	svm.Go(bls.Stream)
	updateStream.streams.Add(svm)
	defer updateStream.streams.Delete(svm)
	return svm.Join()
}

// StreamTables is part of the proto.UpdateStream interface
func (updateStream *UpdateStream) StreamTables(position string, tables []string, charset *mproto.Charset, sendReply func(reply *proto.BinlogTransaction) error) (err error) {
	pos, err := myproto.DecodeReplicationPosition(position)
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

	// Calls cascade like this: BinlogStreamer->TablesFilterFunc->func(*proto.BinlogTransaction)->sendReply
	f := TablesFilterFunc(tables, func(reply *proto.BinlogTransaction) error {
		keyrangeStatements.Add(int64(len(reply.Statements)))
		keyrangeTransactions.Add(1)
		return sendReply(reply)
	})
	bls := NewBinlogStreamer(updateStream.dbname, updateStream.mysqld, charset, pos, f)

	svm := &sync2.ServiceManager{}
	svm.Go(bls.Stream)
	updateStream.streams.Add(svm)
	defer updateStream.streams.Delete(svm)
	return svm.Join()
}

// HandlePanic is part of the proto.UpdateStream interface
func (updateStream *UpdateStream) HandlePanic(err *error) {
	if x := recover(); x != nil {
		log.Errorf("Uncaught panic:\n%v\n%s", x, tb.Stack(4))
		*err = fmt.Errorf("uncaught panic: %v", x)
	}
}
