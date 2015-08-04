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
	DISABLED int64 = iota
	ENABLED
)

var usStateNames = map[int64]string{
	ENABLED:  "Enabled",
	DISABLED: "Disabled",
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

// UpdateStream is the real implementation of proto.UpdateStream
type UpdateStream struct {
	mycnf *mysqlctl.Mycnf

	actionLock     sync.Mutex
	state          sync2.AtomicInt64
	mysqld         mysqlctl.MysqlDaemon
	stateWaitGroup sync.WaitGroup
	dbname         string
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

// UpdateStream is the singleton that gets initialized during
// startup and that gets called by all RPC server implementations
var UpdateStreamRpcService *UpdateStream

// RegisterUpdateStreamServiceFunc is the type to use for delayed
// registration of RPC servers until we have all the objects
type RegisterUpdateStreamServiceFunc func(proto.UpdateStream)

// RegisterUpdateStreamServices is the list of all registration
// callbacks to invoke
var RegisterUpdateStreamServices []RegisterUpdateStreamServiceFunc

// RegisterUpdateStreamService needs to be called to start listening
// to clients
func RegisterUpdateStreamService(mycnf *mysqlctl.Mycnf) {
	// check we haven't been called already
	if UpdateStreamRpcService != nil {
		panic("Update Stream service already initialized")
	}

	// create the singleton
	UpdateStreamRpcService = &UpdateStream{mycnf: mycnf}
	stats.Publish("UpdateStreamState", stats.StringFunc(func() string {
		return usStateNames[UpdateStreamRpcService.state.Get()]
	}))

	// and register all the instances
	for _, f := range RegisterUpdateStreamServices {
		f(UpdateStreamRpcService)
	}
}

func logError() {
	if x := recover(); x != nil {
		log.Errorf("%s", x.(error).Error())
	}
}

// EnableUpdateStreamService enables the RPC service for UpdateStream
func EnableUpdateStreamService(dbname string, mysqld mysqlctl.MysqlDaemon) {
	defer logError()
	UpdateStreamRpcService.enable(dbname, mysqld)
}

// DisableUpdateStreamService disables the RPC service for UpdateStream
func DisableUpdateStreamService() {
	defer logError()
	UpdateStreamRpcService.disable()
}

// ServeUpdateStream sill serve one UpdateStream
func ServeUpdateStream(position string, sendReply func(reply *proto.StreamEvent) error) error {
	return UpdateStreamRpcService.ServeUpdateStream(position, sendReply)
}

// IsUpdateStreamEnabled returns true if the RPC service is enabled
func IsUpdateStreamEnabled() bool {
	return UpdateStreamRpcService.isEnabled()
}

// GetReplicationPosition returns the current replication position of
// the service
func GetReplicationPosition() (myproto.ReplicationPosition, error) {
	return UpdateStreamRpcService.getReplicationPosition()
}

func (updateStream *UpdateStream) enable(dbname string, mysqld mysqlctl.MysqlDaemon) {
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if updateStream.isEnabled() {
		return
	}

	if dbname == "" {
		log.Errorf("Missing db name, cannot enable update stream service")
		return
	}

	if updateStream.mycnf.BinLogPath == "" {
		log.Errorf("Update stream service requires binlogs enabled")
		return
	}

	updateStream.state.Set(ENABLED)
	updateStream.mysqld = mysqld
	updateStream.dbname = dbname
	updateStream.streams.Init()
	log.Infof("Enabling update stream, dbname: %s, binlogpath: %s", updateStream.dbname, updateStream.mycnf.BinLogPath)
}

func (updateStream *UpdateStream) disable() {
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if !updateStream.isEnabled() {
		return
	}

	updateStream.state.Set(DISABLED)
	updateStream.streams.Stop()
	updateStream.stateWaitGroup.Wait()
	log.Infof("Update Stream Disabled")
}

func (updateStream *UpdateStream) isEnabled() bool {
	return updateStream.state.Get() == ENABLED
}

// ServeUpdateStream is part of the proto.UpdateStream interface
func (updateStream *UpdateStream) ServeUpdateStream(position string, sendReply func(reply *proto.StreamEvent) error) (err error) {
	pos, err := myproto.DecodeReplicationPosition(position)
	if err != nil {
		return err
	}

	updateStream.actionLock.Lock()
	if !updateStream.isEnabled() {
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
func (updateStream *UpdateStream) StreamKeyRange(position string, keyspaceIdType key.KeyspaceIdType, keyRange *pb.KeyRange, charset *mproto.Charset, sendReply func(reply *proto.BinlogTransaction) error) (err error) {
	pos, err := myproto.DecodeReplicationPosition(position)
	if err != nil {
		return err
	}

	updateStream.actionLock.Lock()
	if !updateStream.isEnabled() {
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
	f := KeyRangeFilterFunc(keyspaceIdType, keyRange, func(reply *proto.BinlogTransaction) error {
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
	if !updateStream.isEnabled() {
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

func (updateStream *UpdateStream) getReplicationPosition() (myproto.ReplicationPosition, error) {
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if !updateStream.isEnabled() {
		return myproto.ReplicationPosition{}, fmt.Errorf("update stream service is not enabled")
	}

	return updateStream.mysqld.MasterPosition()
}
