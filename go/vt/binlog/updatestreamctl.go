// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlog

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
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

type UpdateStream struct {
	mycnf *mysqlctl.Mycnf

	actionLock     sync.Mutex
	state          sync2.AtomicInt64
	mysqld         *mysqlctl.Mysqld
	stateWaitGroup sync.WaitGroup
	dbname         string
	streams        streamList
}

type streamer interface {
	Stop()
}

type streamList struct {
	sync.Mutex
	streams map[streamer]bool
}

func (sl *streamList) Init() {
	sl.Lock()
	sl.streams = make(map[streamer]bool)
	sl.Unlock()
}

func (sl *streamList) Add(e streamer) {
	sl.Lock()
	sl.streams[e] = true
	sl.Unlock()
}

func (sl *streamList) Delete(e streamer) {
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

// UpdateStreamRpcService is the singleton that gets initialized during
// startup and that gets called by all RPC server implementations
var UpdateStreamRpcService *UpdateStream

// Glue to delay registration of RPC servers until we have all the objects
type RegisterUpdateStreamServiceFunc func(*UpdateStream)

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

func EnableUpdateStreamService(dbname string, mysqld *mysqlctl.Mysqld) {
	defer logError()
	UpdateStreamRpcService.enable(dbname, mysqld)
}

func DisableUpdateStreamService() {
	defer logError()
	UpdateStreamRpcService.disable()
}

func ServeUpdateStream(req *proto.UpdateStreamRequest, sendReply func(reply *proto.StreamEvent) error) error {
	return UpdateStreamRpcService.ServeUpdateStream(req, sendReply)
}

func IsUpdateStreamEnabled() bool {
	return UpdateStreamRpcService.isEnabled()
}

func GetReplicationPosition() (myproto.GTID, error) {
	return UpdateStreamRpcService.getReplicationPosition()
}

func (updateStream *UpdateStream) enable(dbname string, mysqld *mysqlctl.Mysqld) {
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

func (updateStream *UpdateStream) ServeUpdateStream(req *proto.UpdateStreamRequest, sendReply func(reply *proto.StreamEvent) error) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = x.(error)
		}
	}()

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
	log.Infof("ServeUpdateStream starting @ %#v", req.GTIDField.Value)

	evs := NewEventStreamer(updateStream.dbname, updateStream.mysqld)
	updateStream.streams.Add(evs)
	defer updateStream.streams.Delete(evs)

	// Calls cascade like this: BinlogStreamer->func(*proto.StreamEvent)->sendReply
	return evs.Stream(req.GTIDField.Value, func(reply *proto.StreamEvent) error {
		if reply.Category == "ERR" {
			updateStreamErrors.Add("UpdateStream", 1)
		} else {
			updateStreamEvents.Add(reply.Category, 1)
		}
		return sendReply(reply)
	})
}

func (updateStream *UpdateStream) StreamKeyRange(req *proto.KeyRangeRequest, sendReply func(reply *proto.BinlogTransaction) error) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = x.(error)
		}
	}()

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
	log.Infof("ServeUpdateStream starting @ %#v", req.GTIDField.Value)

	bls := NewBinlogStreamer(updateStream.dbname, updateStream.mysqld)
	updateStream.streams.Add(bls)
	defer updateStream.streams.Delete(bls)

	// Calls cascade like this: BinlogStreamer->KeyRangeFilterFunc->func(*proto.BinlogTransaction)->sendReply
	f := KeyRangeFilterFunc(req.KeyspaceIdType, req.KeyRange, func(reply *proto.BinlogTransaction) error {
		keyrangeStatements.Add(int64(len(reply.Statements)))
		keyrangeTransactions.Add(1)
		return sendReply(reply)
	})
	return bls.Stream(req.GTIDField.Value, f)
}

func (updateStream *UpdateStream) StreamTables(req *proto.TablesRequest, sendReply func(reply *proto.BinlogTransaction) error) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = x.(error)
		}
	}()

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
	log.Infof("ServeUpdateStream starting @ %#v", req.GTIDField.Value)

	bls := NewBinlogStreamer(updateStream.dbname, updateStream.mysqld)
	updateStream.streams.Add(bls)
	defer updateStream.streams.Delete(bls)

	// Calls cascade like this: BinlogStreamer->KeyRangeFilterFunc->func(*proto.BinlogTransaction)->sendReply
	f := TablesFilterFunc(req.Tables, func(reply *proto.BinlogTransaction) error {
		keyrangeStatements.Add(int64(len(reply.Statements)))
		keyrangeTransactions.Add(1)
		return sendReply(reply)
	})
	return bls.Stream(req.GTIDField.Value, f)
}

func (updateStream *UpdateStream) getReplicationPosition() (myproto.GTID, error) {
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if !updateStream.isEnabled() {
		return nil, fmt.Errorf("update stream service is not enabled")
	}

	rp, err := updateStream.mysqld.MasterStatus()
	if err != nil {
		return nil, err
	}
	return rp.MasterLogGTIDField.Value, nil
}
