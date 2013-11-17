// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package mysqlctl

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/rpcwrap"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

/* API and config for UpdateStream Service */

const (
	DISABLED = iota
	ENABLED
)

type UpdateStream struct {
	mycnf *Mycnf

	actionLock     sync.Mutex
	state          sync2.AtomicInt64
	mysqld         *Mysqld
	stateWaitGroup sync.WaitGroup
	dbname         string
}

var UpdateStreamRpcService *UpdateStream

func RegisterUpdateStreamService(mycnf *Mycnf) {
	if UpdateStreamRpcService != nil {
		panic("Update Stream service already initialized")
	}

	UpdateStreamRpcService = &UpdateStream{mycnf: mycnf}
	rpcwrap.RegisterAuthenticated(UpdateStreamRpcService)
}

func logError() {
	if x := recover(); x != nil {
		log.Errorf("%s", x.(error).Error())
	}
}

func EnableUpdateStreamService(dbcfgs dbconfigs.DBConfigs) {
	defer logError()
	UpdateStreamRpcService.enable(dbcfgs)
}

func DisableUpdateStreamService() {
	defer logError()
	UpdateStreamRpcService.disable()
}

func ServeUpdateStream(req *BinlogPosition, sendReply sendEventFunc) error {
	return UpdateStreamRpcService.ServeUpdateStream(req, sendReply)
}

func IsUpdateStreamEnabled() bool {
	return UpdateStreamRpcService.isEnabled()
}

func GetReplicationPosition() (*proto.ReplicationCoordinates, error) {
	return UpdateStreamRpcService.getReplicationPosition()
}

func (updateStream *UpdateStream) enable(dbcfgs dbconfigs.DBConfigs) {
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if updateStream.isEnabled() {
		return
	}

	if dbcfgs.Dba.UnixSocket == "" {
		log.Errorf("Missing dba socket connection, cannot enable update stream service")
		return
	}

	if dbcfgs.App.DbName == "" {
		log.Errorf("Missing db name, cannot enable update stream service")
		return
	}

	if updateStream.mycnf.BinLogPath == "" {
		log.Errorf("Update stream service requires binlogs enabled")
		return
	}

	updateStream.state.Set(ENABLED)
	updateStream.mysqld = NewMysqld(updateStream.mycnf, dbcfgs.Dba, dbcfgs.Repl)
	updateStream.dbname = dbcfgs.App.DbName
	log.Infof("Enabling update stream, dbname: %s, binlogpath: %s", updateStream.dbname, updateStream.mycnf.BinLogPath)
}

func (updateStream *UpdateStream) disable() {
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if !updateStream.isEnabled() {
		return
	}

	updateStream.state.Set(DISABLED)
	updateStream.stateWaitGroup.Wait()
	log.Infof("Update Stream Disabled")
}

func (updateStream *UpdateStream) isEnabled() bool {
	return updateStream.state.Get() == ENABLED
}

func (updateStream *UpdateStream) ServeUpdateStream(req *BinlogPosition, sendReply sendEventFunc) (err error) {
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

	rp, err := updateStream.mysqld.BinlogInfo(req.GroupId, req.ServerId)
	if err != nil {
		return fmt.Errorf("error computing start position: %v", err)
	}
	log.Infof("ServeUpdateStream starting @ %v", rp)

	evs := NewEventStreamer(updateStream.dbname, updateStream.mycnf.BinLogPath)
	return evs.Stream(rp.MasterLogFile, int64(rp.MasterLogPosition), func(reply *StreamEvent) error {
		if !updateStream.isEnabled() {
			evs.Stop()
			return nil
		}
		return sendReply(reply)
	})
}

func (updateStream *UpdateStream) getReplicationPosition() (*proto.ReplicationCoordinates, error) {
	updateStream.actionLock.Lock()
	defer updateStream.actionLock.Unlock()
	if !updateStream.isEnabled() {
		return nil, fmt.Errorf("update stream service is not enabled")
	}

	rp, err := updateStream.mysqld.MasterStatus()
	if err != nil {
		return nil, err
	}
	return proto.NewReplicationCoordinates(rp.MasterLogFile, uint64(rp.MasterLogPosition), rp.MasterLogGroupId), nil
}
