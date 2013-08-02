// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package mysqlctl

import (
	"expvar"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/youtube/vitess/go/relog"
	"github.com/youtube/vitess/go/rpcwrap"
	estats "github.com/youtube/vitess/go/stats" // stats is a private type defined somewhere else in this package, so it would conflict
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
	mycnf          *Mycnf
	tabletType     string
	state          sync2.AtomicInt32
	states         *estats.States
	actionLock     sync.Mutex
	binlogPrefix   string
	logsDir        string
	mysqld         *Mysqld
	stateWaitGroup sync.WaitGroup
	dbname         string
}

type UpdateStreamRequest struct {
	StartPosition proto.BinlogPosition
}

var UpdateStreamRpcService *UpdateStream

func RegisterUpdateStreamService(mycnf *Mycnf) {
	if UpdateStreamRpcService != nil {
		//relog.Warning("Update Stream service already initialized")
		return
	}

	UpdateStreamRpcService = &UpdateStream{mycnf: mycnf}
	UpdateStreamRpcService.states = estats.NewStates("", []string{
		"Disabled",
		"Enabled",
	}, time.Now(), DISABLED)
	rpcwrap.RegisterAuthenticated(UpdateStreamRpcService)
	expvar.Publish("UpdateStreamRpcService", estats.StrFunc(func() string { return UpdateStreamRpcService.statsJSON() }))
}

func logError() {
	if x := recover(); x != nil {
		relog.Error("%s", x.(error).Error())
	}
}

func dbcfgsCorrect(tabletType string, dbcfgs dbconfigs.DBConfigs) bool {
	switch tabletType {
	case "master":
		if dbcfgs.Dba.UnixSocket != "" {
			return true
		}
	case "replica", "rdonly", "batch":
		if dbcfgs.Dba.UnixSocket != "" && dbcfgs.Repl.UnixSocket != "" {
			return true
		}
	}
	return false
}

func EnableUpdateStreamService(tabletType string, dbcfgs dbconfigs.DBConfigs) {
	defer logError()
	UpdateStreamRpcService.actionLock.Lock()
	defer UpdateStreamRpcService.actionLock.Unlock()

	if !dbcfgsCorrect(tabletType, dbcfgs) {
		relog.Warning("missing/incomplete db configs file, cannot enable update stream service")
		return
	}

	if UpdateStreamRpcService.mycnf.BinLogPath == "" {
		relog.Warning("Update stream service requires binlogs enabled")
		return
	}

	if UpdateStreamRpcService.isServiceEnabled() {
		relog.Warning("Update stream service is already enabled")
		return
	}

	UpdateStreamRpcService.setState(ENABLED)

	UpdateStreamRpcService.mysqld = NewMysqld(UpdateStreamRpcService.mycnf, dbcfgs.Dba, dbcfgs.Repl)
	UpdateStreamRpcService.dbname = dbcfgs.App.Dbname
	relog.Info("dbcfgs.App.Dbname %v DbName %v", dbcfgs.App.Dbname, UpdateStreamRpcService.dbname)
	relog.Info("mycnf.BinLogPath %v mycnf.RelayLogPath %v", UpdateStreamRpcService.mycnf.BinLogPath, UpdateStreamRpcService.mycnf.RelayLogPath)
	UpdateStreamRpcService.tabletType = tabletType
	UpdateStreamRpcService.binlogPrefix = UpdateStreamRpcService.mycnf.BinLogPath
	UpdateStreamRpcService.logsDir = path.Dir(UpdateStreamRpcService.binlogPrefix)

	relog.Info("Update Stream enabled, logsDir %v", UpdateStreamRpcService.logsDir)
}

func DisableUpdateStreamService() {
	//If the service is already disabled, just return
	if !UpdateStreamRpcService.isServiceEnabled() {
		return
	}
	UpdateStreamRpcService.actionLock.Lock()
	defer UpdateStreamRpcService.actionLock.Unlock()
	disableUpdateStreamService()
	relog.Info("Update Stream Disabled")
}

func IsUpdateStreamEnabled() bool {
	return UpdateStreamRpcService.isServiceEnabled()
}

func disableUpdateStreamService() {
	defer logError()

	UpdateStreamRpcService.setState(DISABLED)
	UpdateStreamRpcService.stateWaitGroup.Wait()
}

func (updateStream *UpdateStream) isServiceEnabled() bool {
	return updateStream.state.Get() == ENABLED
}

func (updateStream *UpdateStream) setState(state int32) {
	updateStream.state.Set(state)
	updateStream.states.SetState(int(state))
}

// expvar.Var interface.
// NOTE(szopa): It's unexported to avoid rpc logspam.
func (updateStream *UpdateStream) statsJSON() string {
	return fmt.Sprintf("{"+
		"\"States\": %v"+
		"}", updateStream.states.String())
}

func LogsDir() string {
	return UpdateStreamRpcService.logsDir
}

func ServeUpdateStream(req *UpdateStreamRequest, sendReply SendUpdateStreamResponse) error {
	return UpdateStreamRpcService.ServeUpdateStream(req, sendReply)
}

func (updateStream *UpdateStream) ServeUpdateStream(req *UpdateStreamRequest, sendReply SendUpdateStreamResponse) error {
	defer func() {
		if x := recover(); x != nil {
			//Send the error to the client.
			//panic(x)
			SendError(sendReply, x.(error), nil)
		}
	}()

	if !updateStream.isServiceEnabled() {
		relog.Warning("Unable to serve client request: Update stream service is not enabled yet")
		return fmt.Errorf("Update stream service is not enabled yet")
	}

	if !IsStartPositionValid(&req.StartPosition) {
		return fmt.Errorf("Invalid start position, cannot serve the stream")
	}
	relog.Info("ServeUpdateStream starting @ %v", req.StartPosition.String())

	startCoordinates := &req.StartPosition.Position
	blp := NewBlp(startCoordinates, updateStream)

	//locate the relay filename and position based on the masterPosition map
	if !IsMasterPositionValid(startCoordinates) {
		return fmt.Errorf("Invalid start position %v", req.StartPosition)
	}

	updateStream.actionLock.Lock()
	updateStream.stateWaitGroup.Add(1)
	updateStream.actionLock.Unlock()
	defer updateStream.clientDone()

	blp.StreamBinlog(sendReply, updateStream.binlogPrefix)
	return nil
}

func (updateStream *UpdateStream) clientDone() {
	updateStream.stateWaitGroup.Done()
}

func (updateStream *UpdateStream) getReplicationPosition() (*proto.ReplicationCoordinates, error) {
	rp, err := updateStream.mysqld.MasterStatus()
	if err != nil {
		return nil, err
	}
	return proto.NewReplicationCoordinates(rp.MasterLogFile, uint64(rp.MasterLogPosition), rp.MasterLogGroupId), nil
}

func GetReplicationPosition() (*proto.ReplicationCoordinates, error) {
	return UpdateStreamRpcService.getReplicationPosition()
}

func IsMasterPositionValid(startCoordinates *proto.ReplicationCoordinates) bool {
	if startCoordinates.MasterFilename == "" || startCoordinates.MasterPosition <= 0 {
		return false
	}
	return true
}

func IsStartPositionValid(startPos *proto.BinlogPosition) bool {
	startCoord := &startPos.Position
	return IsMasterPositionValid(startCoord)
}
